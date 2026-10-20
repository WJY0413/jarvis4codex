"""Explicit paused-namespace transition over one existing provider database.

Preparation and verification are read-only. Registration runs only inside the
ordinary ResearchStore.initialize transaction. Old processes remain retained;
this is a task-scoped supported-pause boundary, not an external operator fence.
"""
from dataclasses import asdict,is_dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import ResearchError,ResearchStore,_research_table,_digest,_json,_require,_stamp,make_shared_union_binding

SCHEMA='jarvis-shared-research-union/v2'
ARCHIVE='jarvis_research_union_archive'


def exact_path(value):
    p=Path(value);_require(p.is_absolute() and p.resolve()==p and not p.is_symlink(),'exact unlinked transition path required');return p


def semantic(state):
    return {k:v for k,v in state.items() if k!='revision'}


def queued_generation(claim):
    current=claim.get('current');_require(isinstance(current,dict),'queued current generation absent')
    return {k:claim.get(k) for k in ('claim_id','hold_id','attempt','thread_id','previous_turn_id','lane','checkpoint','history')}|{'current':{k:current.get(k) for k in ('request_id','tool','arguments','phase','turn_id','prepared_at','sent_at')}}


def read_states(db,old_namespace):
    original=strict_json(db.execute('SELECT data FROM jarvis_research_run WHERE id=1').fetchone()[0])
    retired=strict_json(db.execute(f"SELECT data FROM {_research_table('run',old_namespace)} WHERE id=1").fetchone()[0])
    for state in (original,retired):ResearchStore._validate_state(state)
    _require(original['status']==retired['status']=='paused','both retained controllers must remain supported-paused')
    _require(not any(c.get('current') or c.get('actual_observation') for c in retired['claims']),'retired namespace still has current/observed work')
    _require(len(retired['claims'])==14 and all(c['lane']=='base' for c in retired['claims']),'exact fourteen ordinary predecessors required')
    gate=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    return original,retired,gate


def certificate(binding,claim,registry,*,queued=False):
    from adapters.codex_app_server.finite_terminal_settlement import read_settlement,CERTIFICATE,ARCHIVE_REQUEST
    root=exact_path(binding['root']);_require(root==exact_path(registry)/'task-holds'/claim['hold_id'],'certificate original registry binding mismatch')
    _require(hashlib.sha256((root/CERTIFICATE).read_bytes()).hexdigest()==binding['sha256'],'typed certificate changed')
    proof=read_settlement(root)
    _require(isinstance(proof,dict) and proof.get('hold_released') is True and proof.get('physical_owner_exit_confirmed') is None
        and proof.get('evidence_source')=='finite_terminal_settlement_v2','strict finite certificate authority missing')
    if queued:
        current=claim['current'];identity={'request_id':current['request_id'],'hold_id':claim['hold_id'],'thread_id':claim['thread_id'],'turn_id':None}
        _require(claim['lane']=='base' and current['tool']=='jarvis_resume' and current['phase']=='research'
            and current.get('turn_id') is None and current['arguments']['model']=='gpt-6-luna','only exact queued ordinary resume supported')
        _require(proof.get('release_kind')=='queued_non_dispatch' and proof.get('non_dispatch_settled') is True
            and proof.get('terminal_confirmed') is False and proof.get('native_terminal_confirmed') is False,'queued proof must preserve absent native terminal')
        request=root/ARCHIVE_REQUEST
        _require(hashlib.sha256(request.read_bytes()).hexdigest()==proof['original_request_sha256'],'queued archived request changed')
        _require(strict_json(request.read_text()).get('model')=='gpt-6-luna','queued requested model mismatch')
    else:
        history=claim['history'][-1];identity={'request_id':history['request_id'],'hold_id':claim['hold_id'],'thread_id':claim['thread_id'],'turn_id':history.get('turn_id')}
        _require(proof.get('release_kind')=='accepted_terminal' and proof.get('terminal_confirmed') is True
            and history.get('terminal_confirmed') is True and history.get('hold_released') is True,'exact finite accepted terminal required')
    _require(_digest({k:proof.get(k) for k in identity})==_digest(identity),'finite certificate generation mismatch')
    return proof


def project_queued(original,gate,transition,registry):
    original=json.loads(_json(original));gate=json.loads(_json(gate));q=transition['queued_non_dispatch']
    claim=next(c for c in original['claims'] if c['claim_id']==q['claim_id'])
    _require(_digest(queued_generation(claim))==q['generation_sha256'],'queued generation CAS changed')
    proof=certificate(q['certificate'],claim,registry,queued=True);current=claim['current'];rid=current['request_id']
    reservation=gate['reservations'].get(rid)
    _require(reservation and reservation['status']!='finished' and reservation['owner']==original['config']['owner_id']
        and reservation['lane']=='base' and type(reservation['cost']) is int and reservation['cost']==1,'queued provider lifecycle binding mismatch')
    authority={'schema':'jarvis-finite-non-dispatch-authority/v1','source':'finite_terminal_settlement_v2',
        'certificate_root':q['certificate']['root'],'certificate_sha256':q['certificate']['sha256'],
        'original_request_sha256':proof['original_request_sha256'],'original_ack_sha256':proof['original_ack_sha256'],
        'physical_owner_exit_confirmed':None,'successor_exclusion':proof['successor_exclusion']}
    claim['history'].append({'request_id':rid,'tool':current['tool'],'phase':current['phase'],'turn_id':None,
        'outcome':'non_dispatch_settled','terminal_confirmed':False,'hold_released':True,'non_dispatch_settled':True,
        'physical_owner_exit_confirmed':None,'sent_at':current.get('sent_at'),'finished_at':transition['settled_at'],
        'finite_non_dispatch_authority':authority,'checkpoint_sha256':None})
    claim.update(current=None,actual_observation=None,status='retired_not_sent',last_error=None)
    gate['reservations'][rid]['status']='finished';gate['pending'].pop(rid,None)
    if gate['probe']==rid:gate['probe']=None
    return original,gate,rid


def declarations(claims):return [asdict(c) if is_dataclass(c) else dict(c) for c in claims]


def provenance_for(claims,original,retired,old_namespace):
    candidates={}
    for namespace,state in ((None,original),(old_namespace,retired)):
        for claim in state['claims']:
            key=(claim['country'].strip().casefold(),claim['jurisdiction_code'].strip().casefold())
            candidates[key]=(namespace,claim)
    ids={c['claim_id'] for s in (original,retired) for c in s['claims']}
    result=[]
    for new in claims:
        key=(new['country'].strip().casefold(),new['jurisdiction_code'].strip().casefold())
        _require(key in candidates and new['claim_id'] not in ids,'successor claim identity/jurisdiction is not a released predecessor')
        namespace,old=candidates[key]
        _require(old['lane']==new['lane']=='base' and old.get('current') is None
            and new['jurisdiction']==old['jurisdiction'] and _digest(new.get('checkpoint'))==_digest(old.get('checkpoint')),
            'successor must preserve exact released jurisdiction and latest formal checkpoint')
        result.append({'claim_id':new['claim_id'],'source_namespace':namespace,'source_claim_id':old['claim_id'],
            'checkpoint_sha256':_digest(old.get('checkpoint'))})
    _require(len(claims)==20 and len({x['claim_id'] for x in claims})==20
        and len({(x['country'].strip().casefold(),x['jurisdiction_code'].strip().casefold()) for x in claims})==20,
        'exact twenty distinct ordinary declarations required')
    _require(sum(x['source_namespace']==old_namespace for x in result)==14,'all fourteen retired claims must carry forward')
    return result


def verify_retired(transition,retired):
    _require(_digest(semantic(retired))==transition['old_state_semantic_sha256'],'retired namespace changed or resumed')
    from jarvis_control.partial_predecessor import verify_transition_fences
    verify_transition_fences(transition,retired)


def build_binding(original,retired,gate,entry,namespace,transition,claims):
    projected,finished,_=project_queued(original,gate,transition,transition['original_registry'])
    current=[c for c in projected['claims'] if c.get('current')]
    _require(len(current)==5 and all(c['lane']=='task' for c in current),'original predecessor must retain exactly five TASK currents')
    external=set(entry['binding']['external_request_ids']); task_ids={c['current']['request_id'] for c in current}
    unresolved={rid for rid,row in finished['reservations'].items() if row['status']!='finished'}
    _require(len(external)==10 and len(task_ids)==5 and unresolved==external|task_ids,
        'transition must retain exactly external ten and five TASK reservations')
    _require(all(finished['reservations'][rid]['owner']==original['config']['owner_id']
        and finished['reservations'][rid]['lane']=='task' for rid in task_ids),'retained TASK reservation ownership mismatch')
    # Revalidate the five previously retired ordinary identities used as sources.
    sources=provenance_for(claims,projected,retired,transition['old_namespace'])
    for source in sources:
        if source['source_namespace'] is None and source['source_claim_id']!=transition['queued_non_dispatch']['claim_id']:
            c=next(c for c in projected['claims'] if c['claim_id']==source['source_claim_id'])
            authority=c['history'][-1].get('finite_terminal_authority') or {}
            certificate({'root':authority['certificate_root'],'sha256':authority['certificate_sha256']},c,transition['original_registry'])
    threads=set(entry['binding']['excluded_native_thread_ids'])|{c['thread_id'] for c in retired['claims'] if c.get('thread_id')}
    binding=make_shared_union_binding({'state':projected,'transport':{'gate':finished}},
        external_request_ids=entry['binding']['external_request_ids'],excluded_jurisdictions=entry['binding']['excluded_jurisdictions'],
        excluded_native_thread_ids=sorted(threads))
    transition={**transition,'successor_claims_sha256':_digest(claims),'claim_provenance':sources,
        'retained_task_request_ids':sorted(c['current']['request_id'] for c in current)}
    return {**binding,'schema':SCHEMA,'transition':transition}


def prepare_shared_union_transition(database,namespace,*,old_namespace,old_registry,queued_certificate,claims,close_fences,settled_at=None):
    database=exact_path(database);_research_table('run',namespace);_research_table('run',old_namespace)
    _require(namespace!=old_namespace,'fresh namespace required');claims=declarations(claims)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');original,retired,gate=read_states(db,old_namespace)
        rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
        _require(len(rows)==1 and rows[0][0]==old_namespace,'exact old namespace registration required')
        entry=strict_json(rows[0][1]);currents=[c for c in original['claims'] if c.get('current') and c['lane']=='base']
        _require(len(currents)==1,'exact one original queued ordinary current required');claim=currents[0]
        transition={'old_namespace':old_namespace,'old_registry':str(exact_path(old_registry)),
            'original_registry':str(database.parent),'old_union_entry_sha256':_digest(entry),
            'old_state_semantic_sha256':_digest(semantic(retired)),
            'database_identity':{'device':database.stat().st_dev,'inode':database.stat().st_ino},
            'settled_at':settled_at or _stamp(time.time()),'close_fences':close_fences,
            'queued_non_dispatch':{'claim_id':claim['claim_id'],'generation_sha256':_digest(queued_generation(claim)),
                'certificate':dict(queued_certificate)}}
        verify_retired(transition,retired)
        return build_binding(original,retired,gate,entry,namespace,transition,claims)


def verify_transition_before(db,namespace,binding,claims):
    t=binding['transition'];original,retired,gate=read_states(db,t['old_namespace']);verify_retired(t,retired)
    rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
    _require(len(rows)==1 and rows[0][0]==t['old_namespace'],'old union registration CAS changed')
    entry=strict_json(rows[0][1]);_require(_digest(entry)==t['old_union_entry_sha256'],'old union registration changed')
    expected=build_binding(original,retired,gate,entry,namespace,t,claims)
    _require(_digest(expected)==_digest(binding),'transition is not derived from exact current proof-bound snapshot')
    return original,retired,gate,entry


def verify_transition_after(db,binding):
    t=binding['transition'];_,retired,_=read_states(db,t['old_namespace']);verify_retired(t,retired)
    row=db.execute(f'SELECT data FROM {ARCHIVE} WHERE namespace=?',(t['old_namespace'],)).fetchone()
    _require(row is not None,'retired namespace archive missing');archived=strict_json(row[0])
    _require(_digest(archived['entry'])==t['old_union_entry_sha256'] and archived['binding_sha256']==_digest(binding),
        'retired namespace archive changed')


def check_inode(store):
    stat=store.path.stat();_require(_digest(store.shared_union['transition']['database_identity'])==_digest({'device':stat.st_dev,'inode':stat.st_ino}),
        'shared transition database identity changed')


def register_transition(store,db,config,claims):
    from jarvis_runtime.shared_transport_recovery import SharedTransportGate
    binding=store.shared_union;t=binding['transition'];check_inode(store)
    _require(config.hard_total==35 and config.reserved_external==10 and config.concurrency_policy=='healthy_batch'
        and config.deadline==binding['deadline'],'authorized same-deadline35 batch required')
    claims=declarations(claims);_require(_digest(claims)==t['successor_claims_sha256'],'successor declaration changed')
    existing=db.execute(f"SELECT data FROM {store.tables['run']} WHERE id=1").fetchone()
    if existing:
        store._check_union(db,strict_json(existing[0]));return
    original,retired,before,entry=verify_transition_before(db,store.namespace,binding,claims)
    _require(config.run_id not in {original['config']['run_id'],retired['config']['run_id']}
        and config.owner_id not in {original['config']['owner_id'],retired['config']['owner_id']},'new run/owner identity required')
    projected,expected,rid=project_queued(original,before,t,t['original_registry'])
    receipts=db.execute('SELECT provider,kind,receipt_id,data FROM jarvis_transport_receipts ORDER BY provider,kind,receipt_id').fetchall()
    SharedTransportGate.finish_existing_in_transaction(db,'openai',rid)
    after=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    _require(_digest(after)==_digest(expected),'transition altered unrelated provider state')
    _require(receipts==db.execute('SELECT provider,kind,receipt_id,data FROM jarvis_transport_receipts ORDER BY provider,kind,receipt_id').fetchall(),'transition altered health/dedup receipts')
    projected['revision']+=1;ResearchStore._validate_state(projected)
    db.execute('UPDATE jarvis_research_run SET data=? WHERE id=1',(_json(projected),))
    db.execute(f'CREATE TABLE IF NOT EXISTS {ARCHIVE}(namespace TEXT PRIMARY KEY,data TEXT NOT NULL)')
    db.execute(f'INSERT INTO {ARCHIVE} VALUES(?,?)',(t['old_namespace'],_json({'entry':entry,'binding_sha256':_digest(binding),'retired_state_sha256':t['old_state_semantic_sha256']})))
    db.execute('DELETE FROM jarvis_research_union WHERE namespace=?',(t['old_namespace'],))
    new_entry={'namespace':store.namespace,'run_id':config.run_id,'owner_id':config.owner_id,
        'declaration_sha256':_digest({'config':asdict(config),'claims':claims}),'binding':binding}
    db.execute('INSERT INTO jarvis_research_union VALUES(?,?)',(store.namespace,_json(new_entry)))
    check_inode(store);store._check_union(db,None,register=True)
    # Strict proof readback again before the enclosing initialize transaction commits.
    project_queued(original,before,t,t['original_registry']);verify_retired(t,retired)


def verify_shared_union_transition(database,namespace,binding,claims=None):
    """Read-only preflight both before and after the atomic namespace transition."""
    database=exact_path(database);_require(binding.get('schema')==SCHEMA,'explicit v2 transition required')
    store=object.__new__(ResearchStore);store.path=database;store.namespace=namespace;store.shared_union=binding
    store.tables={kind:_research_table(kind,namespace) for kind in ('run','checkpoints','native_scans')};check_inode(store)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');row=db.execute('SELECT data FROM jarvis_research_union WHERE namespace=?',(namespace,)).fetchone()
        if row:
            state=strict_json(db.execute(f"SELECT data FROM {store.tables['run']} WHERE id=1").fetchone()[0])
            return store._check_union(db,state)
        _require(claims is not None,'pre-registration verification requires exact successor claims')
        original,retired,gate,_=verify_transition_before(db,namespace,binding,declarations(claims))
        _,projected,rid=project_queued(original,gate,binding['transition'],binding['transition']['original_registry'])
        unresolved={k:v for k,v in projected['reservations'].items() if v['status']!='finished'}
        external=set(binding['external_request_ids'])
        _require(len(external)==10 and all(k in unresolved for k in external),'exact unresolved external ten required')
        _require(len(unresolved)==15 and sum(v['lane']=='task' for k,v in unresolved.items() if k not in external)==5,'exact15 retained exposure required')
        return {'potential':15,'external_reserved':10,'predecessor_reserved':5,'sibling_reserved':0,
            'lanes':{'base':0,'task':5},'shared_pending':dict(projected['pending']),'pending_transition':True,'queued_request_id':rid}
