"""Retain a paused namespace's unresolved work while admitting fresh jurisdictions.

Only admission registration moves. No reservation, provider budget, owner, native
identity, history or checkpoint is released, adopted or copied by this module.
"""
from dataclasses import asdict
import hashlib
import json
import sqlite3

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import ResearchStore,_research_table,_digest,_json,_require,make_shared_union_binding
from jarvis_control.shared_union_transition import ARCHIVE,check_inode,declarations,exact_path
from jarvis_control.retired_union_transition import read_state,verify_after as verify_prior
from jarvis_runtime.native_scan_receipt import validate_public_scan,PUBLIC_FIELDS
from adapters.codex_app_server.native_execution_evidence import classify_native_turn,classify_owner_terminal_receipt

SCHEMA='jarvis-shared-research-union/v4'
PROGRESS={'diagnostic_epoch','last_progress_at','progress_sequence','stall_epoch','stalled','stall_diagnostic'}
MUTABLE={'status','last_error','actual_observation','current','history','checkpoint','previous_turn_id'}
QA='jarvis_research_qa_eligibility'
CORRECTIONS=('jarvis_research_business_reconciliations','jarvis_research_exact_json_reconciliations')


def _hash_text(raw):return hashlib.sha256(raw.encode()).hexdigest()
def _claim_static(c):return {k:v for k,v in c.items() if k not in MUTABLE}
def _current_static(c):return {k:v for k,v in c.items() if k not in PROGRESS}
def _scope(c):return (c['country'].strip().casefold(),c['jurisdiction_code'].strip().casefold())


def _provenance_rows(db,namespace):
    tables={x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")};result=[]
    if QA in tables:
        for key,raw in db.execute('SELECT receipt_id,data FROM '+QA+' ORDER BY receipt_id'):
            r=strict_json(raw)
            if r['plan']['namespace']==namespace:result.append((QA,key,raw,r))
    for table in CORRECTIONS:
        if table in tables:
            for key,digest,raw in db.execute(f'SELECT request_id,digest,data FROM {table} WHERE namespace=? ORDER BY request_id',(namespace,)):
                r=strict_json(raw);_require(_digest(r)==digest,'retained correction bytes/digest changed')
                result.append((table,key,raw,r))
    return result


def provenance_pins(db,state,namespace,entry):
    """Verify the applied prompt-amendment chain; pin all correction authorities."""
    rows=_provenance_rows(db,namespace);qa=[r for table,key,raw,r in rows if table==QA]
    claims={c['claim_id']:c for c in state['claims']};by_original={}
    for table,key,raw,r in rows:
        if table==QA:
            plan=r['plan'];original=r['original_declaration'];reg=r['original_registration']
            _require(r['schema']=='jarvis-qa-checkpoint-eligibility/v1' and key==r['receipt_id']==_digest(plan)
                and _digest(original)==plan['declaration_sha256']==reg['declaration_sha256']
                and _digest(reg['binding'])==_digest(entry['binding'])
                and reg['run_id']==entry['run_id'] and reg['owner_id']==entry['owner_id'],
                'retained QA declaration authority differs')
            _require(plan['declaration_sha256'] not in by_original,'ambiguous retained declaration amendment chain')
            amended=json.loads(_json(original));declared={c['claim_id']:c for c in amended['claims']}
            ids=[t['claim_id'] for t in plan['targets']]
            _require(1<=len(ids)<=20 and len(set(ids))==len(ids) and set(ids)<=set(claims),'retained QA target scope differs')
            for t in plan['targets']:
                declared[t['claim_id']]['prompt']=t['new_prompt'];_verify_record_history(db,namespace,claims,t)
            _require(_digest(amended)==r['amended_declaration_sha256'],'retained prompt amendment is not exact')
            by_original[plan['declaration_sha256']]=(r['amended_declaration_sha256'],original,amended)
        else:
            _require(r['namespace']==namespace and r['request_id']==key,'retained correction scope differs')
            _verify_record_history(db,namespace,claims,r)
    if qa:
        terminals={x[0] for x in by_original.values()};starts=[k for k in by_original if k not in terminals]
        _require(len(starts)==1,'retained declaration chain missing or cyclic');cursor=starts[0];seen=set()
        initial=by_original[cursor][1]
        _require(_digest(initial['claims'])==entry['binding']['transition']['successor_claims_sha256']
            and _digest(initial['config'])==_digest(state['config']),'retained initial declaration differs from approved transition')
        while cursor in by_original:
            _require(cursor not in seen,'retained declaration chain cycle');seen.add(cursor)
            cursor=by_original[cursor][0]
        _require(len(seen)==len(qa) and cursor==state['declaration_sha256'],'retained amendment chain is incomplete')
    else:
        _require(_digest(state['declaration']['claims'])==entry['binding']['transition']['successor_claims_sha256'],
            'retained declaration lacks amendment provenance')
    return [{'table':table,'key':key,'sha256':_hash_text(raw)} for table,key,raw,r in rows]


def _verify_record_history(db,namespace,claims,record):
    c=claims[record['claim_id']];hist=[h for h in c['history'] if h['request_id']==record['request_id']]
    _require(len(hist)==1 and _digest(hist[0])==record['original_history_sha256']
        and hist[0].get('native_scan_sha256')==record['raw_scan_sha256'],'retained QA/correction history differs')
    row=db.execute(f"SELECT digest,data FROM {_research_table('native_scans',namespace)} WHERE request_id=?",(record['request_id'],)).fetchone()
    _require(row is not None and row[0]==record['raw_scan_sha256']==_digest(strict_json(row[1])),
        'retained QA/correction public scan differs')


def _pin_claim(c):
    return {'static_sha256':_digest(_claim_static(c)),'settled_claim_sha256':_digest(c) if c['current'] is None else None,
        'history_sha256':[_digest(h) for h in c['history']],'checkpoint_sha256':_digest(c['checkpoint']),
        'previous_turn_id':c['previous_turn_id'],'current':json.loads(_json(c['current']))}


def _verify_settlement(db,state,c,pin,history,gate,namespace):
    current=pin['current'];rid=current['request_id'];turn_id=current['turn_id']
    _require(history.get('request_id')==rid and history.get('turn_id')==turn_id
        and history.get('tool')==current['tool'] and history.get('phase')==current['phase']=='research'
        and history.get('terminal_confirmed') is True and history.get('hold_released') is True
        and gate['reservations'][rid]['status']=='finished' and c['previous_turn_id']==turn_id
        and c.get('actual_observation') is None and c['status']==history['outcome'],
        'retained removal lacks exact ordinary terminal release')
    row=db.execute(f"SELECT digest,data FROM {_research_table('native_scans',namespace)} WHERE request_id=?",(rid,)).fetchone()
    _require(row is not None and row[0]==history.get('native_scan_sha256')==_digest(strict_json(row[1])),
        'retained settlement lacks immutable full native scan')
    scan=strict_json(row[1]);identity={'request_id':rid,'hold_id':c['hold_id'],'thread_id':c['thread_id'],'turn_id':turn_id}
    _require(_digest({k:scan.get(k) for k in identity})==_digest(identity)
        and scan['run_id']==state['config']['run_id'] and scan['claim_id']==c['claim_id'] and scan['attempt']==c['attempt']
        and scan['model']==current['arguments']['model']=='gpt-6-luna' and scan['provider']=='openai'
        and scan['schema']=='jarvis-research-native-scan/v1' and scan['model_authority']=='exact_current_owner_runtime_context'
        and scan['checkpoint_sha256']==history['checkpoint_sha256'],'retained settlement model/generation differs')
    owner=scan['owner_terminal'];readback=scan['readback'];receipt=scan['native_scan'];turn=dict(receipt['native_turn'])
    _require(_digest({k:owner.get(k) for k in identity})==_digest(identity)
        and owner.get('terminal_confirmed') is True and owner.get('hold_released') is True
        and readback.get('verified') is True and readback.get('terminal') is True,'retained settlement owner authority missing')
    evidence=classify_native_turn(c['thread_id'],turn_id,{'id':turn_id,'status':turn.get('status'),
        'completedAt':turn.get('completedAt'),'startedAt':turn.get('startedAt'),'error':turn.get('native_error')},
        observed_thread_id=c['thread_id'],protocol_version=receipt['protocol_version'])
    _require(evidence['terminal_confirmed'] is True,'retained settlement lacks timestamp-strict native terminal evidence')
    finals=[e['envelope']['item'] for page in receipt['items']['pages'] for e in page['entries']
        if e.get('omitted') is not True and e.get('type')=='agentMessage']
    turn.update(items_scan_complete=True,item_count=receipt['item_count'],web_search_count=receipt['web_search_count'],
        items=[{k:v for k,v in finals[0].items() if k in PUBLIC_FIELDS['agentMessage']}] if finals else [])
    validate_public_scan(receipt,thread_id=c['thread_id'],turn=turn,model_provider='openai')
    # The immutable settlement row stores the verified owner model as a separate
    # projection, not a standalone owner/Host receipt. Reassemble only that exact
    # existing model projection for the shared classifier; invent no release.
    owner_evidence=classify_owner_terminal_receipt({**owner,'runtime_context':{'model':scan['model']}},
        **identity,model=scan['model'],native_turn=turn,observed_thread_id=c['thread_id'],
        protocol_version=receipt['protocol_version'])
    owner_status=owner_evidence['status'];native_status=evidence['status']
    _require(owner_evidence['terminal_confirmed'] is True and (owner_status==native_status or owner_status=='blocked'
        or native_status=='interrupted' and owner_status in {'cancelled','canceled'}),
        'retained owner marker/status contradicts exact native terminal outcome')
    digest=history['checkpoint_sha256']
    if digest is None:_require(_digest(c['checkpoint'])==pin['checkpoint_sha256'],'failed retained result changed checkpoint')
    else:
        row=db.execute(f"SELECT digest,data FROM {_research_table('checkpoints',namespace)} WHERE claim_id=? AND attempt=?",(c['claim_id'],c['attempt'])).fetchone()
        _require(row is not None and row[0]==digest==_digest(strict_json(row[1]))==_digest(c['checkpoint']),
            'retained accepted checkpoint lacks exact persisted origin')


def verify_retained(db,binding,entry):
    t=binding['transition'];state=read_state(db,t['old_namespace']);gate=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    _require(state['status']=='paused' and _digest(state['config'])==t['old_config_sha256']
        and state['declaration_sha256']==t['old_declaration_sha256'] and _digest(state['declaration'])==t['old_declaration_sha256'],
        'retained namespace resumed or changed declaration')
    _require(entry['binding']['schema']=='jarvis-shared-research-union/v3' and _digest(entry)==t['old_union_entry_sha256'],
        'retained admission entry changed')
    verify_prior(db,entry['binding'])
    _require(_digest(provenance_pins(db,state,t['old_namespace'],entry))==t['provenance_sha256'],
        'retained amendment/correction authority changed')
    _require(set(t['retained_claims'])=={c['claim_id'] for c in state['claims']} and len(state['claims'])==20,
        'retained jurisdiction roster changed')
    remaining=[]
    for c in state['claims']:
        pin=t['retained_claims'][c['claim_id']];current=c.get('current');baseline=pin['current'];history=c['history'];prefix=pin['history_sha256']
        _require(_digest(_claim_static(c))==pin['static_sha256'] and [_digest(h) for h in history[:len(prefix)]]==prefix,
            'retained identity or settled history changed')
        if baseline is None:
            _require(current is None and _digest(c)==pin['settled_claim_sha256'],'settled retained claim changed or gained work');continue
        if current is not None:
            _require(_digest(_current_static(current))==_digest(_current_static(baseline)) and len(history)==len(prefix)
                and _digest(c['checkpoint'])==pin['checkpoint_sha256'] and c['previous_turn_id']==pin['previous_turn_id'],
                'retained current generation changed or was replayed')
            for key in ('progress_sequence','stall_epoch','diagnostic_epoch'):
                _require(type(current.get(key)) is int and current[key]>=baseline[key],'retained progress regressed')
            _require(type(current.get('last_progress_at')) in (int,float) and current['last_progress_at']>=baseline['last_progress_at'],
                'retained progress timestamp regressed')
            _require(gate['reservations'][current['request_id']]['status'] in {'pending','started','unknown'},
                'retained current lost charged exposure')
            remaining.append(current['request_id'])
        else:
            _require(len(history)==len(prefix)+1,'retained disappearance lacks one exact settlement')
            _verify_settlement(db,state,c,pin,history[-1],gate,t['old_namespace'])
    return state,gate,remaining


def _claims_check(claims,binding):
    t=binding['transition'];_require(len(claims)==13 and len({c['claim_id'] for c in claims})==13
        and len({_scope(c) for c in claims})==13,'exact thirteen fresh jurisdictions required')
    excluded={_scope(x) for x in binding['excluded_jurisdictions']}
    _require(all(c['lane']=='base' and c.get('checkpoint') is None and c['claim_id'] not in t['excluded_claim_ids']
        and _scope(c) not in excluded for c in claims),'fresh ordinary claim overlaps retained/historical scope')
    _require(_digest(claims)==t['successor_claims_sha256'],'fresh declaration differs from reviewed thirteen')


def prepare_retained_union_transition(database,namespace,*,old_namespace,old_registry,claims,excluded_jurisdictions,excluded_native_thread_ids):
    database=exact_path(database);_research_table('run',namespace);_research_table('run',old_namespace)
    _require(namespace!=old_namespace,'fresh namespace required');claims=declarations(claims)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');original=read_state(db,None);retained=read_state(db,old_namespace)
        rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
        _require(len(rows)==1 and rows[0][0]==old_namespace,'exact retained admission registration required');entry=strict_json(rows[0][1])
        _require(entry['binding']['schema']=='jarvis-shared-research-union/v3' and retained['status']=='paused','paused v3 retained namespace required')
        store=object.__new__(ResearchStore);store.path=database;store.namespace=old_namespace;store.shared_union=entry['binding'];store._check_union(db,retained)
        gate=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0]);current=[c for c in retained['claims'] if c.get('current')];tasks=[c for c in original['claims'] if c.get('current')]
        external=set(entry['binding']['external_request_ids']);retained_ids={c['current']['request_id'] for c in current};task_ids={c['current']['request_id'] for c in tasks}
        _require(len(retained['claims'])==20 and all(c['lane']=='base' and c.get('thread_id') for c in retained['claims'])
            and len(current)==7 and all(c['current'].get('turn_id') and c['current']['phase']=='research' for c in current)
            and len(tasks)==5 and all(c['lane']=='task' for c in tasks) and len(external)==10
            and {r for r,v in gate['reservations'].items() if v['status']!='finished'}==external|retained_ids|task_ids,
            'exact retained seven plus TASK five and external ten required')
        exclusions={_scope(x):dict(x) for x in list(excluded_jurisdictions)+entry['binding']['excluded_jurisdictions']}
        for c in retained['claims']:exclusions[_scope(c)]={k:c[k] for k in ('country','jurisdiction_code')}
        threads=set(excluded_native_thread_ids)|set(entry['binding']['excluded_native_thread_ids'])|{c['thread_id'] for c in retained['claims']}
        prior_states=[read_state(db,None),retained]
        for (raw,) in db.execute('SELECT data FROM '+ARCHIVE):
            e=strict_json(raw)['entry'];prior_states.append(read_state(db,e['namespace']))
        records=provenance_pins(db,retained,old_namespace,entry)
        t={'old_namespace':old_namespace,'old_registry':str(exact_path(old_registry)),
            'original_registry':str(database.parent),'old_union_entry_sha256':_digest(entry),
            'old_config_sha256':_digest(retained['config']),'old_declaration_sha256':retained['declaration_sha256'],
            'database_identity':{'device':database.stat().st_dev,'inode':database.stat().st_ino},
            'retained_claims':{c['claim_id']:_pin_claim(c) for c in retained['claims']},
            'retained_request_ids':sorted(retained_ids),'retained_task_request_ids':sorted(task_ids),
            'provenance_pins':records,'provenance_sha256':_digest(records),'successor_claims_sha256':_digest(claims),
            'excluded_claim_ids':sorted({c['claim_id'] for s in prior_states for c in s['claims']}),
            'excluded_run_ids':sorted({s['config']['run_id'] for s in prior_states}),
            'excluded_owner_ids':sorted({s['config']['owner_id'] for s in prior_states}),
            'settlement_certificates':entry['binding']['transition']['settlement_certificates'],
            'old_controllers_must_remain_paused':True,'admission_registration_only':True}
        b=make_shared_union_binding({'state':original,'transport':{'gate':gate}},external_request_ids=sorted(external),
            excluded_jurisdictions=list(exclusions.values()),excluded_native_thread_ids=sorted(threads))
        binding={**b,'schema':SCHEMA,'transition':t};_claims_check(claims,binding);verify_retained(db,binding,entry);return binding


def verify_before(db,namespace,binding,claims):
    t=binding['transition'];_claims_check(claims,binding)
    rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
    _require(len(rows)==1 and rows[0][0]==t['old_namespace'],'retained registration CAS changed');entry=strict_json(rows[0][1])
    state,gate,remaining=verify_retained(db,binding,entry)
    old_store=object.__new__(ResearchStore);old_store.path=exact_path(t['original_registry'])/'research.sqlite3'
    old_store.namespace=t['old_namespace'];old_store.shared_union=entry['binding']
    old_store._check_union(db,state)
    _require(len(t['retained_request_ids'])==7 and set(t['retained_request_ids'])=={p['current']['request_id'] for p in t['retained_claims'].values() if p['current']},
        'retained seven identity scope changed')
    _require(set(gate['reservations'])==set(binding['gate_reservations']),'provider reservation history changed before registration')
    return state,gate,entry,remaining


def verify_after(db,binding):
    t=binding['transition'];row=db.execute('SELECT data FROM '+ARCHIVE+' WHERE namespace=?',(t['old_namespace'],)).fetchone()
    _require(row is not None,'retained admission archive missing');archive=strict_json(row[0])
    _require(archive.get('admission_registration_only') is True and archive['binding_sha256']==_digest(binding),
        'retained admission archive differs')
    return verify_retained(db,binding,archive['entry'])[2]


def register_retained_transition(store,db,config,claims):
    binding=store.shared_union;t=binding['transition'];check_inode(store);claims=declarations(claims)
    _require(config.hard_total==35 and config.reserved_external==10 and config.concurrency_policy=='healthy_batch'
        and config.deadline==binding['deadline'] and config.run_id not in t['excluded_run_ids']
        and config.owner_id not in t['excluded_owner_ids'],'fresh authorized same-deadline35 batch required')
    _claims_check(claims,binding)
    old=db.execute(f"SELECT data FROM {store.tables['run']} WHERE id=1").fetchone()
    if old:store._check_union(db,strict_json(old[0]));return
    _,_,entry,_=verify_before(db,store.namespace,binding,claims)
    provider=db.execute('SELECT * FROM jarvis_transport_gate ORDER BY 1').fetchall();receipts=db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall()
    db.execute('INSERT INTO '+ARCHIVE+' VALUES(?,?)',(t['old_namespace'],_json({'entry':entry,'binding_sha256':_digest(binding),'admission_registration_only':True})))
    db.execute('DELETE FROM jarvis_research_union WHERE namespace=?',(t['old_namespace'],))
    db.execute('INSERT INTO jarvis_research_union VALUES(?,?)',(store.namespace,_json({'namespace':store.namespace,
        'run_id':config.run_id,'owner_id':config.owner_id,'declaration_sha256':_digest({'config':asdict(config),'claims':claims}),'binding':binding})))
    store._check_union(db,None,register=True)
    _require(provider==db.execute('SELECT * FROM jarvis_transport_gate ORDER BY 1').fetchall()
        and receipts==db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall(),'provider state changed during admission-only transition')


def verify_retained_union_transition(database,namespace,binding,claims=None):
    database=exact_path(database);_require(binding.get('schema')==SCHEMA,'explicit retained-namespace v4 route required')
    store=object.__new__(ResearchStore);store.path=database;store.namespace=namespace;store.shared_union=binding
    store.tables={k:_research_table(k,namespace) for k in ('run','checkpoints','native_scans')};check_inode(store)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');row=db.execute('SELECT data FROM jarvis_research_union WHERE namespace=?',(namespace,)).fetchone()
        if row:return store._check_union(db,read_state(db,namespace))
        _require(claims is not None,'exact fresh thirteen claims required');_,gate,_,remaining=verify_before(db,namespace,binding,declarations(claims))
        external=set(binding['external_request_ids']);unresolved={r:v for r,v in gate['reservations'].items() if v['status']!='finished'}
        original_count=sum(c.get('current') is not None for c in read_state(db,None)['claims'])
        return {'potential':len(unresolved),'external_reserved':10,'original_predecessor_reserved':original_count,
            'retained_namespace_reserved':len(remaining),'predecessor_reserved':original_count+len(remaining),'sibling_reserved':0,
            'lanes':{lane:sum(v['lane']==lane for r,v in unresolved.items() if r not in external) for lane in ('base','task')},
            'shared_pending':dict(gate['pending']),'pending_transition':True,'provider_unchanged':True}
