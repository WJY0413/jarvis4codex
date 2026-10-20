"""Proof-bound twenty-to-twenty namespace retirement on the same provider DB.

Only ordinary initialize registers this handoff. Preparation is read-only; no
provider import, lifecycle finish, owner adoption, or process action is present.
"""
from dataclasses import asdict
import hashlib
import json
import sqlite3

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import ResearchStore,_research_table,_digest,_json,_require,make_shared_union_binding
from jarvis_control.shared_union_transition import exact_path,semantic,declarations,ARCHIVE,check_inode

SCHEMA='jarvis-shared-research-union/v3'
AMENDMENTS='jarvis_research_contract_amendments'
CORRECTIONS=('jarvis_research_business_reconciliations','jarvis_research_exact_json_reconciliations')
RETIREMENTS='jarvis_research_identity_retirements'


def read_state(db,namespace):
    row=db.execute(f"SELECT data FROM {_research_table('run',namespace)} WHERE id=1").fetchone()
    _require(row is not None,'retained namespace missing');state=strict_json(row[0]);ResearchStore._validate_state(state)
    return state


def read_states(db,old_namespace):
    original,retired=read_state(db,None),read_state(db,old_namespace)
    _require(original['status']==retired['status']=='paused','both retained controllers must remain supported-paused')
    _require(len(retired['claims'])==20 and all(c['lane']=='base' and c.get('thread_id')
        and c.get('current') is None and c.get('actual_observation') is None for c in retired['claims']),
        'exact twenty created and settled ordinary predecessors required')
    row=db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()
    _require(row is not None,'existing shared provider gate required')
    return original,retired,strict_json(row[0])


def evidence_records(db,retired,namespace,entry):
    """Pin consumed authorities; never silently replace them with manifest claims."""
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    _require(AMENDMENTS in tables and RETIREMENTS in tables,'amendment and identity retirement authority required')
    amendments=[]
    for key,raw in db.execute(f'SELECT receipt_id,data FROM {AMENDMENTS} ORDER BY receipt_id'):
        record=strict_json(raw)
        if record['plan']['namespace']==namespace:
            _require(key==record['receipt_id']==_digest(record['plan']),'amendment receipt identity changed')
            amendments.append(record)
    _require(len(amendments)==1,'one exact applied business contract amendment required');amendment=amendments[0]
    _require(amendment['amended_declaration_sha256']==retired['declaration_sha256']
        and _digest(amendment['original_declaration'])==amendment['original_registration']['declaration_sha256']
        and _digest(amendment['original_registration']['binding'])==_digest(entry['binding'])
        and _digest(amendment['plan']['contracts'])==_digest(amendment['contracts']), 'amended declaration provenance changed')
    contracts=amendment['contracts'];claims={c['claim_id']:c for c in retired['claims']}
    _require(len(contracts)==20 and {x['execution_claim_id'] for x in contracts}==set(claims)
        and len({x['business_claim_id'] for x in contracts})==20,'exact twenty stable business contracts required')
    lineage={x['claim_id']:x for x in entry['binding']['transition']['claim_provenance']}
    for contract in contracts:
        c=claims[contract['execution_claim_id']];source=lineage[c['claim_id']]
        _require(contract['schema']=='jarvis-research-business-identity/v1'
            and all(contract[k]==c[k] for k in ('country','jurisdiction_code'))
            and contract['business_claim_id']==source['source_claim_id']
            and contract['source_namespace']==source['source_namespace']
            and contract['transition_sha256']==_digest(entry['binding']['transition']), 'stable business lineage changed')
    corrections=[]
    for table in CORRECTIONS:
        if table not in tables:continue
        for rid,digest,raw in db.execute(f'SELECT request_id,digest,data FROM {table} WHERE namespace=? ORDER BY request_id',(namespace,)):
            record=strict_json(raw);expected_schema='jarvis-settled-business-reconciliation/v1' if table==CORRECTIONS[0] else 'jarvis-exact-json-reconciliation/v1'
            _require(_digest(record)==digest and record['request_id']==rid and record.get('namespace')==namespace
                and record.get('schema')==expected_schema,'correction record digest/scope changed')
            c=claims[record['claim_id']];history=[h for h in c['history'] if h['request_id']==rid]
            _require(len(history)==1 and _digest(history[0])==record['original_history_sha256']
                and history[0].get('native_scan_sha256')==record['raw_scan_sha256'],'correction original history changed')
            scan=db.execute(f"SELECT digest,data FROM {_research_table('native_scans',namespace)} WHERE request_id=?",(rid,)).fetchone()
            _require(scan is not None and scan[0]==record['raw_scan_sha256']==_digest(strict_json(scan[1])), 'correction original public scan changed')
            corrections.append({'table':table,'request_id':rid,'digest':digest,'data':record})
    retirements=[strict_json(r[0]) for r in db.execute(f'SELECT data FROM {RETIREMENTS} WHERE namespace=? ORDER BY request_id',(namespace,))]
    _require(len(retirements)==1,'exact one consumed failed-before-send identity retirement required')
    # The shared close verifier checks the complete producer/consumer chain again.
    for r in retirements:
        _require(r['plan']['namespace']==namespace and r['old_claim']['claim_id'] in claims
            and r['native_terminal_fabricated'] is False and r['physical_owner_exit_confirmed'] is None,
            'identity retirement record mismatch')
    checkpoint_sources=[]
    for c in retired['claims']:
        digest=_digest(c.get('checkpoint'));found=[]
        for attempt,h,raw in db.execute(f"SELECT attempt,digest,data FROM {_research_table('checkpoints',namespace)} WHERE claim_id=? ORDER BY attempt",(c['claim_id'],)):
            _require(h==_digest(strict_json(raw)),'ordinary checkpoint digest changed')
            if h==digest:found.append({'kind':'native_checkpoint','attempt':attempt,'digest':h})
        for correction in corrections:
            if correction['data']['claim_id']==c['claim_id'] and _digest(correction['data']['checkpoint'])==digest:
                found.append({'kind':'typed_reconciliation','digest':correction['digest'],'request_id':correction['request_id']})
        declared=next(x for x in retired['declaration']['claims'] if x['claim_id']==c['claim_id'])
        if _digest(declared.get('checkpoint'))==digest:found.append({'kind':'inherited_declaration','digest':digest})
        _require(isinstance(c.get('checkpoint'),dict) and found,'latest checkpoint lacks immutable provenance')
        checkpoint_sources.append({'claim_id':c['claim_id'],'checkpoint_sha256':digest,'sources':found})
    return {'amendments':amendments,'corrections':corrections,'identity_retirements':retirements,'checkpoint_sources':checkpoint_sources}


def certificate_pins(original,retirements):
    from adapters.codex_app_server.finite_terminal_settlement import CERTIFICATE,read_settlement
    pins={}
    for c in original['claims']:
        for h in c['history']:
            authority=h.get('finite_terminal_authority') or h.get('finite_non_dispatch_authority')
            if not authority:continue
            root=exact_path(authority['certificate_root']);path=root/CERTIFICATE
            proof=read_settlement(root)
            identity={'request_id':h['request_id'],'hold_id':c['hold_id'],'thread_id':c['thread_id'],'turn_id':h.get('turn_id')}
            _require(isinstance(proof,dict) and _digest({k:proof.get(k) for k in identity})==_digest(identity)
                and proof.get('hold_released') is True and proof.get('physical_owner_exit_confirmed') is None,
                'retained finite settlement dependency changed')
            pins[str(path)]=authority['certificate_sha256']
    for r in retirements:
        t=r['plan']['target'];pins[str(exact_path(t['certificate_root'])/CERTIFICATE)]=t['certificate_sha256']
    _require(len(pins)==7,'exact six inherited and one retired-identity certificates required')
    result=[{'path':p,'sha256':h} for p,h in sorted(pins.items())]
    for pin in result:
        path=exact_path(pin['path']);_require(hashlib.sha256(path.read_bytes()).hexdigest()==pin['sha256']
            and read_settlement(path.parent) is not None,'strict certificate bytes/dependencies changed')
    return result


def source_provenance(claims,retired,records,old_namespace):
    old={c['claim_id']:c for c in retired['claims']};by_scope={(c['country'],c['jurisdiction_code']):c for c in old.values()}
    contracts={c['execution_claim_id']:c for c in records['amendments'][0]['contracts']}
    _require(len(claims)==20 and len({c['claim_id'] for c in claims})==20
        and len({(c['country'],c['jurisdiction_code']) for c in claims})==20,'exact twenty successor jurisdictions required')
    result=[]
    for c in claims:
        previous=by_scope.get((c['country'],c['jurisdiction_code']))
        _require(previous is not None and c['claim_id'] not in old
            and c['claim_id'] not in {x['business_claim_id'] for x in contracts.values()} and c['lane']==previous['lane']=='base'
            and c['jurisdiction']==previous['jurisdiction'] and _digest(c.get('checkpoint'))==_digest(previous.get('checkpoint')),
            'fresh exact jurisdiction and latest full checkpoint required')
        contract={**contracts[previous['claim_id']],'execution_claim_id':c['claim_id']}
        result.append({'claim_id':c['claim_id'],'source_namespace':old_namespace,'source_claim_id':previous['claim_id'],
            'checkpoint_sha256':_digest(previous['checkpoint']),'business_contract':contract})
    return result


def verify_retired(db,t,retired,entry):
    _require(_digest(semantic(retired))==t['old_state_semantic_sha256'],'retired namespace changed or resumed')
    from jarvis_control.shared_union_transition import verify_transition_after
    from jarvis_control.partial_predecessor import verify_transition_fences
    _require(entry['binding']['schema']=='jarvis-shared-research-union/v2','reviewed v2 predecessor required')
    verify_transition_after(db,entry['binding'])
    records=evidence_records(db,retired,t['old_namespace'],entry)
    _require(_digest(records)==t['evidence_records_sha256']==_digest(t['evidence_records'])
        and _digest(records['identity_retirements'])==_digest(t['identity_retirements']),
        'consumed amendment/correction/retirement authority changed')
    verify_transition_fences(t,retired)
    return records


def build_binding(db,original,retired,gate,entry,namespace,t,claims):
    records=verify_retired(db,t,retired,entry)
    current=[c for c in original['claims'] if c.get('current')]
    external=set(entry['binding']['external_request_ids']);tasks={c['current']['request_id'] for c in current}
    unresolved={rid for rid,r in gate['reservations'].items() if r['status']!='finished'}
    _require(len(current)==5 and all(c['lane']=='task' for c in current) and len(external)==10
        and unresolved==external|tasks,'exact external ten plus retained TASK five required; retired reservations must be finished')
    _require(all(gate['reservations'][rid]['owner']==original['config']['owner_id'] and gate['reservations'][rid]['lane']=='task'
        for rid in tasks),'retained TASK ownership changed')
    _require(not ({c['claim_id'] for c in claims} & {c['claim_id'] for c in original['claims']}), 'original predecessor execution identity reused')
    provenance=source_provenance(claims,retired,records,t['old_namespace'])
    excluded=set(entry['binding']['excluded_native_thread_ids'])|{c['thread_id'] for c in retired['claims']}
    excluded|={r['old_claim']['thread_id'] for r in records['identity_retirements']}
    binding=make_shared_union_binding({'state':original,'transport':{'gate':gate}},external_request_ids=sorted(external),
        excluded_jurisdictions=entry['binding']['excluded_jurisdictions'],excluded_native_thread_ids=sorted(excluded))
    t={**t,'successor_claims_sha256':_digest(claims),'claim_provenance':provenance,
        'settlement_certificates':certificate_pins(original,records['identity_retirements']), 'retained_task_request_ids':sorted(tasks)}
    return {**binding,'schema':SCHEMA,'transition':t}


def prepare_retired_union_transition(database,namespace,*,old_namespace,old_registry,claims,close_fences):
    database=exact_path(database);_research_table('run',namespace);_research_table('run',old_namespace)
    _require(namespace!=old_namespace,'fresh namespace required');claims=declarations(claims)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');original,retired,gate=read_states(db,old_namespace)
        rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
        _require(len(rows)==1 and rows[0][0]==old_namespace,'exact retiring namespace registration required');entry=strict_json(rows[0][1])
        store=object.__new__(ResearchStore);store.path=database;store.namespace=old_namespace;store.shared_union=entry['binding']
        store._check_union(db,retired)
        records=evidence_records(db,retired,old_namespace,entry)
        t={'old_namespace':old_namespace,'old_registry':str(exact_path(old_registry)),'original_registry':str(database.parent),
            'old_union_entry_sha256':_digest(entry),'old_state_semantic_sha256':_digest(semantic(retired)),
            'database_identity':{'device':database.stat().st_dev,'inode':database.stat().st_ino},'close_fences':close_fences,
            'evidence_records_sha256':_digest(records),'evidence_records':records,'identity_retirements':records['identity_retirements']}
        return build_binding(db,original,retired,gate,entry,namespace,t,claims)


def verify_before(db,namespace,binding,claims):
    t=binding['transition'];original,retired,gate=read_states(db,t['old_namespace'])
    rows=db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
    _require(len(rows)==1 and rows[0][0]==t['old_namespace'],'retiring union registration CAS changed');entry=strict_json(rows[0][1])
    _require(_digest(entry)==t['old_union_entry_sha256'],'retiring union entry changed')
    expected=build_binding(db,original,retired,gate,entry,namespace,t,claims)
    _require(_digest(expected)==_digest(binding),'transition differs from exact current proof-bound snapshot')
    return original,retired,gate,entry


def verify_after(db,binding):
    t=binding['transition'];original,retired,_=read_states(db,t['old_namespace'])
    row=db.execute(f'SELECT data FROM {ARCHIVE} WHERE namespace=?',(t['old_namespace'],)).fetchone()
    _require(row is not None,'retired twenty archive missing');archive=strict_json(row[0]);entry=archive['entry']
    _require(_digest(entry)==t['old_union_entry_sha256'] and archive['binding_sha256']==_digest(binding),'retired twenty archive changed')
    verify_retired(db,t,retired,entry)
    _require(_digest(certificate_pins(original,t['identity_retirements']))==_digest(t['settlement_certificates']), 'retained settlement certificate pins changed')


def register_retired_transition(store,db,config,claims):
    binding=store.shared_union;t=binding['transition'];check_inode(store);claims=declarations(claims)
    _require(config.hard_total==35 and config.reserved_external==10 and config.concurrency_policy=='healthy_batch'
        and config.deadline==binding['deadline'],'authorized same-deadline35 batch required')
    _require(_digest(claims)==t['successor_claims_sha256'],'successor declaration changed')
    old=db.execute(f"SELECT data FROM {store.tables['run']} WHERE id=1").fetchone()
    if old:store._check_union(db,strict_json(old[0]));return
    original,retired,gate,entry=verify_before(db,store.namespace,binding,claims)
    for (raw,) in db.execute(f'SELECT data FROM {ARCHIVE}'):
        archived=strict_json(raw)['entry']
        _require(config.run_id!=archived['run_id'] and config.owner_id!=archived['owner_id'], 'archived run or owner identity reused')
    _require(config.run_id not in {original['config']['run_id'],retired['config']['run_id']}
        and config.owner_id not in {original['config']['owner_id'],retired['config']['owner_id']},'fresh run and owner identities required')
    provider=db.execute('SELECT * FROM jarvis_transport_gate ORDER BY 1').fetchall()
    receipts=db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall()
    db.execute(f'INSERT INTO {ARCHIVE} VALUES(?,?)',(t['old_namespace'],_json({'entry':entry,'binding_sha256':_digest(binding),'retired_state_sha256':t['old_state_semantic_sha256']})))
    db.execute('DELETE FROM jarvis_research_union WHERE namespace=?',(t['old_namespace'],))
    new={'namespace':store.namespace,'run_id':config.run_id,'owner_id':config.owner_id,
        'declaration_sha256':_digest({'config':asdict(config),'claims':claims}),'binding':binding}
    db.execute('INSERT INTO jarvis_research_union VALUES(?,?)',(store.namespace,_json(new)))
    check_inode(store);store._check_union(db,None,register=True)
    _require(provider==db.execute('SELECT * FROM jarvis_transport_gate ORDER BY 1').fetchall()
        and receipts==db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall(),'provider bytes changed during namespace-only handoff')


def verify_retired_union_transition(database,namespace,binding,claims=None):
    database=exact_path(database);_require(binding.get('schema')==SCHEMA,'explicit v3 retired-twenty route required')
    store=object.__new__(ResearchStore);store.path=database;store.namespace=namespace;store.shared_union=binding
    store.tables={k:_research_table(k,namespace) for k in ('run','checkpoints','native_scans')};check_inode(store)
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=10) as db:
        db.execute('BEGIN');row=db.execute('SELECT data FROM jarvis_research_union WHERE namespace=?',(namespace,)).fetchone()
        if row:return store._check_union(db,read_state(db,namespace))
        _require(claims is not None,'exact successor claims required before registration')
        _,_,gate,_=verify_before(db,namespace,binding,declarations(claims))
        return {'potential':15,'external_reserved':10,'predecessor_reserved':5,'sibling_reserved':0,
            'lanes':{'base':0,'task':5},'shared_pending':dict(gate['pending']),'pending_transition':True,'provider_unchanged':True}
