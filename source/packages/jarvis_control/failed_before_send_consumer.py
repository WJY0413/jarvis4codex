"""One proof-bound failed-before-send retirement on an existing running controller.

No native dispatch or Host lifecycle action. A new Hold identity is published only
with the exact old reservation's typed non-dispatch settlement, in one SQLite CAS.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from datetime import datetime, timezone

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import ResearchStore,_digest,_json,_require,_research_table,_stamp
from jarvis_runtime.shared_transport_recovery import SharedTransportGate

SCHEMA='jarvis-failed-before-send-consumer/v1'
TABLE='jarvis_research_identity_retirements'
POLICY='permanent_distinct_hold_root_and_native_thread'


def path(value):
    p=Path(value);_require(p.is_absolute() and p.resolve()==p and not p.is_symlink(),'exact absolute unlinked path required');return p

def sha(p):return hashlib.sha256(path(p).read_bytes()).hexdigest()
def read(p):return strict_json(path(p).read_text())
def code_hash(root):return _digest({str(p.relative_to(root)):sha(p) for p in sorted(root.rglob('*.py')) if '__pycache__' not in p.parts})
def inode(p):return {'device':p.stat().st_dev,'inode':p.stat().st_ino}
def generation(c):
    current=c.get('current') or {}
    return {k:c.get(k) for k in ('claim_id','lane','hold_id','attempt','thread_id','previous_turn_id','checkpoint','history','phase')}|{'current':{k:current.get(k) for k in ('request_id','tool','arguments','phase','turn_id','prepared_at','sent_at')}}


def runtime(manifest):
    p=path(manifest['path']);_require(sha(p)==manifest['sha256'],'original manifest changed');m=read(p)
    _require(m.get('schema')=='jarvis-sustained-runner/v1','original supported runner required')
    for p,digest in m['frozen_inputs'].items():_require(sha(p)==digest,'frozen authorization changed')
    for name in ('launcher','transport'):_require(sha(m[name]['path'])==m[name]['sha256'],'runtime configuration changed')
    source=path(read(m['transport']['path'])['standard_bridge_package_root'])
    _require(code_hash(source)==m['source_code_sha256'],'loaded package changed')
    registry=path(m['state_dir'])
    _require(read(registry/'runner-manifest.json')['manifest_sha256']==manifest['sha256'],'retained runner changed')
    return m,registry


def host_identity(registry,old_hold,expected=None):
    h=read(registry/'hold-host.json');identity={k:h.get(k) for k in ('host_instance_id','pid','state_dir')}
    _require(type(identity['pid']) is int and identity['pid']>0 and isinstance(identity['host_instance_id'],str)
        and identity['host_instance_id'] and identity['state_dir']==str(registry),'exact retained Host identity required')
    _require(h.get('status') in {'ready','holding'} and h.get('worker_capacity')==50 and isinstance(h.get('active_hold_ids'),list)
        and old_hold not in h['active_hold_ids'] and 0<=time.time()-datetime.fromisoformat(h['observed_at']).timestamp()<=30,
        'fresh Host publication must exclude retired owner')
    if expected is not None:_require(_digest(identity)==_digest(expected),'retained Host generation changed')
    return identity


def state_and_registration(db,namespace,binding):
    state=read_row(db,_research_table('run',namespace));ResearchStore._validate_state(state)
    row=db.execute('SELECT data FROM jarvis_research_union WHERE namespace=?',(namespace,)).fetchone()
    _require(row is not None,'registered namespace required');reg=strict_json(row[0])
    _require(_digest(reg['binding'])==_digest(binding) and reg['declaration_sha256']==state['declaration_sha256'],'registered binding changed')
    return state,reg

def read_row(db,table):
    row=db.execute(f'SELECT data FROM {table} WHERE id=1').fetchone();_require(row is not None,'existing run required');return strict_json(row[0])


def certificate(root,claim):
    from adapters.codex_app_server.finite_terminal_settlement import read_settlement,CERTIFICATE,ARCHIVE_REQUEST
    proof=read_settlement(root);current=claim['current']
    wanted={'request_id':current['request_id'],'hold_id':claim['hold_id'],'thread_id':claim['thread_id'],'turn_id':None}
    _require(isinstance(proof,dict) and _digest({k:proof.get(k) for k in wanted})==_digest(wanted)
        and proof.get('release_kind')=='failed_before_send' and proof.get('evidence_source')=='finite_terminal_settlement_v2'
        and proof.get('successor_policy')==POLICY and proof.get('physical_owner_exit_confirmed') is None
        and proof.get('non_dispatch_settled') is True and proof.get('hold_released') is True
        and proof.get('terminal_confirmed') is False and proof.get('native_terminal_confirmed') is False,
        'strict failed-before-send certificate required')
    _require(_digest(proof['successor_exclusion'])==_digest({'registry':str(root.parents[1]),'root':str(root),'hold_id':claim['hold_id'],'thread_id':claim['thread_id']}),'old identity exclusion mismatch')
    request=read(root/ARCHIVE_REQUEST);ack=read(root/'ack.json');result=read(root/'result.json')
    _require(sha(root/ARCHIVE_REQUEST)==proof['original_request_sha256'] and sha(root/'ack.json')==proof['original_ack_sha256'], 'original request/ACK changed')
    _require(request.get('model')==current['arguments']['model']=='gpt-6-luna'
        and (ack.get('runtime_context') or {}).get('model')=='gpt-6-luna'
        and result.get('local_observer_stopped') is True and result.get('terminal_confirmed') is False
        and result.get('status')=='failed','original failed owner/model binding mismatch')
    return proof,{'certificate_sha256':sha(root/CERTIFICATE),'original_request_sha256':proof['original_request_sha256'],
        'original_ack_sha256':proof['original_ack_sha256'],'original_result_sha256':sha(root/'result.json')}


def target(db,state,claim_id,registry,namespace):
    c=next(c for c in state['claims'] if c['claim_id']==claim_id);current=c.get('current') or {}
    _require(state['status'] in {'running','paused'} and c['lane']=='base' and c['phase']=='research'
        and c['status'] in {'unknown','queued'} and c.get('actual_observation') is None
        and current.get('tool')=='jarvis_resume' and current.get('turn_id') is None and c.get('thread_id')
        and c.get('checkpoint') and c['attempt']<c['max_attempts'],'exact unresolved ordinary resume required')
    _require(current['arguments']['hold_id']==c['hold_id'] and current['arguments']['task_id']==c['thread_id']
        and current['arguments']['request_id']==current['request_id'] and current['phase']=='research','submitted request binding mismatch')
    root=registry/'task-holds'/c['hold_id'];proof,pins=certificate(root,c)
    gate=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    rid=current['request_id'];reservation=gate['reservations'].get(rid)
    _require(isinstance(reservation,dict) and reservation.get('owner')==state['config']['owner_id']
        and reservation.get('lane')=='base' and type(reservation.get('cost')) is int and reservation['cost']==1
        and reservation.get('status')=='unknown' and gate['pending'].get(rid)==1 and gate.get('probe')!=rid,
        'exact unresolved non-probe pending reservation required')
    key=_digest({'run':state['config']['run_id'],'claim':claim_id,'request':rid,'certificate':pins['certificate_sha256']})[:24]
    fresh='research-'+key
    _require(not (registry/'task-holds'/fresh).exists() and fresh not in {x['hold_id'] for x in state['claims']}
        and not any(r.startswith(fresh+'-a') for r in gate['reservations']),'fresh Hold identity collision')
    return c,{'claim_id':claim_id,'generation_sha256':_digest(generation(c)),'certificate_root':str(root),**pins,
        'reservation':reservation,'new_hold_id':fresh,'old_native_exclusion':proof['successor_exclusion'],'proof':proof}


def prepare(manifest_path,manifest_sha256,claim_id):
    manifest={'path':str(path(manifest_path)),'sha256':manifest_sha256};m,registry=runtime(manifest)
    partial=m['partial_predecessor'];database=path(partial['database']);namespace=partial['namespace']
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as db:
        db.execute('BEGIN');state,reg=state_and_registration(db,namespace,partial['union'])
        c,t=target(db,state,claim_id,registry,namespace)
        return {'schema':SCHEMA,'manifest':manifest,'database':str(database),'database_identity':inode(database),
            'namespace':namespace,'registry':str(registry),'run_id':state['config']['run_id'],'owner_id':state['config']['owner_id'],
            'declaration_sha256':state['declaration_sha256'],'registration_sha256':_digest(reg),'target':t,
            'host_identity':host_identity(registry,c['hold_id']),'operation_code_sha256':code_hash(Path(__file__).resolve().parents[1]),
            'prepared_at':_stamp(time.time()),'ready_eligibility':'fresh native identity; unfinished checkpoint continuation only'}


def replay(db,plan):
    _require(db.in_transaction and plan.get('schema')==SCHEMA,'transactional exact consumer plan required')
    _require(code_hash(Path(__file__).resolve().parents[1])==plan['operation_code_sha256'],'operation source changed')
    database=path(plan['database']);_require(_digest(inode(database))==_digest(plan['database_identity']),'database inode changed')
    m,registry=runtime(plan['manifest']);partial=m['partial_predecessor'];namespace=plan['namespace']
    _require(str(registry)==plan['registry'] and partial['database']==str(database) and partial['namespace']==namespace,'runtime scope changed')
    state,reg=state_and_registration(db,namespace,partial['union'])
    _require(state['config']['run_id']==plan['run_id'] and state['config']['owner_id']==plan['owner_id']
        and state['declaration_sha256']==plan['declaration_sha256'] and _digest(reg)==plan['registration_sha256'],'run binding CAS changed')
    claim,t=target(db,state,plan['target']['claim_id'],registry,namespace)
    _require(_digest(t)==_digest(plan['target']),'target generation/certificate CAS changed')
    host_identity(registry,claim['hold_id'],plan['host_identity'])
    original=json.loads(_json(claim));before=json.loads(_json(state));rid=claim['current']['request_id']
    gate_before=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    provider_receipts=db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall()
    db.execute(f'CREATE TABLE IF NOT EXISTS {TABLE}(namespace TEXT,request_id TEXT,data TEXT NOT NULL,PRIMARY KEY(namespace,request_id))')
    _require(db.execute(f'SELECT 1 FROM {TABLE} WHERE namespace=? AND request_id=?',(namespace,rid)).fetchone() is None,'one-shot retirement already consumed')
    history={'request_id':rid,'hold_id':claim['hold_id'],'thread_id':claim['thread_id'],'turn_id':None,
        'source_turn_id':claim['previous_turn_id'],'tool':claim['current']['tool'],'phase':'research','outcome':'non_dispatch_settled',
        'terminal_confirmed':False,'native_terminal_confirmed':False,'non_dispatch_settled':True,'hold_released':True,
        'physical_owner_exit_confirmed':None,'checkpoint_sha256':None,'finished_at':plan['prepared_at'],
        'finite_non_dispatch_authority':{'source':'finite_terminal_settlement_v2','kind':'failed_before_send',
            'certificate_root':t['certificate_root'],'certificate_sha256':t['certificate_sha256'],'successor_policy':POLICY}}
    claim['history'].append(history)
    claim.update(hold_id=t['new_hold_id'],thread_id=None,previous_turn_id=None,current=None,actual_observation=None,status='ready',last_error=None)
    SharedTransportGate.finish_existing_in_transaction(db,'openai',rid)
    expected=json.loads(_json(gate_before));expected['pending'].pop(rid);expected['reservations'][rid]['status']='finished'
    after=strict_json(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?',('openai',)).fetchone()[0])
    _require(_digest(after)==_digest(expected) and provider_receipts==db.execute('SELECT * FROM jarvis_transport_receipts ORDER BY 1,2,3').fetchall(),'provider mutation exceeded exact lifecycle finish')
    state['revision']+=1;ResearchStore._validate_state(state)
    db.execute(f"UPDATE {_research_table('run',namespace)} SET data=? WHERE id=1",(_json(state),))
    store=object.__new__(ResearchStore);store.path=database;store.namespace=namespace;store.shared_union=partial['union']
    store.tables={k:_research_table(k,namespace) for k in ('run','checkpoints','native_scans')};union=store._check_union(db,state)
    _require(all(_digest(a)==_digest(b) for a,b in zip(before['claims'],state['claims']) if a['claim_id']!=claim['claim_id']), 'another claim changed')
    _require(_digest(certificate(path(t['certificate_root']),original)[1])==_digest({k:t[k] for k in ('certificate_sha256','original_request_sha256','original_ack_sha256','original_result_sha256')}),'certificate changed before commit')
    host_identity(registry,original['hold_id'],plan['host_identity'])
    receipt={'schema':SCHEMA,'plan':plan,'old_claim':original,'history':history,'new_hold_id':claim['hold_id'],
        'checkpoint_sha256':_digest(claim['checkpoint']),'provider_expected_sha256':_digest(expected),'union':union,
        'native_terminal_fabricated':False,'physical_owner_exit_confirmed':None,'original_request_redispatched':False,
        'automatic_runner_finish':{'supported_by_loaded_version':False,'result':'retain Host for later reviewed lifetime reconciliation'},
        'historical_scan_access':{'legacy_getter_supports_rotated_identity':False,'reader':'failed_before_send_consumer.read_retired_scan','raw_scan_rewritten':False}}
    db.execute(f'INSERT INTO {TABLE} VALUES(?,?,?)',(namespace,rid,_json(receipt)))
    return receipt


def read_retired_scan(database,namespace,request_id,attempt):
    """Historical owner binding for rotated identities; original raw proof is unchanged."""
    with sqlite3.connect(path(database).as_uri()+'?mode=ro',uri=True) as db:
        db.execute('BEGIN');row=db.execute(f'SELECT data FROM {TABLE} WHERE namespace=? AND request_id=?',(namespace,request_id)).fetchone()
        _require(row is not None,'retirement receipt absent');retirement=strict_json(row[0]);old=retirement['old_claim']
        _require(retirement.get('schema')==SCHEMA and retirement['plan']['namespace']==namespace
            and retirement['plan']['target']['proof']['request_id']==request_id
            and _digest(generation(old))==retirement['plan']['target']['generation_sha256'],'retired identity index changed')
        row=db.execute(f"SELECT request_id,digest,data FROM {_research_table('native_scans',namespace)} WHERE claim_id=? AND attempt=?",(old['claim_id'],attempt)).fetchone()
        _require(row is not None,'original scan absent');value=strict_json(row[2]);history=[h for h in old['history'] if h['request_id']==row[0]]
        _require(len(history)==1 and _digest(value)==row[1]==history[0]['native_scan_sha256']
            and value['claim_id']==old['claim_id'] and value['run_id']==retirement['plan']['run_id']
            and value['model']==old['current']['arguments']['model'] and value['request_id']==row[0]
            and value['hold_id']==old['hold_id'] and value['thread_id']==old['thread_id']
            and value['turn_id']==history[0]['turn_id'] and value['checkpoint_sha256']==history[0]['checkpoint_sha256'],'archived original owner/scan binding differs')
        checkpoint=db.execute(f"SELECT digest,data FROM {_research_table('checkpoints',namespace)} WHERE claim_id=? AND attempt=?",(old['claim_id'],attempt)).fetchone()
        _require(checkpoint is not None and checkpoint[0]==value['checkpoint_sha256']==_digest(strict_json(checkpoint[1])),'original checkpoint differs')
        return value


def durable_new(p,value):
    p=path(p);raw=(json.dumps(value,ensure_ascii=False,indent=2)+'\n').encode();fd=os.open(p,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    fd=os.open(p.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def execute(plan,*,apply=False,intent=None):
    database=path(plan['database'])
    if not apply:
        with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as src,sqlite3.connect(':memory:') as db:
            src.backup(db);db.execute('BEGIN IMMEDIATE');receipt=replay(db,plan);db.rollback();return receipt
    _require(intent is not None and _digest(read(intent))==_digest({'schema':SCHEMA,'plan_sha256':_digest(plan),'automatic_retry':False}),'durable exact intent required')
    with sqlite3.connect(database) as db:db.execute('BEGIN IMMEDIATE');receipt=replay(db,plan);db.commit();return receipt


def lookup(plan):
    with sqlite3.connect(path(plan['database']).as_uri()+'?mode=ro',uri=True) as db:
        exists=db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(TABLE,)).fetchone()
        if not exists:return None
        row=db.execute(f'SELECT data FROM {TABLE} WHERE namespace=? AND request_id=?',(plan['namespace'],plan['target']['proof']['request_id'])).fetchone()
        return strict_json(row[0]) if row else None


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=('prepare','dry-run','apply'));p.add_argument('--manifest');p.add_argument('--manifest-sha256');p.add_argument('--claim-id');p.add_argument('--plan');p.add_argument('--plan-sha256');p.add_argument('--output',required=True);a=p.parse_args(argv)
    output=path(a.output);_require(not output.exists(),'output exists; inspect without replay')
    if a.action=='prepare':result=prepare(a.manifest,a.manifest_sha256,a.claim_id)
    else:
        _require(sha(a.plan)==a.plan_sha256,'approved exact plan required');plan=read(a.plan)
        if a.action=='apply':
            intent=Path(str(output)+'.intent.json');durable_new(intent,{'schema':SCHEMA,'plan_sha256':_digest(plan),'automatic_retry':False})
            try:result={'applied':True,'receipt':execute(plan,apply=True,intent=intent)}
            except BaseException as exc:
                try:observed=lookup(plan)
                except Exception:observed=None
                durable_new(output,{'applied':None,'error_type':type(exc).__name__,'observed_receipt':observed,'automatic_retry':False});return 1
        else:result={'applied':False,'receipt':execute(plan)}
    try:durable_new(output,result)
    except BaseException as exc:
        observed=None
        if a.action=='apply':
            try:observed=lookup(plan)
            except Exception:pass
        print(json.dumps({'output_failed':True,'error_type':type(exc).__name__,'commit_observed':observed is not None,'automatic_retry':False}));return 1
    print(json.dumps({'output':str(output),'sha256':sha(output),'applied':a.action=='apply'}));return 0

if __name__=='__main__':raise SystemExit(main())
