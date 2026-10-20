"""Explicit, permanent logical retirement of a finite, authoritative native set.

No automatic invocation, process stop, lock deletion, physical-exit assertion,
provider-budget mutation or successor admission. A prepared/partial operation
requires separate recovery review; it is never retried automatically.
"""
from __future__ import annotations
import hashlib
import json
import os
import re
from pathlib import Path
import time

from jarvis_runtime.cancellation import _dispatch_guard, cancel_management
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig
from .native_execution_evidence import classify_native_turn
from .jarvis_task_hold_host import _write_json

SCHEMA='jarvis-finite-terminal-settlement/v2'
CERTIFICATE='finite-terminal-settlement.json'
ARCHIVE_REQUEST='finite-terminal-original-request.json'
ARCHIVE_OWNER='finite-terminal-original-owner'
POLICY='permanent_distinct_registry_hold_and_native_thread'
FAILED_POLICY='permanent_distinct_hold_root_and_native_thread'

def successor_policy(kind):return FAILED_POLICY if kind=='failed_before_send' else POLICY
def successor_exclusion(root,identity,kind):
    result={'registry':str(root.parents[1]),'hold_id':identity['hold_id'],'thread_id':identity['thread_id']}
    if kind=='failed_before_send':result['root']=str(root)
    return result
AUDITED_OWNER_PROFILE={'adapters/codex_app_server/jarvis_task_hold_host.py': '7942d3b3564a90dba9f9e8c455f60c9cd2a9ad4c8632519569a7e739aa594e40', 'adapters/codex_app_server/jarvis_hold_host_service.py': 'e9eead34c944133309c593d8591b5cec978a2b12182028f2ff0b1f49821e0d8c', 'jarvis_runtime/cancellation.py': '50df45655aa15605a14152cf69e8b9ac1d350f7328100e6eea5e73ba0eff99cd', 'jarvis_runtime/jarvis_native_task_launcher.py': '2783337df7c74e405d9ba40a91b862609d86d8a5cd7024c31e2c1e81a3b48b48', 'jarvis_monitor/hold_turn_monitor.py': '7b1cff5678e2cec2034a4e1a147c9af5f256397f162e9c8df314ee9e62ea7ef1'}

AUDITED_V004_OWNER_PROFILE={'adapters/codex_app_server/jarvis_task_hold_host.py': '31f9ad32faa970701002edcc5c803150d15739ed06c396ea7851f76c1523f7a1', 'adapters/codex_app_server/jarvis_hold_host_service.py': '4e3ffe4482b248ef4415d7b739a422a6031476f642cdf44212c4389d836f2a2e', 'jarvis_runtime/cancellation.py': '50df45655aa15605a14152cf69e8b9ac1d350f7328100e6eea5e73ba0eff99cd', 'jarvis_runtime/jarvis_native_task_launcher.py': '3a41b7dd9423e9e74e1347c5c16ae95c10bb64800e88d050ddab54ef3588fd5a', 'jarvis_monitor/hold_turn_monitor.py': '7b1cff5678e2cec2034a4e1a147c9af5f256397f162e9c8df314ee9e62ea7ef1'}

def audited_profile(kind):
    return AUDITED_V004_OWNER_PROFILE if kind=="failed_before_send" else AUDITED_OWNER_PROFILE

class SettlementBlocked(RuntimeError):pass

def require(condition,message):
    if not condition:raise SettlementBlocked(message)

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def same(a,b):return canonical(a)==canonical(b)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def file_hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    require(not path.is_symlink(),'symlink evidence')
    if not path.exists():return {}
    value=json.loads(path.read_text());require(isinstance(value,dict),'nonobject evidence');return value

def snapshot(root):
    root=Path(root)
    require(root.is_absolute() and root.parent.name=='task-holds','exact absolute Hold root required')
    require(not any(p.is_symlink() for p in [root,root.parent,root.parents[1],root/'.user-host-claim']), 'symlink root/claim')
    names=['request.json','ack.json','result.json','request.lock','.user-host-claim/owner.json']
    for directory in ('history','dispatch-commits'):
        base=root/directory
        require(not base.is_symlink(),'symlink evidence directory')
        for path in sorted(base.rglob('*')):
            require(not path.is_symlink(),'symlink evidence descendant')
            if path.is_file():
                require(path.suffix=='.json','unexpected journal/history file')
                names.append(str(path.relative_to(root)))
    files={}
    for name in names:
        path=root/name;require(not path.is_symlink(),'symlink evidence')
        files[name]=file_hash(path) if path.is_file() else None
    claim=root/'.user-host-claim'
    if claim.exists():require(claim.is_dir() and sorted(p.name for p in claim.iterdir())==['owner.json'],'unexpected claim contents')
    return {'files':files,'claim_exists':claim.exists(),
            'claim_inode':[claim.stat().st_dev,claim.stat().st_ino] if claim.exists() else None}

def binding(root,kind):
    req=read(root/'request.json');ack=read(root/'ack.json')
    identity={k:req.get(k) for k in ('request_id','hold_id','thread_id')}
    require(all(isinstance(v,str) and v for v in identity.values()),'missing generation identity')
    require(all(ack.get(k)==identity[k] for k in ('request_id','hold_id')),'ACK generation differs')
    require(ack.get('thread_id')==identity['thread_id'] if kind=='accepted_terminal' else
            ack.get('thread_id') in (None,identity['thread_id']),'ACK native identity differs')
    require(req.get('mode')=='resume' and type(req.get('max_turns')) is int and req['max_turns']==1
            and req.get('auto_continue') is False and type(req.get('initial_turn_count',1)) is int
            and req.get('initial_turn_count',1)==1
            and not req.get('prediction_options') and not req.get('input_binding')
            and not req.get('dispatch_ancestors'),'only single-turn ordinary resume supported')
    require(not (root/'request.lock').exists(),'owner request lock remains')
    if kind!='failed_before_send':require(not read(root/'result.json'),'owner publication changed')
    if kind=='accepted_terminal':
        identity['turn_id']=ack.get('turn_id')
        require(isinstance(identity['turn_id'],str) and identity['turn_id'] and ack.get('status')=='holding'
                and ack.get('phase')=='turn_holding','exact accepted turn required')
    elif kind=='queued_non_dispatch':
        require(not ack.get('turn_id') and ack.get('status')=='accepted' and ack.get('phase')=='queued_for_user_host',
                'exact queued request required')
        identity['turn_id']=None
    elif kind=='failed_before_send':
        result=read(root/'result.json')
        for record in (ack,result):
            require(all(record.get(k)==v for k,v in identity.items())
                    and record.get('turn_id') in (None,'') and record.get('status')=='failed'
                    and record.get('phase')=='turn_starting' and record.get('terminal_confirmed') is False
                    and record.get('dispatch_evidence')=='unknown','exact failed-before-send receipt required')
        require(result.get('local_observer_stopped') is True,'original owner observer not confirmed stopped')
        require(isinstance(req.get('model'),str) and req['model']
                and (result.get('runtime_context') or {}).get('model')==req['model'],'failed owner model binding differs')
        identity['turn_id']=None
    else:raise SettlementBlocked('unsupported settlement kind')
    return req,ack,identity

def map_commits(root,identity,kind):
    records=[read(root/'ack.json'),read(root/'result.json')]+[read(p) for p in sorted((root/'history').rglob('*.json'))]
    mapped=[];seen=set()
    for path in sorted((root/'dispatch-commits').glob('*.json')):
        commit=read(path);keys=('request_id','hold_id','thread_id','client_user_message_id')
        bound={k:commit.get(k) for k in keys}
        require(all(isinstance(v,str) and v for v in bound.values()),'missing commit identity')
        require(path.name==hashlib.sha256(json.dumps(bound,sort_keys=True).encode()).hexdigest()+'.json'
                and commit.get('schema')=='jarvis-dispatch-commit/v1'
                and commit['hold_id']==identity['hold_id'] and commit['thread_id']==identity['thread_id']
                and commit['client_user_message_id']==commit['request_id']
                and commit.get('automatic_retry') is False
                and isinstance(commit.get('params_sha256'),str) and re.fullmatch(r'[a-f0-9]{64}',commit['params_sha256']),
                'unbound/continuation commit')
        ids={r['turn_id'] for r in records if all(r.get(k)==commit[k] for k in ('request_id','hold_id','thread_id'))
             and r.get('turn_id') and (r.get('dispatch_evidence')=='sent' or r.get('terminal_confirmed') is True or r.get('phase')=='turn_holding')}
        require(len(ids)==1,'unmapped or ambiguous committed operation')
        turn_id=next(iter(ids));require(turn_id not in seen,'aliased native operations');seen.add(turn_id)
        mapped.append({'journal':path.name,'journal_sha256':file_hash(path),'request_id':commit['request_id'],
                       'turn_id':turn_id,'params_sha256':commit['params_sha256']})
    require(mapped,'prior native set absent')
    current=[x for x in mapped if x['request_id']==identity['request_id']]
    require((len(current)==1 and current[0]['turn_id']==identity['turn_id']) if kind=='accepted_terminal' else not current,
            'current dispatch set does not match settlement kind')
    return mapped

def native_metadata(config,thread_id,*,client=None):
    """Supported read-only complete native metadata; no final or reasoning data."""
    owns_client=client is None
    if owns_client:
        client=AppServerClient(NativeTaskLauncherConfig(config));client.read_only=True
    require(client.read_only is True,"settlement requires a read-only observer")
    if not owns_client:
        require(client.process is not None and client.process.poll() is None,
                "batch observer must already be connected")
    deadline=time.monotonic()+60
    try:
        if owns_client:client.start(deadline=deadline)
        turns=[];cursor=None;seen=set()
        metadata=client.request('thread/read',{'threadId':thread_id,'includeTurns':False},deadline=deadline)
        require(isinstance(metadata.get('thread'),dict) and metadata['thread'].get('id')==thread_id,'native thread identity differs')
        while True:
            require(time.monotonic()<deadline and len(seen)<256,'native scan bound exceeded')
            params={'threadId':thread_id,'itemsView':'notLoaded','sortDirection':'desc','limit':100}
            if cursor is not None:params['cursor']=cursor
            page=client.request('thread/turns/list',params,deadline=deadline)
            require(isinstance(page.get('data'),list) and 'nextCursor' in page,'native scan incomplete')
            for turn in page['data']:
                require(isinstance(turn,dict) and turn.get('itemsView')=='notLoaded' and turn.get('items')==[], 'invalid native metadata')
                turns.append({k:turn.get(k) for k in ('id','status','startedAt','completedAt','error')})
                require(len(turns)<=10000,'native turn bound exceeded')
            cursor=page.get('nextCursor')
            if cursor is None:break
            require(isinstance(cursor,str) and cursor and cursor not in seen,'native cursor invalid')
            seen.add(cursor)
        require(time.monotonic()<deadline,'native scan deadline exceeded')
        return {'thread_id':thread_id,'protocol_version':client.cli_version,'complete':True,'turns':turns}
    finally:
        if owns_client:client.close()

def validate_native(native,identity,commits):
    require(native.get('thread_id')==identity['thread_id'] and native.get('complete') is True,'native scope incomplete')
    turns=native.get('turns');require(isinstance(turns,list),'native turns missing')
    ids=[t.get('id') for t in turns if isinstance(t,dict)]
    require(len(ids)==len(turns) and len(set(ids))==len(ids) and set(ids)=={c['turn_id'] for c in commits},
            'extra, missing or duplicate accepted native operation')
    for turn in turns:
        evidence=classify_native_turn(identity['thread_id'],turn['id'],turn,observed_thread_id=native['thread_id'],
                                      protocol_version=native.get('protocol_version'))
        require(evidence['terminal_confirmed'] is True,'native terminal event witness unresolved')

def expected_fence(identity,kind):
    return {**identity,'schema':'jarvis-permanent-terminal-fence/v2','permanent_retirement':True,
            'original_generation_fenced':True,'execution_state':'stopped','physical_owner_exit_confirmed':None,
            'successor_policy':successor_policy(kind),'settlement_kind':kind}

def inspect_candidate(root,kind):
    root=Path(root);before=snapshot(root);req,ack,identity=binding(root,kind)
    require(not req.get('resume_pause') and not (root/'cancellation.json').exists(),'already fenced/cancelled; separate recovery required')
    require(before['claim_exists']==(kind=='accepted_terminal'),'unexpected owner claim state')
    if before['claim_exists']:require(read(root/'.user-host-claim/owner.json').get('request_id')==identity['request_id'],'owner generation differs')
    commits=map_commits(root,identity,kind)
    require(same(snapshot(root),before),'inspection raced generation')
    return {'kind':kind,'identity':identity,'generation_digest':digest(before),'snapshot':before,'commits':commits}

def source_provenance(root,manifest_path,source_root,kind=None):
    manifest_path=Path(manifest_path);source_root=Path(source_root)
    manifest=read(manifest_path);pin=read(root.parents[1]/'runner-manifest.json')
    require(manifest.get('schema')=='jarvis-sustained-runner/v1'
            and manifest.get('state_dir')==str(root.parents[1])
            and pin.get('manifest_sha256')==file_hash(manifest_path),'owner launch provenance mismatch')
    profile=audited_profile(kind)
    for name,expected in profile.items():
        path=source_root/name
        require(path.is_absolute() and not path.is_symlink() and file_hash(path)==expected
                 ,'unaudited owner source profile')
    files={str(p.relative_to(source_root)).replace(os.sep,'/'):file_hash(p) for p in sorted(source_root.rglob('*.py')) if '__pycache__' not in p.parts}
    require(digest(files)==manifest.get('source_code_sha256'),'owner package differs from launch pin')
    transport=manifest.get('transport') or {}
    transport_path=Path(transport.get('path',''))
    require(file_hash(transport_path)==transport.get('sha256') and read(transport_path).get('standard_bridge_package_root')==str(source_root),'owner package root provenance differs')
    return {'manifest_path':str(manifest_path),'manifest_sha256':file_hash(manifest_path),
            'source_profile':dict(profile),
            'basis':'launch_manifest_pinned_source_not_process_introspection'}

def settle_candidate(root,config,*,kind,expected_generation_digest,close_request_id,owner_manifest,owner_source_root,native_client=None):
    """Explicit operation. Caller must separately review loaded-source and successor exclusion proofs."""
    require(isinstance(close_request_id,str) and bool(close_request_id),'exact close operation identity required')
    root=Path(root);plan=inspect_candidate(root,kind);before=plan['snapshot'];identity=plan['identity']
    provenance=source_provenance(root,owner_manifest,owner_source_root,kind=kind)
    require(plan['generation_digest']==expected_generation_digest,'reviewed generation changed')
    require(all(not os.path.lexists(root/n) for n in (CERTIFICATE,ARCHIVE_REQUEST,ARCHIVE_OWNER)),'partial settlement requires explicit recovery')
    req=read(root/'request.json');original=(root/'request.json').read_bytes()
    owner_bytes=(root/'.user-host-claim/owner.json').read_bytes() if before['claim_exists'] else None
    cancel_management(root,scope='hold',subject_id=identity['hold_id'],request_id=identity['request_id'],
                      close_request_id=close_request_id,original=original)
    with _dispatch_guard(root,req) as roots:
        require(set(roots)=={root},'cross-root alias unsupported')
        require(same(snapshot(root),before),'generation changed before settlement')
        commits=map_commits(root,identity,kind)
        require(same(commits,plan['commits']),'commit set changed')
        native=native_metadata(config,identity['thread_id'],client=native_client);validate_native(native,identity,commits)
        require(same(snapshot(root),before),'generation changed during native read')
        fence=expected_fence(identity,kind)
        proof={'schema':SCHEMA,'phase':'prepared','kind':kind,'identity':identity,'original_snapshot':before,
               'generation_digest':expected_generation_digest,'commits':commits,'native':native,'fence':fence,
               'physical_owner_exit_confirmed':None,'successor_policy':successor_policy(kind),'owner_source_provenance':provenance,
               'successor_exclusion':successor_exclusion(root,identity,kind),
               'cancellation_sha256':file_hash(root/'cancellation.json')}
        with (root/ARCHIVE_REQUEST).open('xb') as handle:
            handle.write(original);handle.flush();os.fsync(handle.fileno())
        _write_json(root/CERTIFICATE,proof,durable=True)
        _write_json(root/'request.json',{**req,'resume_pause':fence},durable=True)
        if before['claim_exists']:
            claim=root/'.user-host-claim'
            require(not claim.is_symlink() and [claim.stat().st_dev,claim.stat().st_ino]==before['claim_inode']
                    and (claim/'owner.json').read_bytes()==owner_bytes
                    and sorted(p.name for p in claim.iterdir())==['owner.json'],'exact claim CAS changed')
            claim.rename(root/ARCHIVE_OWNER)
        require(not (root/'.user-host-claim').exists(),'claim raced retirement')
        proof['fenced_request_sha256']=file_hash(root/'request.json');proof['phase']='retired'
        _write_json(root/CERTIFICATE,proof,durable=True)
        directory=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
        result=read_settlement(root)
        require(result is not None,'settlement readback incomplete')
        return result

def read_settlement(root):
    """Strict typed proof consumer; old logical fences alone remain unresolved."""
    root=Path(root)
    try:
        proof=read(root/CERTIFICATE)
        if not proof:return None
        require(proof.get('schema')==SCHEMA and proof.get('phase')=='retired'
                and proof.get('physical_owner_exit_confirmed') is None and proof.get('successor_policy')==successor_policy(proof.get('kind')),'invalid settlement type')
        observed=snapshot(root);require(not observed['claim_exists'],'claim reappeared')
        provenance=proof['owner_source_provenance']
        require(provenance.get('source_profile')==audited_profile(proof['kind'])
                and read(root.parents[1]/'runner-manifest.json').get('manifest_sha256')==provenance.get('manifest_sha256'),
                'owner source provenance changed')
        req,ack,identity=binding(root,proof['kind']);require(same(identity,proof['identity']),'settlement identity differs')
        require(same(proof.get('successor_exclusion'),successor_exclusion(root,identity,proof['kind'])),'permanent successor exclusion differs')
        original=proof['original_snapshot'];restored={'files':dict(observed['files']),
            'claim_exists':original['claim_exists'],'claim_inode':original['claim_inode']}
        old=read(root/ARCHIVE_REQUEST)
        require(same(proof['fence'],expected_fence(identity,proof['kind'])),'exact retirement fence differs')
        require(same(req,{**old,'resume_pause':proof['fence']}) and req.get('resume_pause',{}).get('permanent_retirement') is True
                and file_hash(root/'request.json')==proof['fenced_request_sha256'],'fenced request differs')
        restored['files']['request.json']=file_hash(root/ARCHIVE_REQUEST)
        archive=root/ARCHIVE_OWNER
        if original['claim_exists']:
            require(not archive.is_symlink() and archive.is_dir() and not (archive/'owner.json').is_symlink()
                    and sorted(p.name for p in archive.iterdir())==['owner.json']
                    and [archive.stat().st_dev,archive.stat().st_ino]==original['claim_inode'],'archived owner CAS differs')
            restored['files']['.user-host-claim/owner.json']=file_hash(archive/'owner.json')
        else:require(not archive.exists(),'unexpected owner archive')
        require(same(restored,original) and digest(original)==proof['generation_digest'],'original evidence changed')
        marker=read(root/'cancellation.json')
        require(marker.get('request_id')==identity['request_id'] and marker.get('subject_id')==identity['hold_id']
                and marker.get('dispatch_cancelled') is True and file_hash(root/'cancellation.json')==proof['cancellation_sha256'],'cancellation changed')
        commits=map_commits(root,identity,proof['kind']);require(same(commits,proof['commits']),'journal changed')
        validate_native(proof['native'],identity,commits)
        require(same(snapshot(root),observed),'settlement read raced state')
        accepted=proof['kind']=='accepted_terminal'
        native_status=next((t['status'] for t in proof['native']['turns'] if t['id']==identity['turn_id']),None)
        return {**identity,'status':native_status if accepted else 'not_started','terminal_confirmed':accepted,'native_terminal_confirmed':accepted,
                'non_dispatch_settled':not accepted,'hold_released':True,'logical_owner_retired':True,
                'physical_owner_exit_confirmed':None,'release_kind':proof['kind'],
                'successor_policy':successor_policy(proof['kind']),'successor_exclusion':proof['successor_exclusion'],
                'evidence_source':'finite_terminal_settlement_v2','settlement_certificate_sha256':file_hash(root/CERTIFICATE),
                'original_ack_sha256':original['files']['ack.json'],
                'original_result_sha256':original['files']['result.json'],
                'original_request_sha256':original['files']['request.json'],
                'scheduling_closed':True,'management_closed':True,'external_execution_unresolved':False}
    except (KeyError,TypeError,ValueError,OSError,SettlementBlocked):return None
