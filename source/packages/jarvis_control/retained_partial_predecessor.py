"""Read-only proof boundary for fresh jurisdictions beside retained unknown owners.

A stale Host publication binds identity only. Process exit never proves a native
turn ended. Every retained reservation stays in the same live provider database.
"""
from dataclasses import asdict
import hashlib
from pathlib import Path

from jarvis_control.file_task import strict_json
from jarvis_control.partial_predecessor import absolute, native_threads, require, same
from jarvis_control.sustained_research import ResearchStore, _digest

SCHEMA='jarvis-partial-predecessor/v4'


class RetainedPartialPredecessor:
    def __init__(self,manifest):
        self.manifest=manifest;self.spec=manifest.data['partial_predecessor'];s=self.spec
        require(s.get('schema')==SCHEMA and 'formal_predecessor' not in manifest.data,'explicit retained partial route required')
        self.root=absolute(s['state_dir']);self.old_root=absolute(s['retained_state_dir'])
        require(manifest.database==self.root/'research.sqlite3' and manifest.database.is_file(),'exact existing shared provider database required')
        require(s.get('provider_key')==manifest.run_config.provider_key=='openai','shared provider scope changed')
        require(s.get('predecessor_resume_forbidden') is True,'retained controllers must remain paused')
        for old in (self.root,self.old_root):
            require(old!=manifest.state_dir and old not in manifest.state_dir.parents
                and manifest.state_dir not in old.parents,'fresh disjoint private registry required')
        require(self.old_root!=self.root,'retained namespace registry must be distinct')
        self.original=self.pinned(s['manifest']);self.snapshot=self.pinned(s['snapshot']);self.binding=self.pinned(s['union_binding'])
        require(same(s['union'],self.binding) and self.binding.get('schema')=='jarvis-shared-research-union/v4','exact retained union binding required')
        require(self.original.get('schema')=='jarvis-sustained-runner/v1' and self.original.get('state_dir')==str(self.old_root)
            and same(self.original['legacy'],manifest.data['legacy']),'original retained run provenance changed')
        require(self.snapshot.get('schema')=='jarvis-retained-recovery-preparation/v1','reviewed retained state snapshot required')
        old=self.snapshot['old_state'];base=self.snapshot['base_state'];ResearchStore._validate_state(old);ResearchStore._validate_state(base)
        require(old['status']==base['status']=='paused' and same(old['config'],self.original['run']),'exact supported-paused run/config required')
        require(len([c for c in old['claims'] if c.get('current')])==7,'exact seven unknown native generations must remain reserved')
        require(manifest.run_config.deadline==old['config']['deadline'] and manifest.run_config.project==old['config']['project']
            and manifest.run_config.run_id not in {old['config']['run_id'],base['config']['run_id']}
            and manifest.run_config.owner_id not in {old['config']['owner_id'],base['config']['owner_id']},'fresh run/owner and unchanged deadline required')
        require(manifest.run_config.hard_total==35 and manifest.run_config.reserved_external==10
            and manifest.run_config.concurrency_policy=='healthy_batch','reviewed35 batch profile required')
        claims=[asdict(c) for c in manifest.claims]
        require(len(claims)==13 and all(c['lane']=='base' and c['checkpoint'] is None for c in claims)
            and same(self.pinned(s['claims']),claims),'exact thirteen fresh ordinary declarations required')
        transition=self.binding['transition']
        from jarvis_control.retained_union_transition import _pin_claim
        require(transition['old_config_sha256']==_digest(old['config'])
            and transition['old_declaration_sha256']==old['declaration_sha256']
            and same(transition['retained_claims'],{c['claim_id']:_pin_claim(c) for c in old['claims']}),
            'reviewed retained snapshot differs from exact union generation')
        require(transition['old_namespace']==self.snapshot['old_namespace']
            and transition['old_registry']==str(self.old_root)
            and transition['successor_claims_sha256']==_digest(claims),'retained transition/claims mismatch')
        require(set(self.binding['external_request_ids'])==set(manifest.data['legacy']['reservations']),'original external ten must remain reserved')
        exclusions=self.pinned(s['jurisdiction_exclusions'])
        require(isinstance(exclusions.get('excluded_codes'),list) and exclusions['excluded_codes'] and not exclusions.get('errors'),
            'complete reviewed historical assignment exclusions required')
        codes={x['jurisdiction_code'] for x in self.binding['excluded_jurisdictions']}
        require(set(exclusions['excluded_codes'])<=codes and not ({c['jurisdiction_code'] for c in claims}&codes),
            'new jurisdictions overlap or omit historical exclusions')
        self.excluded=set(self.binding['excluded_native_thread_ids']);inherited=native_threads(self.original)|native_threads(self.snapshot)
        for path,digest in self.original['frozen_inputs'].items():
            require(manifest.data['frozen_inputs'].get(path)==digest,'inherited frozen provenance omitted')
            if Path(path).suffix=='.json':inherited.update(native_threads(self.pinned({'path':path,'sha256':digest})))
        require(inherited<=self.excluded,'retained historical native identity exclusion omitted')
        prior=self.original['partial_predecessor'];self.base_manifest=self.pinned(prior['manifest'])
        require(self.base_manifest['state_dir']==str(self.root) and same(base['config'],self.base_manifest['run']),
            'original singleton run identity changed')
        self.base_host=self.pinned(prior['retained_host_proof']);self.old_host=self.pinned(s['retained_host_proof'])
        for proof,root in ((self.base_host,self.root),(self.old_host,self.old_root)):
            require(proof.get('schema')=='jarvis-retained-predecessor-host/v1' and proof.get('state_dir')==str(root)
                and proof.get('action')=='retain' and proof.get('stop_requested') is False
                and type(proof.get('pid')) is int and proof['pid']>0
                and isinstance(proof.get('host_instance_id'),str) and proof['host_instance_id'], 'exact retained registry identity required')
        pause=self.pinned(s['supported_pause']);exit_record=self.pinned(s['process_exit'])
        require(pause.get('run_id')==old['config']['run_id'] and pause.get('status')=='paused'
            and pause.get('admissions_open') is False and type(pause.get('reserved')) is int and pause['reserved']==7,'original supported pause receipt required')
        require(type(exit_record.get('exit_code')) is int and exit_record['exit_code']==137
            and type(exit_record.get('original_session')) is int
            and exit_record.get('native_turns_terminal_not_implied') is True,'reviewed original process-exit observation required')
        self.certificates=s['settlement_certificates']
        require(len(self.certificates)==7 and same(self.certificates,transition['settlement_certificates']),
            'exact inherited finite certificate pins required')
        transport=strict_json(manifest.transport_path.read_text())
        require(absolute(transport['standard_bridge_package_root'])==Path(__file__).resolve().parents[1],'fresh bridge must load reviewed code')
        for key in ('db_path','health_path','lock_path','heartbeat_contracts_dir'):
            require(absolute(transport[key]).parent==manifest.state_dir.parent,'fresh private bridge paths required')
        # Startup inventory only. Ongoing core checks bind every retained generation.
        # This is not platform-global fencing against unrelated external clients.
        for root in (self.root,self.old_root):
            for directory in ('task-holds','task-monitors'):
                for hold in (root/directory).glob('*'):
                    for name in ('request.json','ack.json','result.json'):
                        path=hold/name
                        if path.is_file():require(native_threads(strict_json(path.read_text()))<=self.excluded,
                            'retained native identity outside reviewed exclusions')
        self.verify()

    def pinned(self,binding):
        path=absolute(binding['path']);digest=binding['sha256']
        require(self.manifest.data['frozen_inputs'].get(str(path))==digest,'retained evidence must be review-pinned')
        raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==digest,'retained frozen evidence changed')
        return strict_json(raw.decode('utf-8'))

    def verify(self):
        from adapters.codex_app_server.finite_terminal_settlement import read_settlement
        from jarvis_control.retained_union_transition import verify_retained_union_transition
        stat=self.manifest.database.stat()
        require(same(self.spec['database_identity'],{'device':stat.st_dev,'inode':stat.st_ino}),'shared database inode changed')
        for key in ('manifest','snapshot','union_binding','supported_pause','process_exit','retained_host_proof'):
            self.pinned(self.spec[key])
        for root,proof,digest in ((self.root,self.base_host,self.original['partial_predecessor']['manifest']['sha256']),
                                  (self.old_root,self.old_host,self.spec['manifest']['sha256'])):
            record=strict_json((root/'runner-manifest.json').read_text())
            require(record.get('manifest_sha256')==digest,'retained loaded manifest identity changed')
            health=strict_json((root/'hold-host.json').read_text())
            require(all(type(health.get(k)) is type(proof[k]) and health.get(k)==proof[k]
                        for k in ('state_dir','host_instance_id','pid')),'retained registry Host identity changed')
            if root==self.old_root:
                require(isinstance(proof.get('host_started_at'),str) and health.get('host_started_at')==proof['host_started_at'],
                    'retained Host start identity changed')
            # Stale publication is intentionally NOT process liveness, terminal,
            # release or readiness authority. Unknown owners remain charged.
        for pin in self.certificates:
            self.pinned(pin);require(read_settlement(absolute(pin['path']).parent) is not None,'inherited finite fence dependency changed')
        return verify_retained_union_transition(self.manifest.database,self.spec['namespace'],self.binding,claims=self.manifest.claims)
