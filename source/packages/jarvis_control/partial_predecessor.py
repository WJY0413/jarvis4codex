"""Review-bound partial predecessor: retain owners and share one atomic gate.

This reader never initializes a store, imports a budget, claims PID liveness, or
settles an old owner. The controller checks the same binding atomically again.
"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import ResearchError, make_shared_union_binding, verify_shared_union


def require(value, message):
    if not value:
        raise ResearchError(message)


def same(left, right):
    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).digest()
    return digest(left) == digest(right)


def native_threads(value):
    result = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {'thread_id', 'threadId', 'prediction_thread_id', 'parent_thread_id'} and isinstance(child, str) and child:
                result.add(child)
            if key == 'excluded_native_thread_ids' and isinstance(child, list):
                result.update(x for x in child if isinstance(x, str) and x)
            result.update(native_threads(child))
    elif isinstance(value, list):
        for child in value:
            result.update(native_threads(child))
    return result


def absolute(value):
    path = Path(value)
    require(path.is_absolute() and path.resolve() == path and not path.is_symlink(), 'partial predecessor path traverses a link')
    return path


def verify_transition_fences(transition, retired_state):
    """Read-only exact fences for an immutable retired namespaced run.

    The transition itself is review-pinned by the controller binding. No stored
    flag substitutes for original owner, cancellation and supported close bytes.
    """
    from jarvis_control.formal_predecessor import verify_normal_close_fences
    registry = absolute(transition['old_registry'])
    def pinned(binding):
        path = absolute(binding['path'])
        raw = path.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == binding['sha256'], 'retired close proof bytes changed')
        return strict_json(raw.decode('utf-8'))
    return verify_normal_close_fences(registry, retired_state, transition['close_fences'], pinned,
        retirement_receipts=transition.get('identity_retirements', []))


class PartialPredecessor:
    def __init__(self, manifest):
        self.manifest = manifest
        self.spec = manifest.data['partial_predecessor']
        self.retired_twenty = self.spec.get('schema') == 'jarvis-partial-predecessor/v3'
        self.transition_route = self.spec.get('schema') in {'jarvis-partial-predecessor/v2', 'jarvis-partial-predecessor/v3'}
        require(self.spec.get('schema') in {'jarvis-partial-predecessor/v1', 'jarvis-partial-predecessor/v2', 'jarvis-partial-predecessor/v3'}, 'unsupported partial predecessor')
        require('formal_predecessor' not in manifest.data, 'partial shared gate cannot import a formal snapshot')
        self.root = absolute(self.spec['state_dir'])
        require(self.root != manifest.state_dir and self.root not in manifest.state_dir.parents
                and manifest.state_dir not in self.root.parents, 'fresh disjoint registry required')
        require(manifest.database == self.root / 'research.sqlite3' and manifest.database.is_file(),
                'partial route requires the exact existing predecessor provider database')
        require(self.spec.get("provider_key") == manifest.run_config.provider_key == "openai", "shared provider mismatch")
        stat = manifest.database.stat()
        require(same(self.spec.get("database_identity"), {"device": stat.st_dev, "inode": stat.st_ino}), "shared database inode changed")
        require(self.spec.get("predecessor_resume_forbidden") is True, "explicit retained predecessor no-resume rule required")
        self.original = self.pinned(self.spec['manifest'])
        self.snapshot = self.pinned(self.spec['snapshot'])
        self.binding = self.pinned(self.spec['union_binding'])
        require(same(self.binding, self.spec['union']), 'embedded shared binding differs from pinned artifact')
        require(self.original.get('schema') == 'jarvis-sustained-runner/v1'
                and self.original.get('state_dir') == str(self.root)
                and same(self.original['legacy'], manifest.data['legacy']), 'partial predecessor provenance mismatch')
        require(self.snapshot.get('schema') == 'jarvis-formal-predecessor-snapshot/v1'
                and self.snapshot['state']['status'] == 'paused', 'supported paused predecessor snapshot required')
        old_config = dict(self.snapshot['state']['config']); old_config.setdefault('concurrency_policy', 'healthy_ramp')
        require(same(old_config, self.original['run']), 'snapshot is not the exact reviewed predecessor run')
        require(manifest.run_config.deadline == old_config['deadline']
                and manifest.run_config.project == old_config['project']
                and manifest.run_config.run_id != old_config['run_id']
                and manifest.run_config.owner_id != old_config['owner_id'], 'partial run identity/deadline mismatch')
        require(manifest.run_config.hard_total == 35 and manifest.run_config.reserved_external == 10
                and manifest.run_config.concurrency_policy == 'healthy_batch', 'partial route requires reviewed35 batch')
        require(same(self.pinned(self.spec['claims']), [asdict(c) for c in manifest.claims]), 'reviewed fresh claim declarations changed')
        if self.transition_route:
            require(set(self.binding.get('external_request_ids', [])) == set(manifest.data['legacy']['reservations']),
                    'consolidated transition must retain exact original external ten')
            require(self.binding.get('schema') == ('jarvis-shared-research-union/v3' if self.retired_twenty else 'jarvis-shared-research-union/v2')
                    and len(manifest.claims) == 20 and all(c.lane == 'base' for c in manifest.claims),
                    'consolidated transition requires exactly20 ordinary claims')
            require(self.binding['transition']['successor_claims_sha256'] == hashlib.sha256(
                json.dumps([asdict(c) for c in manifest.claims], sort_keys=True, separators=(',', ':'),
                           ensure_ascii=False, allow_nan=False).encode()).hexdigest(),
                    'consolidated claims differ from proof-bound transition')
        else:
            require(1 <= len(manifest.claims) <= 14 and all(c.lane == 'base' and c.checkpoint is None for c in manifest.claims),
                    'partial route permits at most14 fresh ordinary claims')
            expected = make_shared_union_binding(self.snapshot,
                external_request_ids=manifest.data['legacy']['reservations'],
                excluded_jurisdictions=self.binding['excluded_jurisdictions'],
                excluded_native_thread_ids=self.binding['excluded_native_thread_ids'])
            require(same(expected, self.binding), 'shared union is not derived from the exact baseline')
        exclusions = self.pinned(self.spec['jurisdiction_exclusions'])
        require(isinstance(exclusions.get('excluded_codes'), list) and not exclusions.get('errors')
                and exclusions['excluded_codes'], 'reviewed cross-registry jurisdiction exclusions required')
        bound_codes = {x['jurisdiction_code'] for x in self.binding['excluded_jurisdictions']}
        allowed = {c.jurisdiction_code for c in manifest.claims} if self.transition_route else set()
        require(set(exclusions['excluded_codes']) - allowed <= bound_codes, 'historical jurisdiction exclusion omitted')
        self.excluded = set(self.binding['excluded_native_thread_ids'])
        inherited = native_threads(self.original)
        for path, digest in self.original['frozen_inputs'].items():
            require(manifest.data['frozen_inputs'].get(path) == digest, 'inherited frozen evidence omitted')
            if Path(path).suffix == '.json':
                inherited.update(native_threads(self.pinned({'path': path, 'sha256': digest})))
        require(inherited <= self.excluded, 'inherited native identity exclusion omitted')
        self.retained = self.pinned(self.spec['retained_host_proof'])
        require(self.retained.get('schema') == 'jarvis-retained-predecessor-host/v1'
                and self.retained.get('state_dir') == str(self.root)
                and self.retained.get('action') == 'retain' and self.retained.get('stop_requested') is False
                and isinstance(self.retained.get('host_instance_id'), str) and self.retained['host_instance_id']
                and type(self.retained.get('pid')) is int and self.retained['pid'] > 0,
                'reviewed retained-owner identity required')
        self.retired = None
        if self.transition_route:
            self.retired_root = absolute(self.binding['transition']['old_registry'])
            require(self.retired_root not in {self.root, manifest.state_dir}
                    and self.retired_root not in manifest.state_dir.parents
                    and manifest.state_dir not in self.retired_root.parents, 'fresh registry must be disjoint from retired namespace')
            self.retired_manifest = self.pinned(self.spec['retired_manifest'])
            require(self.retired_manifest.get('state_dir') == str(self.retired_root)
                    and same(self.retired_manifest['legacy'], manifest.data['legacy']), 'retired manifest provenance mismatch')
            for path, digest in self.retired_manifest['frozen_inputs'].items():
                require(manifest.data['frozen_inputs'].get(path) == digest, 'retired frozen evidence omitted')
                if Path(path).suffix == '.json':
                    require(native_threads(self.pinned({'path':path, 'sha256':digest})) <= self.excluded,
                            'retired native identity exclusion omitted')
            self.retired = self.pinned(self.spec['retired_host_proof'])
            require(self.retired.get('schema') == 'jarvis-retained-predecessor-host/v1'
                    and self.retired.get('state_dir') == str(self.retired_root)
                    and self.retired.get('action') == 'retain' and self.retired.get('stop_requested') is False
                    and isinstance(self.retired.get('host_instance_id'), str) and self.retired['host_instance_id']
                    and type(self.retired.get('pid')) is int and self.retired['pid'] > 0,
                    'exact retired Host retention identity required')
            from adapters.codex_app_server.finite_terminal_settlement import CERTIFICATE
            if self.retired_twenty:
                expected_certificates = self.binding['transition']['settlement_certificates']
                require(isinstance(expected_certificates, list) and len(expected_certificates) == 7
                        and len({c['path'] for c in expected_certificates}) == 7,
                        'exact six inherited plus one retired identity certificate pins required')
                expected_certificates = {c['path']:c['sha256'] for c in expected_certificates}
            else:
                expected_certificates = {}
                for claim in self.snapshot['state']['claims']:
                    authority = (claim['history'][-1].get('finite_terminal_authority') or {}) if claim['history'] else {}
                    if authority:
                        expected_certificates[str(absolute(authority['certificate_root']) / CERTIFICATE)] = authority['certificate_sha256']
                queued = self.binding['transition']['queued_non_dispatch']['certificate']
                expected_certificates[str(absolute(queued['root']) / CERTIFICATE)] = queued['sha256']
            self.certificates = self.spec['settlement_certificates']
            count = 7 if self.retired_twenty else 6
            require(len(expected_certificates) == count and len(self.certificates) == count
                    and same({p['path']:p['sha256'] for p in self.certificates}, expected_certificates),
                    'exact inherited finite settlement certificate pins required')
            for certificate in self.certificates:self.pinned(certificate)
            for fence in self.binding['transition']['close_fences']:
                for name in ('request', 'result', 'close', 'cancellation'):
                    self.pinned({'path':str(absolute(fence['root']) / (name+'.json')), 'sha256':fence[name+'_sha256']})
                for name in ('original_request', 'original_result', 'native_readback'):
                    if name in fence:self.pinned(fence[name])
        transport = strict_json(manifest.transport_path.read_text())
        require(absolute(transport['standard_bridge_package_root']) == Path(__file__).resolve().parents[1],
                'new private bridge must load the reviewed source')
        for key in ('db_path', 'health_path', 'lock_path', 'heartbeat_contracts_dir'):
            require(absolute(transport[key]).parent == manifest.state_dir.parent, 'private successor bridge path required')
        self.verify()

    def pinned(self, binding):
        path = absolute(binding['path']); digest = binding['sha256']
        require(self.manifest.data['frozen_inputs'].get(str(path)) == digest, 'partial evidence is not frozen')
        raw = path.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == digest, 'partial frozen evidence changed')
        return strict_json(raw.decode('utf-8'))

    def verify(self):
        stat = self.manifest.database.stat()
        require(same(self.spec["database_identity"], {"device": stat.st_dev, "inode": stat.st_ino}), "shared database inode changed")
        self.pinned(self.spec['manifest']); self.pinned(self.spec['snapshot']); self.pinned(self.spec['union_binding'])
        record = strict_json((self.root / 'runner-manifest.json').read_text())
        require(record.get('manifest_sha256') == self.spec['manifest']['sha256'], 'retained runner manifest changed')
        health = strict_json((self.root / 'hold-host.json').read_text())
        require(all(type(health.get(k)) is type(self.retained[k]) and health.get(k) == self.retained[k]
                    for k in ('state_dir', 'host_instance_id', 'pid')), 'retained Host identity changed')
        registries = [self.root]
        if self.transition_route:
            self.pinned(self.spec['retired_manifest']); self.pinned(self.spec['retired_host_proof'])
            from adapters.codex_app_server.finite_terminal_settlement import read_settlement
            for certificate in self.certificates:
                self.pinned(certificate)
                require(read_settlement(absolute(certificate['path']).parent) is not None,
                        'retained finite dispatch fence/cancellation/dependency changed')
            record = strict_json((self.retired_root / 'runner-manifest.json').read_text())
            require(record.get('manifest_sha256') == self.spec['retired_manifest']['sha256'], 'retired runner manifest changed')
            from adapters.codex_app_server import jarvis_hold_host_service as hosts
            health = strict_json((self.retired_root / 'hold-host.json').read_text())
            observed, now = hosts._parse_time(health.get('observed_at')), hosts._parse_time(hosts._now())
            require(all(type(health.get(k)) is type(self.retired[k]) and health.get(k) == self.retired[k]
                        for k in ('state_dir', 'host_instance_id', 'pid'))
                    and type(health.get('active_count')) is int and health['active_count'] == 0
                    and health.get('active_hold_ids') == [] and health.get('status') == 'ready'
                    and type(health.get('worker_capacity')) is int and health['worker_capacity'] == 50
                    and observed is not None and now is not None
                    and 0 <= (now-observed).total_seconds() <= hosts._HOST_HEALTH_MAX_AGE.total_seconds(),
                    'retired Host exact idle publication required; never replace it')
            if self.retired_twenty:
                require(all(type(health.get(k)) is int and health[k] == 0
                            for k in ('occupied_count', 'starting_reservation_count', 'backlog_count')),
                        'retired20 Host must have no occupied, starting or queued management work')
            registries.append(self.retired_root)
        # Publication identity is not process liveness or terminal authority.
        for registry in registries:
            for directory in ('task-holds', 'task-monitors'):
                for root in (registry / directory).glob('*'):
                    for name in ('request.json', 'ack.json', 'result.json'):
                        path = root / name
                        if path.is_file():
                            require(native_threads(strict_json(path.read_text())) <= self.excluded,
                                    'retained native identity outside reviewed exclusions')
        if self.retired_twenty:
            from jarvis_control.retired_union_transition import verify_retired_union_transition
            return verify_retired_union_transition(self.manifest.database, self.spec['namespace'], self.binding, claims=self.manifest.claims)
        if self.transition_route:
            from jarvis_control.shared_union_transition import verify_shared_union_transition
            return verify_shared_union_transition(self.manifest.database, self.spec['namespace'], self.binding, claims=self.manifest.claims)
        return verify_shared_union(self.manifest.database, self.spec['namespace'], self.binding)
