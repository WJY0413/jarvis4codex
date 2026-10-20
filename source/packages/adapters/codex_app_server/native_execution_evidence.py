"""Conservative classification of supported native history metadata.

This is evidence classification, never a release or global no-active-turn proof.
The caller must bind observed_thread_id to the successful RPC request, not model
output. For CLI 0.159.2, thread_history_projection.rs derives completedAt from
TurnComplete/TurnAborted; thread_processor.rs normalize_thread_turns_status can
rewrite InProgress to Interrupted without a terminal event. A null timestamp
may be a real legacy terminal event, but cannot distinguish that from projection.
"""
from collections.abc import Mapping

SCHEMA = 'jarvis-native-execution-evidence/v1'
SUPPORTED_PROTOCOL = '0.159.2'
SOURCE_URL = ('https://github.com/openai/codex/blob/rust-v0.159.2/'
              'codex-rs/app-server-protocol/src/protocol/thread_history_projection.rs')
TERMINAL = frozenset({'completed', 'failed', 'interrupted'})


def classify_native_turn(thread_id, turn_id, native_turn, *, observed_thread_id,
                         source='cross_client_history', protocol_version=None):
    """Return unknown unless exact, version-audited durable terminal evidence exists.

    A completed item, ACK, PID, absent error, elapsed time, and a local idle status
    are intentionally insufficient. This function never infers active execution.
    Unknown versions fail closed until their producer semantics are reviewed.
    """
    evidence = {'schema': SCHEMA, 'thread_id': thread_id, 'turn_id': turn_id,
                'source': source, 'protocol_version': protocol_version,
                'status': 'unknown', 'terminal_confirmed': False,
                'witness_source': None, 'observed_status': None,
                'scope': 'exact_turn_only', 'hold_released': False}
    def unknown(reason):
        return {**evidence, 'reason': reason}
    if (not isinstance(thread_id, str) or not thread_id or not isinstance(turn_id, str)
            or not turn_id or observed_thread_id != thread_id
            or not isinstance(native_turn, Mapping) or native_turn.get('id') != turn_id):
        return unknown('native response identity is not exactly bound')
    evidence['observed_status'] = native_turn.get('status')
    if source != 'cross_client_history' or protocol_version != SUPPORTED_PROTOCOL:
        return unknown('native source/version provenance is not audited')
    status = native_turn.get('status')
    if not isinstance(status, str) or status not in TERMINAL:
        return unknown('history metadata does not establish terminal execution')
    completed = native_turn.get('completedAt')
    started = native_turn.get('startedAt')
    if type(completed) is not int or completed <= 0:
        return unknown('normalized terminal status lacks durable terminal timestamp')
    if started is not None and (type(started) is not int or started < 0 or completed < started):
        return unknown('native lifecycle timestamps conflict')
    if native_turn.get('error') is not None and status == 'completed':
        return unknown('completed status conflicts with native error')
    return {**evidence, 'status': status, 'terminal_confirmed': True,
            'witness_source': 'persisted_native_terminal_event_timestamp',
            'completed_at': completed, 'started_at': started,
            'source_semantics': SOURCE_URL,
            'reason': 'exact terminal event metadata; no claim about other turns or owner exit'}


OWNER_TERMINAL_EVIDENCE = frozenset({
    'owner_turn_completed', 'owner_exact_terminal_readback', 'holder_readback_and_owned_exit',
})
OWNER_TERMINAL_STATUSES = frozenset({
    'completed', 'turn_limit_reached', 'failed', 'interrupted', 'cancelled', 'canceled', 'blocked',
})


def classify_owner_terminal_receipt(receipt, *, request_id, hold_id, thread_id,
                                    turn_id, model, native_turn, observed_thread_id,
                                    protocol_version):
    """Classify one exact current-owner receipt, without releasing its Hold.

    Inputs must come from the supported owner-bound adapter, never model output.
    The adapter/runner owns Host/state-root provenance; historical result files do
    not carry a standalone Host identity. This function never invents one.
    Physical release and a second unchanged owner read remain caller obligations.

    The typed readback fallback is emitted by the owning launcher only after the
    audited native timestamp classifier and a complete final scan succeed. Recheck
    that positive native witness here; a label alone or normalized interrupted
    history with a null timestamp cannot reproduce the fallback proof.
    """
    evidence = {'schema': 'jarvis-owner-terminal-evidence/v1', 'request_id': request_id,
        'hold_id': hold_id, 'thread_id': thread_id, 'turn_id': turn_id, 'model': model,
        'scope': 'exact_owner_generation', 'status': 'unknown',
        'terminal_confirmed': False, 'hold_released': False, 'witness_source': None}
    def unknown(reason):
        return {**evidence, 'reason': reason}
    expected = dict(request_id=request_id, hold_id=hold_id, thread_id=thread_id, turn_id=turn_id)
    if (not isinstance(receipt, Mapping) or not all(isinstance(x, str) and x for x in (*expected.values(), model))
            or any(receipt.get(k) != v for k, v in expected.items())
            or not isinstance(receipt.get('runtime_context'), Mapping)
            or receipt['runtime_context'].get('model') != model):
        return unknown('current owner request/hold/thread/turn/model binding mismatch')
    marker, status = receipt.get('terminal_evidence'), receipt.get('status')
    if (receipt.get('terminal_confirmed') is not True
            or not isinstance(marker, str) or marker not in OWNER_TERMINAL_EVIDENCE
            or not isinstance(status, str) or status not in OWNER_TERMINAL_STATUSES):
        return unknown('positive typed owner terminal receipt is absent')
    if marker == 'owner_exact_terminal_readback':
        if not isinstance(native_turn, Mapping):
            return unknown('owner readback fallback requires exact native scan')
        native = classify_native_turn(thread_id, turn_id,
            {'id': native_turn.get('turn_id'), 'status': native_turn.get('status'),
             'startedAt': native_turn.get('startedAt'), 'completedAt': native_turn.get('completedAt'),
             'error': native_turn.get('native_error')}, observed_thread_id=observed_thread_id,
            protocol_version=protocol_version)
        normalized_status = 'completed' if status == 'turn_limit_reached' else status
        compatible = (native['status'] == normalized_status or normalized_status == 'blocked'
                      or native['status'] == 'interrupted' and normalized_status in {'cancelled', 'canceled'})
        if (native['terminal_confirmed'] is not True or not compatible
                or native_turn.get('items_scan_complete') is not True):
            return unknown('owner readback fallback lacks audited positive timestamp/complete scan')
        if native['status'] == 'completed':
            items = native_turn.get('items')
            if (not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], Mapping)
                    or items[0].get('type') != 'agentMessage' or items[0].get('phase') != 'final_answer'
                    or not isinstance(items[0].get('text'), str) or not items[0]['text'].strip()):
                return unknown('completed owner readback fallback has no exact nonempty final')
    return {**evidence, 'status': 'completed' if status == 'turn_limit_reached' else status,
            'terminal_confirmed': True, 'witness_source': marker,
            'reason': 'exact typed owner terminal evidence; physical Hold release is separate'}
