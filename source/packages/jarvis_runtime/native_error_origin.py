"""Content-free native error provenance shared by owner and history readers.

Only structured fields are inspected. Page/message text never identifies a model
rate limit. Unknown origins remain vetoes; tool failures are not model failures.
"""
TOOL_TYPES = frozenset(('webSearch', 'commandExecution', 'mcpToolCall', 'dynamicToolCall'))


def structured_error(value):
    return (not isinstance(value, dict) or value.get('error') is not None
            or value.get('codexErrorInfo') is not None
            or value.get('type') in ('error', 'providerError', 'error_result', 'rate_limit_exceeded')
            or value.get('status') in ('failed', 'error'))


def _http_code(value):
    if not isinstance(value, dict):
        return None
    for key in ('httpStatusCode', 'statusCode', 'http_status', 'code'):
        code = value.get(key)
        if type(code) is int and 100 <= code <= 599:
            return code
    # Protocol-native provider error detail, not arbitrary business objects.
    info = value.get('codexErrorInfo')
    if isinstance(info, dict):
        for detail in info.values():
            if isinstance(detail, dict):
                code = detail.get('httpStatusCode')
                if type(code) is int and 100 <= code <= 599:
                    return code
    return None


def _error_chain(value):
    """Inspect only explicit error wrappers, never arbitrary nested payloads."""
    chain=[]
    while isinstance(value,dict) and len(chain)<8:
        chain.append(value)
        value=value.get('error')
    return chain, isinstance(value,dict)


def error_fact(path, value, origin='unknown'):
    chain, truncated = _error_chain(value)
    if truncated:
        origin='unknown'
    if any(v.get('type') in ('providerError','rate_limit_exceeded') or v.get('codexErrorInfo') is not None for v in chain):
        origin = 'native_request'
    code = next((code for v in chain if (code := _http_code(v)) is not None), None)
    return dict(path=path, origin=origin, http_status=code,
                classification=('model_request_429' if code == 429 else 'native_request_error')
                if origin == 'native_request' else
                ('web_tool_error' if origin == 'web_tool' else 'tool_error' if origin == 'native_tool' else 'unknown_error'))


def native_request_is_429(error):
    """Caller must supply the native request error field, never a tool result."""
    chain, truncated = _error_chain(error)
    return (not truncated and bool(chain) and all(v.get('provider','openai') == 'openai' for v in chain)
            and any(_http_code(v) == 429 or v.get('type') == 'rate_limit_exceeded' for v in chain))


def facts_block_transport(facts):
    return any(fact['origin'] not in ('web_tool', 'native_tool') for fact in facts)


def _item_facts(item):
    if not isinstance(item, dict):
        return [error_fact('item', None)]
    origin = ('web_tool' if item.get('type') == 'webSearch' else
              'native_tool' if item.get('type') in TOOL_TYPES else 'unknown')
    facts = []
    if item and structured_error(item):
        facts.append(error_fact('item.error', item, origin))
    results = item.get('results')
    if results is not None and not isinstance(results, list):
        facts.append(error_fact('item.results', None))
    elif isinstance(results, list):
        for result in results:
            if structured_error(result):
                # Malformed results have no trustworthy tool/provider origin.
                fact = error_fact('item.results.error', result, origin if isinstance(result, dict) else 'unknown')
                if fact not in facts:
                    if len(facts)<32:
                        facts.append(fact)
                    elif not any(f['path']=='item.results.overflow' for f in facts):
                        facts.append(error_fact('item.results.overflow',None))
    return facts


def transport_error_facts(entry):
    if not isinstance(entry, dict):
        return [error_fact('envelope', None)]
    facts = [error_fact('envelope.error', entry)] if structured_error(entry) else []
    return facts + _item_facts(entry.get('item'))


def notification_error_facts(method, params, turn, item):
    facts = []
    if params.get('error') is not None:
        origin = ('web_tool' if item.get('type') == 'webSearch' else 'native_tool' if item.get('type') in TOOL_TYPES else 'unknown') if (method or '').startswith('item/') else 'native_request' if method == 'error' else 'unknown'
        facts.append(error_fact('params.error', params['error'], origin))
    if turn.get('error') is not None:
        facts.append(error_fact('turn.error', turn['error'], 'native_request'))
    facts.extend(_item_facts(item))
    if method == 'error' and not facts:
        facts.append(error_fact('notification.error', None))
    return facts


# Compatibility name for existing scanners; single predicate implementation.
has_structured_error = structured_error
