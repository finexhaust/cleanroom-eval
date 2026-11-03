from copy import deepcopy
import json

import httpx
import pytest

from banking_v3.contract import digest, load_json
from banking_v3.evaluate import reference_rollout
from banking_v3.generate import build_case
from banking_v3.panel import parse_action, request, run_case, summarize


@pytest.fixture
def setup():
    case = build_case('panel-test', 'settlement', (False, False, True, False), 0, 'development')[0]
    _, actions, _ = reference_rollout(case)
    model = {'model': 'test-model', 'provider': 'deepseek', 'base_url': 'https://test.invalid',
             'reasoning_effort': 'max'}
    plan = {'turn_limit': 24, 'case_output_tokens': 32768, 'turn_output_tokens': 8192}
    return case, actions, model, plan


def test_live_boundary_replay_resume_and_freeze(tmp_path, setup):
    case, actions, model, plan = setup
    calls = []
    def handler(req):
        body = json.loads(req.content)
        # Only public observation and tool output, never gold or grading feedback.
        for message in body['messages']:
            assert '"checks"' not in message['content']
            assert '"_gold"' not in message['content']
        index = len(calls)
        calls.append(body)
        return httpx.Response(200, json={'model': 'test-model-snapshot',
            'usage': {'prompt_tokens': 100, 'completion_tokens': 20},
            'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps(actions[index])}}]})
    transport = httpx.MockTransport(handler)
    result = run_case(case, model, tmp_path, plan, 'fake', 'flow-test', transport)
    assert result['grade']['complete'] and result['replay_verified']
    assert result['actual_models'] == ['test-model-snapshot']
    assert run_case(case, model, tmp_path, plan, 'fake', 'flow-test', transport) == result
    assert len(calls) == len(actions)
    # Interrupted after last provider result: restore exclusively from saved turns.
    (tmp_path / model['model'] / case['id'] / 'record.json').unlink()
    restored = run_case(case, model, tmp_path, plan, 'fake', 'flow-test', transport)
    assert restored['grade'] == result['grade'] and len(calls) == len(actions)
    changed = {**plan, 'turn_limit': 25}
    with pytest.raises(ValueError, match='different case/protocol'):
        run_case(case, model, tmp_path, changed, 'fake', 'flow-test', transport)


def test_quota_error_not_retried_or_counted_as_model_completion(tmp_path, setup):
    case, _, model, plan = setup
    calls = []
    def handler(req):
        calls.append(req)
        return httpx.Response(429, json={'error': {'code': 'insufficient_quota'}})
    result = run_case(case, model, tmp_path, plan, 'fake', 'flow-test', httpx.MockTransport(handler))
    assert result['outcome'] == 'provider_error' and not result['grade']['complete']
    assert len(calls) == 1
    summary = summarize([result], 2)
    assert summary['assigned'] == 2 and summary['missing'] == 1 and summary['complete'] == 0


def test_partial_resume_rejects_modified_input(tmp_path, setup):
    case, actions, model, plan = setup
    count = 0
    def handler(req):
        nonlocal count
        if count == 1:
            raise KeyboardInterrupt()
        count += 1
        return httpx.Response(200, json={'model': 'test', 'usage': {'completion_tokens': 10},
            'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps(actions[0])}}]})
    with pytest.raises(KeyboardInterrupt):
        run_case(case, model, tmp_path, plan, 'fake', 'flow', httpx.MockTransport(handler))
    changed = {**plan, 'turn_output_tokens': 4000}
    with pytest.raises(ValueError, match='replayed model input'):
        run_case(case, model, tmp_path, changed, 'fake', 'flow', httpx.MockTransport(handler))


@pytest.mark.parametrize('content,finish,error', [
    ('```json\n{"tool":"inspect","args":{}}\n```', 'stop', None),
    ('not json', 'stop', 'invalid_json'),
    ('{"tool":"inspect","args":{}}', 'length', 'response_token_limit'),
    (None, 'stop', 'invalid_response'),
])
def test_response_errors_are_not_answer_repaired(content, finish, error):
    action, actual, _ = parse_action({'choices': [{'finish_reason': finish, 'message': {'content': content}}]})
    assert actual == error
    if error is None:
        assert action == {'tool': 'inspect', 'args': {}}


def test_service_retry_is_byte_identical(monkeypatch):
    monkeypatch.setattr('banking_v3.panel.time.sleep', lambda n: None)
    calls = []
    def handler(req):
        calls.append(req.content)
        return httpx.Response(503 if len(calls) < 3 else 200, json={'choices': []})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        row = request(client, 'https://test.invalid', {'messages': ['same']})
    assert len(calls) == 3 and len(set(calls)) == 1 and not row['provider_error']
