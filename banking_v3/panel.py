"""Frozen, resumable model comparisons. All model inputs are environment outputs.

Run with python -m banking_v3.panel. Outputs contain private model traces and must
live outside the repository. No evaluator answers or rewards enter a model call.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import time
from urllib.parse import urlparse

import httpx
from dotenv import dotenv_values
from prefect import flow
from prefect.runtime import flow_run

from .contract import canonical, digest, load_json, load_package
from .environment import Environment
from .evaluate import replay
from .flow import code_commitment
from .provider import SYSTEM


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_bytes(canonical(value) + b'\n')
    temporary.replace(path)


def credential(name):
    """Load an existing credential in memory; never save it in a plan or trace."""
    config = {**dotenv_values(Path.home() / 'structric/env/config.local'),
              **dotenv_values(Path.home() / '.config/llm-keys.env'), **os.environ}
    if name == 'NEURALWATT_API_KEY':
        path = Path.home() / '.neurawatt_key'
        if path.is_file():
            return path.read_text().strip()
    value = config.get(name)
    if not value:
        raise ValueError(f'missing credential: {name}')
    return value


def payload_for(model, messages, remaining, per_turn):
    body = {'model': model['model'], 'messages': messages,
            'max_tokens': min(remaining, per_turn),
            'response_format': {'type': 'json_object'},
            'reasoning_effort': model['reasoning_effort']}
    if model['provider'] == 'deepseek':
        body['thinking'] = {'type': 'enabled'}
    if model['provider'] == 'openai':
        # gpt-5.x reasoning models reject max_tokens (unsupported_parameter); same budget, renamed.
        body['max_completion_tokens'] = body.pop('max_tokens')
    return body


def request(client, endpoint, body, attempts=3):
    """Retry only transport/service failures with identical input, never answers."""
    rows = []
    for attempt in range(attempts):
        started = time.monotonic()
        result = None
        try:
            response = client.post(endpoint + '/chat/completions', json=body)
            row = {'http_status': response.status_code,
                   'seconds': round(time.monotonic() - started, 4),
                   'request_id': response.headers.get('x-request-id')}
            try:
                result = response.json()
            except ValueError:
                row['error_class'] = 'non_json_response'
            error = (result or {}).get('error') or {}
            row['error_code'] = error.get('code') if isinstance(error, dict) else None
            retryable = response.status_code >= 500 or (
                response.status_code == 429 and row['error_code'] not in
                ('insufficient_quota', 'billing_hard_limit_reached', 'credit_balance_exhausted'))
            success = response.is_success
        except httpx.HTTPError as exc:
            row = {'error_class': type(exc).__name__,
                   'seconds': round(time.monotonic() - started, 4)}
            retryable, success = True, False
        rows.append(row)
        if success or not retryable or attempt == attempts - 1:
            return {'request_sha256': digest(body), 'attempts': rows,
                    'response': result if success else None,
                    'response_sha256': digest(result), 'provider_error': not success}
        time.sleep(2 ** (attempt + 1))
    raise AssertionError('unreachable')


def parse_action(body):
    """No answer repair. A single enclosing Markdown fence is syntax-only."""
    try:
        choice = body['choices'][0]
        if choice.get('finish_reason') == 'length':
            return None, 'response_token_limit', None
        message = choice['message']
        content = message.get('content')
        if not isinstance(content, str):
            return None, 'invalid_response', None
        text = content.strip()
        if text.startswith('```') and text.endswith('```'):
            text = text.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        action = json.loads(text)
        # The environment, not this adapter, validates action names and arguments.
        return action, None, {'role': 'assistant', 'content': content}
    except (KeyError, IndexError, TypeError, ValueError):
        return None, 'invalid_json', None


def run_case(case, model, root, plan, key, flow_id, transport=None):
    case_dir = root / model['model'] / case['id']
    record_path = case_dir / 'record.json'
    binding = {'plan_sha256': digest(plan), 'case_sha256': digest(case), 'model': model['model']}
    if record_path.exists():
        result = load_json(record_path)
        if any(result[k] != v for k, v in binding.items()):
            raise ValueError('refusing to resume a different case/protocol')
        return result
    case_dir.mkdir(parents=True, exist_ok=True)
    env = Environment(case)
    messages = [{'role': 'system', 'content': SYSTEM},
                {'role': 'user', 'content': canonical(env.observe()).decode()}]
    atomic_json(case_dir / 'initial-messages.json', messages)
    outcome, output_tokens = 'turn_limit', 0
    usages, actual_models, responses = [], set(), []
    started = time.monotonic()
    with httpx.Client(headers={'Authorization': 'Bearer ' + key}, timeout=240, transport=transport) as client:
        for turn in range(plan['turn_limit']):
            remaining = plan['case_output_tokens'] - output_tokens
            if remaining < 128:
                outcome = 'case_token_limit'
                break
            body = payload_for(model, messages, remaining, plan['turn_output_tokens'])
            path = case_dir / f'turn-{turn:02d}.json'
            if path.exists():
                row = load_json(path)
                if row['request_sha256'] != digest(body):
                    raise ValueError('saved response does not match replayed model input')
            else:
                row = request(client, model['base_url'], body)
                row['prefect_flow_run_id'] = flow_id
                atomic_json(path, row)
            responses.append({'path': path.name, 'sha256': digest(row)})
            if row['provider_error']:
                outcome = 'provider_error'
                break
            envelope = row['response']
            if not isinstance(envelope, dict):
                outcome = 'invalid_response'
                break
            usage = envelope.get('usage') or {}
            usages.append(usage)
            if type(usage.get('completion_tokens')) is not int:
                outcome = 'usage_missing'
                break
            output_tokens += usage['completion_tokens']
            actual_models.add(envelope.get('model', 'UNREPORTED'))
            action, failure, message = parse_action(envelope)
            if failure:
                outcome = failure
                break
            result = env.step(action)
            messages.extend([message, {'role': 'user', 'content': canonical(result).decode()}])
            if env.observe()['state']['closure']:
                outcome = 'closed'
                break
    transcript = env.transcript()
    grade = env.grade()
    regraded = replay(case, [t['request'] for t in transcript])
    if grade != regraded:
        raise AssertionError('deterministic replay mismatch')
    record = {**binding, 'case_id': case['id'], 'family': case['family'],
              'prefect_flow_run_id': flow_id, 'outcome': outcome, 'grade': grade,
              'transcript': transcript, 'responses': responses, 'usage': usages,
              'output_tokens': output_tokens, 'actual_models': sorted(actual_models),
              'seconds_this_invocation': round(time.monotonic() - started, 3),
              'replay_verified': True}
    atomic_json(record_path, record)
    return record


def summarize(rows, expected):
    """Denominators retain every assigned case, including provider failures."""
    outcomes = Counter(r['outcome'] for r in rows)
    failures = Counter(k for r in rows for k, passed in r['grade']['checks'].items() if not passed)
    return {'assigned': expected, 'recorded': len(rows), 'missing': expected - len(rows),
            'complete': sum(r['grade']['complete'] for r in rows),
            'mean_score': sum(r['grade']['score'] for r in rows) / expected,
            'outcomes': dict(outcomes), 'check_failures': dict(failures),
            'safety_cases': sum(bool(r['grade']['safety_violations']) for r in rows),
            'actual_models': sorted({m for r in rows for m in r['actual_models']}),
            'replay_verified': all(r['replay_verified'] for r in rows)}


@flow(name='bankingenv-v3-model-panel', persist_result=False, log_prints=True)
def run_panel(dataset: str, output: str, config: str, smoke: bool = False):
    os.umask(0o077)
    root = Path(output).resolve()
    if root.is_relative_to(Path(__file__).resolve().parents[1]):
        raise ValueError('private run artifacts must be outside the repository')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / '.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest, cases = load_package(dataset)
        cfg = load_json(config)
        if smoke:
            if manifest['split'] != 'development':
                raise ValueError('smoke tests must use the public development split')
            by_family = {}
            for c in cases:
                by_family.setdefault(c['family'], c)
            cases = list(by_family.values())
        models = cfg['models']
        if len({m['model'] for m in models}) != len(models):
            raise ValueError('duplicate model arm')
        for model in models:
            if urlparse(model['base_url']).scheme != 'https' or '/' in model['model']:
                raise ValueError('unsafe endpoint or model directory')
        if not 1 <= cfg['turn_limit'] <= 64 or not 1 <= cfg['workers_per_model'] <= 16:
            raise ValueError('invalid limits')
        plan = {'schema': 'bankingenv.model-panel-plan/v1',
                'case_index_sha256': manifest['case_index_sha256'],
                'case_ids': [c['id'] for c in cases], 'split': manifest['split'],
                'code_sha256': code_commitment(), 'system_sha256': digest(SYSTEM),
                'models': models, 'turn_limit': cfg['turn_limit'],
                'case_output_tokens': cfg['case_output_tokens'],
                'turn_output_tokens': cfg['turn_output_tokens'],
                'workers_per_model': cfg['workers_per_model'], 'samples_per_case': 1,
                'primary_metric': 'all_eight_checks_pass_no_safety_violation',
                'missing_results': 'zero retained in assigned denominator',
                'scope': 'public_smoke' if smoke else 'full_set',
                'temperature': 'provider default, thinking enabled, max effort',
                'retry_policy': '3 identical transport/service attempts; never answer-dependent',
                'history': 'initial observation and verbatim assistant text/tool outputs',
                'parser': 'JSON with optional single enclosing Markdown fence'}
        plan_path = root / 'plan.json'
        if plan_path.exists() and load_json(plan_path) != plan:
            raise ValueError('refusing protocol drift on resume')
        atomic_json(plan_path, plan)
        flow_id = str(flow_run.id)
        atomic_json(root / 'invocations' / f'{flow_id}.json',
                    {'started_at': datetime.now(timezone.utc).isoformat(), 'plan_sha256': digest(plan)})
        keys = {m['model']: credential(m['key_env']) for m in models}
        def arm(model):
            rows = []
            with ThreadPoolExecutor(max_workers=cfg['workers_per_model']) as pool:
                pending = [pool.submit(run_case, c, model, root, plan, keys[model['model']], flow_id)
                           for c in sorted(cases, key=lambda c: digest(c['id']))]
                for future in as_completed(pending):
                    rows.append(future.result())
                    summary = summarize(rows, len(cases))
                    atomic_json(root / model['model'] / 'progress.json', summary)
                    if len(rows) % 16 == 0 or len(rows) == len(cases):
                        print({'model': model['model'], 'recorded': len(rows),
                               'assigned': len(cases), 'complete': summary['complete'],
                               'outcomes': summary['outcomes']}, flush=True)
            return model['model'], summarize(rows, len(cases))
        with ThreadPoolExecutor(max_workers=len(models)) as pool:
            results = dict(pool.map(arm, models))
        report = {'schema': 'bankingenv.model-panel/v1', 'plan_sha256': digest(plan),
                  'prefect_flow_run_id': flow_id, 'models': results,
                  'evidence_class': 'LIVE_MODEL_RUN', 'human_review': 'NOT_RUN'}
        atomic_json(root / 'report.json', report)
        return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    run_panel(args.dataset, args.output, args.config, args.smoke)
