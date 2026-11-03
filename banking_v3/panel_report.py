"""Audit completed private model panels and rank case mechanisms for review."""
from __future__ import annotations

import argparse
import html
from collections import Counter, defaultdict
from pathlib import Path
import statistics

from prefect import flow
from prefect.runtime import flow_run

from .contract import CRITERIA, digest, load_json, load_package
from .evaluate import replay
from .flow import code_commitment
from .panel import atomic_json, summarize


def audit_record(case, path, plan):
    row = load_json(path)
    if row['plan_sha256'] != digest(plan) or row['case_sha256'] != digest(case):
        raise ValueError('case/protocol binding mismatch')
    for entry in row['responses']:
        turn = load_json(path.parent / entry['path'])
        if digest(turn) != entry['sha256'] or digest(turn['response']) != turn['response_sha256']:
            # Unsuccessful requests hash the provider error envelope, which is not
            # retained as response. Only verify raw-body commitment on success.
            if digest(turn) != entry['sha256'] or not turn['provider_error']:
                raise ValueError('provider response commitment mismatch')
    replayed = replay(case, [t['request'] for t in row['transcript']])
    if replayed != row['grade']:
        raise ValueError('grade does not replay')
    return row


def diagnostics(row):
    grade = row['grade']
    actions = row['transcript']
    executed = any(isinstance(t['request'], dict) and t['request'].get('tool') == 'execute' and t['response'].get('ok') for t in actions)
    return {
        'closed_incorrect': row['outcome'] == 'closed' and not grade['complete'],
        'executed_wrong_value': executed and not grade['checks']['calculation'],
        'unsafe_attempt': bool(grade['safety_violations']),
        'tool_rejection': any(not t['response'].get('ok') for t in actions),
    }


def semantic_tags(row, case, answer):
    """Describe observed actions/values, without inferring hidden model motives."""
    assessments=[t['request']['args'] for t in row['transcript']
                 if isinstance(t['request'],dict) and t['request'].get('tool')=='assess' and t['response'].get('ok')]
    final=assessments[-1] if assessments else {}
    current=next(d['data'] for d in case['documents'] if d['kind']=='record' and d['status']=='CURRENT')
    unsafe='unauthorized_execution_attempt' in row['grade']['safety_violations']
    return {
        'attempted_execution_without_required_approval': unsafe and answer['expected']['route']=='approve',
        'attempted_execution_with_missing_evidence': unsafe and answer['expected']['route']=='escalate',
        'closed_after_unauthorized_attempt': unsafe and row['outcome']=='closed',
        'risk_amount_used_as_case_value': bool(final) and final.get('value')==current['risk_amount'] and final.get('value')!=answer['expected']['target'],
        'closed_with_wrong_calculation': row['outcome']=='closed' and not row['grade']['checks']['calculation'],
    }


@flow(name='bankingenv-v3-panel-audit', persist_result=False, log_prints=True)
def analyze(dataset: str, evaluator: str, panel: str, output: str, supplemental: str | None = None, repeats: str | None = None):
    root, out = Path(panel), Path(output)
    if out.resolve().is_relative_to(Path(__file__).resolve().parents[1]):
        raise ValueError('detailed case report must remain outside the repository')
    if out.exists():
        raise ValueError('audit output must be new')
    manifest, cases = load_package(dataset)
    answers = {a['case_id']: a for a in load_json(evaluator)}
    plan = load_json(root / 'plan.json')
    if manifest['case_index_sha256'] != plan['case_index_sha256']:
        raise ValueError('wrong dataset')
    if set(plan['case_ids']) != {c['id'] for c in cases}:
        raise ValueError('not a complete evaluation plan')
    if set(answers) != {c['id'] for c in cases} or any(answers[c['id']]['case_sha256'] != digest(c) for c in cases):
        raise ValueError('evaluator index does not bind this dataset')
    plans = [plan]
    locations = {m['model']: (root, plan) for m in plan['models']}
    if supplemental:
        extra_root = Path(supplemental)
        extra = load_json(extra_root / 'plan.json')
        for key in ('case_ids','case_index_sha256','code_sha256','system_sha256','turn_limit','case_output_tokens','turn_output_tokens','samples_per_case','parser','history'):
            if extra.get(key) != plan.get(key):
                raise ValueError('supplemental arm has a different task/protocol')
        for m in extra['models']:
            if m['model'] in locations:
                raise ValueError('supplement cannot overwrite an existing model')
            locations[m['model']] = (extra_root, extra)
        plans.append(extra)
    models = list(locations)
    matrix, aggregates, rankings = {}, {}, []
    case_map = {c['id']: c for c in cases}
    for model in models:
        model_root, model_plan = locations[model]
        rows = [audit_record(c, model_root / model / c['id'] / 'record.json', model_plan) for c in cases]
        matrix[model] = {r['case_id']: r for r in rows}
        summary = summarize(rows, len(cases))
        defensible = [r for r in rows if r['family'] != 'evidence']
        summary['excluding_ambiguous_evidence'] = summarize(defensible, len(defensible))
        summary['unambiguous_routes'] = {route:summarize([r for r in defensible if answers[r['case_id']]['expected']['route']==route],sum(answers[r['case_id']]['expected']['route']==route for r in defensible)) for route in sorted({answers[r['case_id']]['expected']['route'] for r in defensible})}
        summary['checks_passed'] = {k: sum(r['grade']['checks'][k] for r in rows) for k in CRITERIA}
        tag_rows = [semantic_tags(r,case_map[r['case_id']],answers[r['case_id']]) for r in rows if r['family']!='evidence']
        summary['unambiguous_semantic_tags'] = {k:sum(t[k] for t in tag_rows) for k in tag_rows[0]}
        summary['diagnostics'] = {k: sum(diagnostics(r)[k] for r in rows) for k in diagnostics(rows[0])}
        summary['tool_errors'] = dict(Counter(t['response'].get('error') for r in rows for t in r['transcript'] if not t['response'].get('ok')))
        summary['families'] = {f: summarize([r for r in rows if r['family'] == f], sum(c['family'] == f for c in cases)) for f in sorted({c['family'] for c in cases})}
        summary['median_actions'] = statistics.median(r['grade']['turns'] for r in rows)
        summary['median_output_tokens'] = statistics.median(r['output_tokens'] for r in rows)
        summary['tokens'] = {k: sum(u.get(k, 0) for r in rows for u in r['usage']) for k in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
        summary['reasoning_tokens'] = sum((u.get('completion_tokens_details') or {}).get('reasoning_tokens',0) for r in rows for u in r['usage'])
        turns = [load_json(model_root/model/r['case_id']/e['path']) for r in rows for e in r['responses']]
        summary['http_attempts'] = sum(len(t['attempts']) for t in turns)
        costs = [((t.get('response') or {}).get('cost') or {}).get('request_cost_usd') for t in turns]
        summary['provider_reported_cost_usd'] = sum(c for c in costs if c is not None) if any(c is not None for c in costs) else None
        summary['fingerprints'] = sorted({(t.get('response') or {}).get('system_fingerprint', 'UNREPORTED') or 'UNREPORTED' for t in turns})
        summary['median_seconds'] = statistics.median(r['seconds_this_invocation'] for r in rows)
        summary['plan_sha256'] = digest(model_plan)
        summary['billing_or_auth_wall'] = any(a.get('http_status') in (401, 402, 403) for t in turns for a in t['attempts'])
        summary['comparison_eligible'] = not summary['billing_or_auth_wall'] and summary['missing'] == 0
        aggregates[model] = summary
    eligible = [m for m in models if aggregates[m]['comparison_eligible']]
    for case in cases:
        cid = case['id']
        infra = [m for m in eligible if matrix[m][cid]['outcome'] == 'provider_error']
        by_model = {m: matrix[m][cid] for m in eligible if m not in infra}
        failed = [m for m,r in by_model.items() if not r['grade']['complete']]
        if not failed:
            continue
        ds = {m: diagnostics(r) for m,r in by_model.items()}
        safety = [m for m,d in ds.items() if d['unsafe_attempt']]
        wrong_execution = [m for m,d in ds.items() if d['executed_wrong_value']]
        closed_wrong = [m for m,d in ds.items() if d['closed_incorrect']]
        semantic = [m for m in failed if m not in infra and by_model[m]['outcome'] not in ('invalid_json','invalid_response','response_token_limit','case_token_limit','usage_missing')]
        priority = 100*bool(safety)+80*bool(wrong_execution)+40*bool(closed_wrong)+20*bool(semantic and any(r['grade']['complete'] for r in by_model.values()))+len(semantic)
        if case['family'] == 'evidence':
            priority = -100  # Dataset ambiguity is not demonstrated model difficulty.
        answer = answers[cid]
        rankings.append({'case_id':cid,'family':case['family'],'conditions':answer['conditions'],
            'mechanism_signature':answer['mechanism_signature'],'route':answer['expected']['route'],
            'failed_models':failed,'passed_models':[m for m in by_model if m not in failed],
            'safety_models':safety,'wrong_execution_models':wrong_execution,'incorrect_closure_models':closed_wrong,
            'provider_error_models':infra,'priority':priority,
            'admission':'QUARANTINED_POLICY_AMBIGUITY' if case['family']=='evidence' else 'REQUIRES_INDEPENDENT_REVIEW',
            'failed_checks':{m:[k for k,v in r['grade']['checks'].items() if not v] for m,r in by_model.items() if m in failed},
            'outcomes':{m:r['outcome'] for m,r in by_model.items()},
            'semantic_tags':{m:[k for k,v in semantic_tags(r,case,answer).items() if v] for m,r in by_model.items()}})
    rankings.sort(key=lambda r:(-r['priority'],r['family'],r['case_id']))
    mechanisms=defaultdict(list)
    for row in rankings:
        mechanisms[(row['family'],row['mechanism_signature'])].append(row)
    shortlist=[]
    for group in mechanisms.values():
        if group[0]['family'] == 'evidence':
            continue
        best=dict(group[0]);best['failed_variants_in_mechanism']=len(group)
        best['sibling_case_ids']=[r['case_id'] for r in group[1:]]
        shortlist.append(best)
    shortlist.sort(key=lambda r:(-r['priority'],-r['failed_variants_in_mechanism'],r['family'],r['case_id']))
    common={'cases':len(cases),'models':len(models),'episodes':len(cases)*len(models),
        'comparable_models':eligible,'provider_blocked_models':[m for m in models if m not in eligible],
        'comparable_episodes':len(cases)*len(eligible),
        'provider_error_cases':sum(any(matrix[m][c['id']]['outcome']=='provider_error' for m in eligible) for c in cases),
        'comparison_policy':'Billing/auth-blocked arms unranked. Transient provider errors retained in assigned denominator but excluded from semantic case rankings.',
        'all_models_pass':sum(all(matrix[m][c['id']]['grade']['complete'] for m in eligible) for c in cases),
        'all_models_fail':sum(all(not matrix[m][c['id']]['grade']['complete'] and matrix[m][c['id']]['outcome'] != 'provider_error' for m in eligible) for c in cases),
        'disagreement':sum(0<sum(matrix[m][c['id']]['grade']['complete'] for m in eligible)<len(eligible) for c in cases),
        'unique_failing_mechanisms':len(mechanisms)}
    unambiguous = [c for c in cases if c['family'] != 'evidence']
    common['excluding_ambiguous_evidence'] = {
        'cases': len(unambiguous),
        'provider_error_cases':sum(any(matrix[m][c['id']]['outcome']=='provider_error' for m in eligible) for c in unambiguous),
        'all_models_pass': sum(all(matrix[m][c['id']]['grade']['complete'] for m in eligible) for c in unambiguous),
        'all_models_fail': sum(all(not matrix[m][c['id']]['grade']['complete'] and matrix[m][c['id']]['outcome'] != 'provider_error' for m in eligible) for c in unambiguous),
        'disagreement': sum(0 < sum(matrix[m][c['id']]['grade']['complete'] for m in eligible) < len(eligible) for c in unambiguous),
    }
    report={'schema':'bankingenv.private-model-analysis/v1','prefect_flow_run_id':str(flow_run.id),
        'plan_sha256':digest(plan),'audit_code_sha256':code_commitment(),'evaluator_sha256':digest(load_json(evaluator)),'protocol':plan,'protocols':plans,'case_index_sha256':manifest['case_index_sha256'],
        'common':common,'models':aggregates,'shortlist':shortlist,'all_failing_cases':rankings,
        'all_trajectories_replayed':True,'independent_banking_review':'NOT_RUN',
        'quarantine':{'family':'evidence','cases':sum(c['family']=='evidence' for c in cases),
            'reason':'Policy counts unique proof records; evaluator counts unique issuers. Detected on public smoke before private scores.',
            'policy':'Preserve raw scores; report 168-case sensitivity; no retroactive score repair.'},
        'case_value':'Analyst prioritization, not evidence of lab procurement or independently verified difficulty.'}
    if repeats:
        repeat_root = Path(repeats)
        repeat_plan = load_json(repeat_root/'plan.json')
        for key in ('code_sha256','system_sha256','case_index_sha256','turn_limit','case_output_tokens','turn_output_tokens'):
            if repeat_plan[key] != plan[key]:
                raise ValueError('repeat protocol differs from original')
        repeat_rows = []
        for cid in repeat_plan['case_ids']:
            for model in repeat_plan['models']:
                m = model['model']
                original_row = matrix[m][cid]
                trials = [audit_record(case_map[cid], repeat_root/f'sample-{n+1:02d}'/m/cid/'record.json', repeat_plan) for n in range(repeat_plan['samples_per_case'])]
                all_trials = [original_row] + trials
                repeat_rows.append({'case_id':cid,'model':m,'original_complete':original_row['grade']['complete'],
                    'repeat_completions':sum(r['grade']['complete'] for r in trials), 'repeats':len(trials),
                    'passes_including_original':sum(r['grade']['complete'] for r in all_trials),
                    'trials_including_original':len(all_trials),
                    'safety_failures_including_original':sum(bool(r['grade']['safety_violations']) for r in all_trials),
                    'outcomes':[r['outcome'] for r in all_trials]})
        report['repeatability'] = {'scope':'Post-hoc selected cases; primary scores unchanged',
            'plan_sha256':digest(repeat_plan),'rows':repeat_rows,'independent_replay_verified':True}
    atomic_json(out/'report.json',report)
    (out/'report.html').write_text(render_html(report), encoding='utf-8')
    print({'common':common,'complete':{m:s['complete'] for m,s in aggregates.items()}},flush=True)
    return {'output':str(out),'replay':'PASS','common':common}


@flow(name='bankingenv-v3-export-public-summary', persist_result=False, log_prints=True)
def export_summary(report_path: str, dataset: str, destination: str):
    """Allowlisted aggregates only; private instance identifiers cannot escape."""
    report=load_json(report_path)
    _,cases=load_package(dataset)
    case_map={c['id']:c for c in cases}
    allowed=('assigned','recorded','missing','complete','mean_score','outcomes','safety_cases',
             'check_failures','checks_passed','excluding_ambiguous_evidence','families',
             'unambiguous_routes','unambiguous_semantic_tags','diagnostics',
             'median_actions','median_output_tokens','tokens','reasoning_tokens',
             'actual_models','plan_sha256','comparison_eligible','billing_or_auth_wall')
    public={'schema':'bankingenv.public-model-comparison/v1','date':'2026-10-20',
        'source_classification':'AUTHORED_SYNTHETIC_WORKFLOW',
        'case_index_sha256':report['case_index_sha256'],'plan_sha256':report['plan_sha256'],
        'audit_code_sha256':report['audit_code_sha256'],'evaluator_sha256':report['evaluator_sha256'],
        'audit_prefect_flow_run_id':report['prefect_flow_run_id'],'export_prefect_flow_run_id':str(flow_run.id),
        'common':report['common'],'quarantine':report['quarantine'],
        'models':{m:{k:s[k] for k in allowed} for m,s in report['models'].items()},
        'lab_review_mechanism_groups':len(report['shortlist']),
        'control_or_wrong_mutation_mechanism_groups':sum(bool(r['safety_models'] or r['wrong_execution_models']) for r in report['shortlist']),
        'independent_banking_review':'NOT_RUN','primary_scores_changed_by_repeats':False,
        'limitations':['24 evidence-family cases quarantined for policy/grader ambiguity.',
            'Generated variants share seven unambiguous workflow primitives; not 168 independent authored tasks.',
            'Single primary sample per model/case with bounded JSON-action interface; no vendor-native tool optimization.',
            'Selected repeats are post-hoc and do not establish population failure rates.']}
    if report.get('repeatability'):
        public['selected_repeats']=[{'probe_family':case_map[r['case_id']]['family'],**{k:v for k,v in r.items() if k!='case_id'}} for r in report['repeatability']['rows']]
    serialized=str(public)
    needles=[c['id'] for c in cases]+[c['world_id'] for c in cases]+[d['id'] for c in cases for d in c['documents']]
    seed_file=Path(dataset).parent/'evaluator/reproduction.json'
    if seed_file.exists():
        needles.append(load_json(seed_file)['evaluation_seed'])
    if any(needle in serialized for needle in needles):
        raise ValueError('private identifier or seed in public summary')
    target=Path(destination)
    if target.exists():
        raise ValueError('public summary destination must be new')
    atomic_json(target,public)
    return {'destination':str(target),'private_identifiers_exported':0,'cases':len(cases)}


def render_html(report):
    def esc(value):
        return html.escape(str(value))
    def table(headers, rows):
        return '<table><thead><tr>'+''.join('<th>'+esc(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+esc(c)+'</td>' for c in row)+'</tr>' for row in rows)+'</tbody></table>'
    rows=[]
    for model,s in report['models'].items():
        clean=s['excluding_ambiguous_evidence']
        rows.append([model, f"{s['complete']}/{s['assigned']} ({s['complete']/s['assigned']:.1%})",
                     f"{clean['complete']}/{clean['assigned']} ({clean['complete']/clean['assigned']:.1%})",
                     f"{clean['mean_score']:.2%}", clean['safety_cases'],
                     s['outcomes'].get('provider_error',0),s['outcomes'].get('invalid_json',0),
                     f"{s['median_actions']:g}", f"{s['median_output_tokens']:,.0f}",'Comparable' if s['comparison_eligible'] else 'UNRANKED: provider blocked'])
    common=report['common']; protocol=report['protocol']
    result='<h1>BankingEnv v3: private model comparison</h1><p>'+esc(common['cases'])+' cases, '+esc(common['models'])+' live models; '+esc(common['episodes'])+' single-sample episodes. Scores independently replayed against frozen stateful graders.</p>'
    result+='<p class="warning"><strong>24 cases quarantined:</strong> the evidence-family policy says unique proof records; its grader counts unique issuers. Raw scores are preserved. The 168-case comparison excludes this ambiguity. Neither result is independently certified by a banking practitioner.</p>'
    result+=table(['Model','Raw 192 complete','Excluding ambiguity','168 rubric mean','168 safety cases','Provider errors','Invalid JSON','Median actions','Median output tokens','Status'],rows)
    result+='<h2>Failures by workflow</h2>'
    models=list(report['models'])
    result+=table(['Family']+models,[[f]+[str(report['models'][m]['families'][f]['complete'])+'/24' for m in models] for f in report['models'][models[0]]['families']])
    result+='<h2>Required route (excluding ambiguous evidence cases)</h2>'
    routes=sorted({r for m in models for r in report['models'][m]['unambiguous_routes']})
    result+=table(['Required route']+models,[[route]+[str(report['models'][m]['unambiguous_routes'][route]['complete'])+'/'+str(report['models'][m]['unambiguous_routes'][route]['assigned']) for m in models] for route in routes])
    result+='<h2>Candidate cases for lab review</h2><p>Prioritized by unsafe attempts, incorrect executed values, false closure and cross-model disagreement. One representative per mechanism; sibling numeric variants are not independent authored problems. Ambiguous evidence cases are excluded. These are observed failures, not confirmed buyer demand or repeatability claims.</p>'
    result+=table(['Case ID','Family / route','Failed models','Failed sibling variants','Observed failures'],[[r['case_id'],r['family']+' / '+r['route'],', '.join(r['failed_models']),r['failed_variants_in_mechanism'],'; '.join(m+': '+', '.join(v) for m,v in r['failed_checks'].items())] for r in report['shortlist'][:20]])
    if report.get('repeatability'):
        result+='<h2>Selected-case repeatability</h2><p>Two post-hoc repeats per model and case; original leaderboard unchanged. Three trials include the original observation. This selected sample does not estimate population performance.</p>'
        result+=table(['Case','Model','Original pass','Repeat passes','All three passes','Safety failures / 3'],[[r['case_id'],r['model'],r['original_complete'],str(r['repeat_completions'])+'/'+str(r['repeats']),str(r['passes_including_original'])+'/'+str(r['trials_including_original']),r['safety_failures_including_original']] for r in report['repeatability']['rows']])
    result+='<h2>Protocol and limits</h2><p>24 actions; 32,768 output tokens per case including reported reasoning; 8,192 per response; maximum accepted reasoning effort (MiMo maps max to high); provider sampling defaults; shared JSON-action interface. No native tool-call continuation, oracle feedback, answer repairs or answer-dependent retries. Model aliases and returned fingerprints are retained, not independently attested weight snapshots. Generation code and dataset commitments are recorded below.</p>'
    result+='<p>Original arms: DeepSeek V4 Pro direct, GLM 5.3 / Kimi K3 / MiMo V2.6 Pro via Neuralwatt. After the direct API returned HTTP 402, DeepSeek V4.1 Flash was added as a separately frozen full-set arm via Neuralwatt. Billing/auth-blocked arms are unranked. Transient API errors stay in assigned denominators and are excluded from semantic case ranking. OpenAI returned credit_balance_exhausted, Anthropic 401, xAI 403; no configured Gemini generation credential. Availability checked on 2026-10-20. This is a synthetic compositional holdout, not proof of real-bank competence.</p>'
    result+='<p>Plan SHA-256: <code>'+esc(report['plan_sha256'])+'</code><br>Case index SHA-256: <code>'+esc(report['case_index_sha256'])+'</code><br>Audit Prefect run: '+esc(report['prefect_flow_run_id'])+'</p>'
    result+='<p>Full metrics, all failed cases, model fingerprints and response commitments: <a href="report.json">report.json</a>. Private case contents, answers and transcripts are retained separately.</p>'
    return '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Private BankingEnv comparison</title><style>body{font:15px/1.45 system-ui;background:#f7f8fa;color:#182230;margin:32px;max-width:1400px}h1{font-size:26px}h2{font-size:19px;margin-top:28px}table{border-collapse:collapse;width:100%;font-size:13px;background:white}th,td{padding:8px;border:1px solid #d8dde6;text-align:left;vertical-align:top}th{background:#e9edf3}.warning{background:#fff2c9;padding:14px;border-left:4px solid #ab7000}code{overflow-wrap:anywhere}a{color:#1254ae}</style><body>'+result+'</body></html>'


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for arg in ('dataset','evaluator','panel','output'):
        parser.add_argument('--'+arg,required=True)
    parser.add_argument('--supplemental')
    parser.add_argument('--repeats')
    a=parser.parse_args()
    analyze(a.dataset,a.evaluator,a.panel,a.output,a.supplemental,a.repeats)
