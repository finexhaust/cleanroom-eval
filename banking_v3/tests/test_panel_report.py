from copy import deepcopy
import json

import httpx
import pytest

from banking_v3.contract import load_json
from banking_v3.evaluate import reference_rollout
from banking_v3.generate import build_case
from banking_v3.panel import atomic_json, run_case
from banking_v3.panel_report import audit_record, diagnostics


def test_report_replays_and_rejects_tampered_provider_trace(tmp_path):
    case=build_case('report-test','margin',(False,False,False,False),0,'development')[0]
    _,actions,_=reference_rollout(case)
    model={'model':'test','provider':'deepseek','base_url':'https://test.invalid','reasoning_effort':'max'}
    plan={'turn_limit':24,'case_output_tokens':32768,'turn_output_tokens':8192}
    calls=[]
    def handler(req):
        action=actions[len(calls)];calls.append(req)
        return httpx.Response(200,json={'model':'test','usage':{'completion_tokens':10},
            'choices':[{'finish_reason':'stop','message':{'content':json.dumps(action)}}]})
    result=run_case(case,model,tmp_path,plan,'fake','flow',httpx.MockTransport(handler))
    path=tmp_path/'test'/case['id']/'record.json'
    assert audit_record(case,path,plan)==result
    assert not any(diagnostics(result).values())
    turn=path.parent/'turn-00.json';x=load_json(turn);x['response']['usage']['completion_tokens']=20
    atomic_json(turn,x)
    with pytest.raises(ValueError,match='response commitment'):
        audit_record(case,path,plan)


def test_quarantine_is_a_separate_denominator_and_never_repairs_raw_scores(tmp_path):
    from banking_v3.contract import digest
    from banking_v3.panel_report import analyze
    dataset=tmp_path/'dataset'; records=tmp_path/'panel'
    entries=[]; answers=[]; cases=[]
    for family in ('evidence','margin'):
        case,answer=build_case('report-denominator-test',family,(False,False,False,True),0,'evaluation')
        cases.append(case);answers.append(answer)
        relative='cases/'+case['id']+'.json'
        atomic_json(dataset/relative,case)
        entries.append({'id':case['id'],'path':relative,'sha256':digest(case)})
    manifest={'schema':'bankingenv.dataset/v3','split':'evaluation','case_count':2,
              'cases':entries,'case_index_sha256':digest(entries)}
    atomic_json(dataset/'manifest.json',manifest)
    atomic_json(tmp_path/'answers.json',answers)
    models=[{'model':m,'provider':'deepseek','base_url':'https://test.invalid','reasoning_effort':'max'} for m in ('correct','wrong','outage')]
    plan={'turn_limit':24,'case_output_tokens':32768,'turn_output_tokens':8192,
          'case_index_sha256':manifest['case_index_sha256'],'case_ids':[c['id'] for c in cases],
          'models':models}
    atomic_json(records/'plan.json',plan)
    for model in models:
        for case in cases:
            _,actions,_=reference_rollout(case)
            if model['model']=='wrong':
                # Both families fail raw arithmetic; only the margin failure
                # remains in the sensitivity score and lab-review shortlist.
                next(a for a in actions if a['tool']=='assess')['args']['value']+=1
            cursor=[0]
            def handler(req):
                if model['model']=='outage':
                    return httpx.Response(402,json={'error':{'code':'insufficient_balance'}})
                action=actions[cursor[0]];cursor[0]+=1
                return httpx.Response(200,json={'model':model['model'],'usage':{'completion_tokens':10},
                    'choices':[{'finish_reason':'stop','message':{'content':json.dumps(action)}}]})
            run_case(case,model,records,plan,'fake','test-flow',httpx.MockTransport(handler))
    analyze.fn(str(dataset),str(tmp_path/'answers.json'),str(records),str(tmp_path/'analysis'))
    result=load_json(tmp_path/'analysis/report.json')
    good=result['models']['correct'];bad=result['models']['wrong']
    assert good['complete']==2 and bad['complete']==0
    assert good['excluding_ambiguous_evidence']['assigned']==1
    assert bad['excluding_ambiguous_evidence']['assigned']==1
    assert good['excluding_ambiguous_evidence']['complete']==1
    assert bad['excluding_ambiguous_evidence']['complete']==0
    assert result['common']['provider_blocked_models']==['outage']
    assert result['common']['comparable_models']==['correct','wrong']
    assert result['quarantine']['cases']==1
    assert {r['family'] for r in result['shortlist']}=={'margin'}
    assert (tmp_path/'analysis/report.html').is_file()

    extra=deepcopy(plan);extra['turn_limit']=25
    atomic_json(tmp_path/'extra/plan.json',extra)
    with pytest.raises(ValueError,match='different task/protocol'):
        analyze.fn(str(dataset),str(tmp_path/'answers.json'),str(records),str(tmp_path/'bad-analysis'),str(tmp_path/'extra'))


def test_public_projection_refuses_private_identifier(tmp_path):
    from banking_v3.panel_report import export_summary
    from banking_v3.contract import digest
    case,_=build_case('public-summary-test','margin',(False,False,False,True),0,'evaluation')
    relative='cases/'+case['id']+'.json'
    atomic_json(tmp_path/'dataset'/relative,case)
    entries=[{'id':case['id'],'path':relative,'sha256':digest(case)}]
    atomic_json(tmp_path/'dataset/manifest.json',{'schema':'bankingenv.dataset/v3','split':'evaluation','case_count':1,'cases':entries,'case_index_sha256':digest(entries)})
    report={'case_index_sha256':digest(entries),'plan_sha256':'plan','audit_code_sha256':'audit',
            'evaluator_sha256':'gold','prefect_flow_run_id':'flow','common':{},'quarantine':{},
            'models':{},'shortlist':[]}
    atomic_json(tmp_path/'report.json',report)
    export_summary.fn(str(tmp_path/'report.json'),str(tmp_path/'dataset'),str(tmp_path/'public.json'))
    assert case['id'] not in (tmp_path/'public.json').read_text()
    report['common']['accidental_private_case_id']=case['id']
    atomic_json(tmp_path/'report.json',report)
    with pytest.raises(ValueError,match='private identifier'):
        export_summary.fn(str(tmp_path/'report.json'),str(tmp_path/'dataset'),str(tmp_path/'bad-public.json'))
    assert not (tmp_path/'bad-public.json').exists()
