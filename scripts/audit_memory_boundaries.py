"""Reproduce tracked and untracked boundaries using isolated fictional data."""
import json
import sys
from pathlib import Path
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / 'tests')]
from test_memory import UpstreamContractTests, wire_record
from personal_memory.access import session_allowed
from personal_memory import __version__

case = UpstreamContractTests('test_provider_tools_capture_and_session_switch')
case.setUp()
observations = []
try:
    provider = case.provider()
    wire = wire_record()
    wire.update(source_id='audit-original', text='The fictional keepsake code is orchid-cobalt-739.')
    rid = case.client.call('/v1/ingest', {'items': [wire]})['records'][0]['id']
    recalled = json.loads(provider.handle_tool_call('personal_memory_search', {'query': 'orchid-cobalt-739'}))
    assert recalled['episodes']
    messages = [{'role': 'user', 'content': 'What is the keepsake code?'},
                {'role': 'assistant', 'content': 'The keepsake code is orchid-cobalt-739.'}]
    provider.sync_turn(messages[0]['content'], messages[1]['content'], messages=messages)
    provider.on_pre_compress(messages, require_checkpoint=True)
    provider.outbox.flush()
    deletion = case.client.call('/v1/forget', {'record_id': rid})
    result = case.client.call('/v1/search', {'query': 'orchid-cobalt-739'})
    observations.append({'id': 'tracked_echo_after_source_forget', 'gap_reproduced': bool(result['episodes']),
                         'deleted_records': len(deletion['affected_records']),
                         'scope': 'Source exposed through provider search; no assertion about unattributed historical copies.'})
    provider.on_session_end([{'role': 'assistant', 'tool_calls': [{'id': 'audit-call', 'function': {'name': 'session_search'}}]},
                             {'role': 'tool', 'tool_call_id': 'audit-call', 'content': 'quartz-harbour-582'}])
    provider.sync_turn('Repeat the native result', 'quartz-harbour-582')
    provider.outbox.flush()
    native = case.client.call('/v1/search', {'query': 'quartz-harbour-582'})
    observations.append({'id': 'native_session_search_recaptured', 'gap_reproduced': bool(native['episodes']),
                         'scope': 'Untracked native recall blocks generated recapture; federation remains missing.'})
    try:
        provider.on_pre_compress([{'role': 'assistant', 'content': 'quartz-harbour-582'}], require_checkpoint=True)
        withheld = False
    except RuntimeError:
        withheld = True
    observations.append({'id': 'untracked_checkpoint', 'fail_closed': withheld})
    observations.append({'id': 'default_platform_access', 'allowed': {
        p: session_allowed({'owners': {}}, {'platform': p})
        for p in ['cli', 'cron', 'tui', 'desktop', 'api', 'acp', 'subagent']},
        'gap_remaining': 'Authenticated local TUI/desktop host binding remains missing.'})
finally:
    case.doCleanups()
report = {'framework_version': __version__, 'scope': 'Real HTTP service/outbox and pinned provider ABC; fictional data only.',
          'global_forgetting_verified': False, 'observations': observations}
Path('docs/MEMORY_BOUNDARY_PROBES.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
