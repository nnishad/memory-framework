"""Actual pinned Hermes SessionDB plus local memory service, fictional data only."""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / 'tests')]
import test_memory as fixtures
from personal_memory.native_history import HermesHistoryConnector, sync_history

with tempfile.TemporaryDirectory(prefix='hermes-history-runtime-') as temp:
    os.environ['HERMES_HOME'] = str(Path(temp) / 'native-home')
    from hermes_state import SessionDB
    home = Path(os.environ['HERMES_HOME'])
    home.mkdir(exist_ok=True)
    native = SessionDB(home / 'state.db')
    case = fixtures.HTTPFixture()
    case.setUp()
    checks = []
    try:
        native.create_session('selected', 'cli')
        mid = native.append_message('selected', 'user', 'Fictional canoe is at copper-lake-837.')
        native.append_message('selected', 'assistant', 'Untracked archive output violet-moon-395.')
        connector = HermesHistoryConnector(home / 'state.db', 'actual-fixture', ['selected'])
        result = sync_history(case.client, connector, home / 'personal-memory/outbox.db')
        assert result['imported'] == 1 and result['withheld_generated'] == 1
        hits = case.client.call('/v1/search', {'query': 'copper-lake-837'})['episodes']
        assert hits
        evidence = case.client.call('/v1/evidence', {'record_id': hits[0]['id']})
        assert evidence['metadata']['extensions']['hermes.history']['data']['native_message_id'] == str(mid)
        checks.append('actual SessionDB schema and append_message import with stable message identity')
        assert len(native.get_messages('selected')) == 2
        checks.append('native session/messages preserved by read-only synchronization')
        assert native.delete_session('selected')
        result = sync_history(case.client, connector, home / 'personal-memory/outbox.db')
        assert result['removed_native_items'] == 1
        assert not case.client.call('/v1/search', {'query': 'copper-lake-837'})['episodes']
        checks.append('actual SessionDB.delete_session propagates to canonical source tombstone')
    finally:
        native.close()
        case.doCleanups()
report = {'passed': True, 'actual_SessionDB': True, 'hermes_tag': 'v2026.9.14',
          'checks': checks, 'native_session_search_replaced': False, 'native_database_modified_by_connector': False}
Path('docs/HERMES_NATIVE_HISTORY_RUNTIME.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
