import json
import os
import unittest
import test_memory as fixtures
from test_memory import wire_record


@unittest.skipUnless(os.environ.get('HERMES_PROVIDER_CONTRACT'), 'Pinned Hermes contract required')
class CaptureLineageTests(fixtures.HTTPFixture):
    provider = fixtures.UpstreamContractTests.provider
    def seed_and_recall(self, provider):
        item = wire_record()
        item['source_id'] = 'lineage-canary'
        item['text'] = 'Fictional keepsake code orchid-cobalt-739.'
        rid = self.client.call('/v1/ingest', {'items': [item]})['records'][0]['id']
        result = json.loads(provider.handle_tool_call('personal_memory_search', {'query': 'orchid-cobalt-739'}))
        self.assertTrue(result['episodes'])
        return rid

    def test_forget_cascades_answer_checkpoint_and_tools_preserves_user(self):
        p = self.provider()
        rid = self.seed_and_recall(p)
        messages = [{'role': 'user', 'content': 'Independent user violet-lantern-923'},
                    {'role': 'assistant', 'content': 'orchid-cobalt-739'}]
        p.sync_turn(messages[0]['content'], messages[1]['content'], messages=messages)
        p.on_pre_compress(messages, require_checkpoint=True)
        p.on_session_end(messages)
        p.outbox.flush()
        before = self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes']
        self.assertGreater(len(before), 1)
        for row in before:
            if row['id'] != rid:
                self.assertIn(rid, self.client.call('/v1/evidence', {'record_id': row['id']})['metadata']['parent_record_ids'])
        forgotten = self.client.call('/v1/forget', {'record_id': rid})
        self.assertGreater(len(forgotten['affected_records']), 1)
        self.assertFalse(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])
        self.assertTrue(self.client.call('/v1/search', {'query': 'violet-lantern-923'})['episodes'])

    def test_offline_queue_payload_removed_after_forget(self):
        p = self.provider()
        rid = self.seed_and_recall(p)
        # Serialize with worker to ensure the deletion happens before delivery.
        with p.outbox.delivery_lock:
            p.sync_turn('Independent user', 'orchid-cobalt-739')
            self.client.call('/v1/forget', {'record_id': rid})
        p.outbox.flush()
        self.assertEqual(p.outbox.health()['dead_letters'], 0)
        with p.outbox.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM pending').fetchone()[0], 0)
        self.assertFalse(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])

    def test_lineage_survives_provider_restart(self):
        p = self.provider()
        rid = self.seed_and_recall(p)
        p.shutdown()
        p2 = self.provider()
        p2.sync_turn('Please repeat', 'orchid-cobalt-739')
        p2.outbox.flush()
        self.client.call('/v1/forget', {'record_id': rid})
        self.assertFalse(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])

    def test_native_recall_withholds_untracked_generated_copies(self):
        p = self.provider()
        messages = [{'role': 'assistant', 'tool_calls': [{'id': 's1', 'function': {'name': 'session_search'}}]},
                    {'role': 'tool', 'tool_call_id': 's1', 'content': 'quartz-harbour-582'}]
        p.sync_turn('User note fern-orbit-823', 'quartz-harbour-582', messages=messages)
        p.outbox.flush()
        self.assertFalse(self.client.call('/v1/search', {'query': 'quartz-harbour-582'})['episodes'])
        self.assertTrue(self.client.call('/v1/search', {'query': 'fern-orbit-823'})['episodes'])
        with self.assertRaisesRegex(RuntimeError, 'Checkpoint incomplete'):
            p.on_pre_compress([{'role': 'assistant', 'content': 'quartz-harbour-582'}], require_checkpoint=True)

    def test_replayed_truncated_memory_result_after_live_exposure_keeps_the_turn(self):
        p = self.provider()
        rid = self.seed_and_recall(p)  # the live observation attributes rid to this session
        truncated = '{"episodes": [{"id": "' + rid  # compaction rewrote the host transcript copy
        messages = [
            {'role': 'user', 'content': 'Independent user note fern-orbit-823'},
            {'role': 'assistant', 'tool_calls': [{'id': 'r1', 'function': {'name': 'personal_memory_search', 'arguments': '{"query":"orchid-cobalt-739"}'}}]},
            {'role': 'tool', 'tool_call_id': 'r1', 'content': truncated},
            {'role': 'assistant', 'content': 'orchid-cobalt-739'}]
        p.sync_turn(messages[0]['content'], messages[3]['content'], messages=messages)
        p.outbox.flush()
        # A replay the host truncated has nothing left to attribute, so it must not widen the block
        # and must not cost the user their turn: the answer is still captured against live evidence.
        self.assertIsNone(p.capture_warning)
        self.assertEqual(p.lineage.parents(p.session_id), [rid])
        self.assertTrue(self.client.call('/v1/search', {'query': 'fern-orbit-823'})['episodes'])
        self.assertTrue(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])

    def test_live_unparseable_memory_result_without_exposure_blocks_but_preserves_user_row(self):
        p = self.provider()
        ack = p.observe_tool_result('personal_memory_search', {'query': 'x'}, 'not-json', metadata={})
        self.assertEqual(ack['state'], 'withheld')
        # Nothing was attributed live, so the conservative block applies to generated content.
        with self.assertRaisesRegex(ValueError, 'unparseable memory tool result'):
            p.lineage.parents(p.session_id)
        p.sync_turn('Independent user note violet-lantern-923', 'Generated echo orchid-cobalt-739')
        p.outbox.flush()
        self.assertTrue(self.client.call('/v1/search', {'query': 'violet-lantern-923'})['episodes'])
        self.assertFalse(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])
        with p.outbox.connect() as db:
            self.assertIn('withheld', [r[0] for r in db.execute('SELECT state FROM observation_receipts')])

    def test_interrupted_user_input_committed_before_completion(self):
        p = self.provider()
        p.on_turn_start(1, 'Fictional interrupted note amber-pine-592')
        p.outbox.flush()
        self.assertTrue(self.client.call('/v1/search', {'query': 'amber-pine-592'})['episodes'])

    def test_deleted_dependencies_remove_existing_dead_letter_payload(self):
        from personal_memory.client import ServiceError
        p = self.provider()
        rid = self.seed_and_recall(p)
        real_call = p.outbox.client.call
        with p.outbox.delivery_lock:
            p.sync_turn('', 'orchid-cobalt-739')
            def reject(path, *args, **kwargs):
                if path == '/v1/ingest':
                    raise ServiceError(422, 'Fictional validation rejection')
                return real_call(path, *args, **kwargs)
            p.outbox.client.call = reject
        p.outbox.flush()
        self.assertEqual(p.outbox.health()['dead_letters'], 1)
        p.outbox.client.call = real_call
        self.client.call('/v1/forget', {'record_id': rid})
        p.outbox.flush()
        self.assertEqual(p.outbox.health()['dead_letters'], 0)

    def test_invalid_large_dependency_set_is_withheld_without_truncation(self):
        p = self.provider()
        p.lineage.add(p.session_id, {'record_ids': ['rec_fake_' + str(i) for i in range(101)]})
        p.sync_turn('User note remains green-falcon-284', 'Do not persist overflow-daisy-493')
        p.outbox.flush()
        self.assertIn('provenance group', p.capture_warning)
        self.assertFalse(self.client.call('/v1/search', {'query': 'overflow-daisy-493'})['episodes'])
        self.assertTrue(self.client.call('/v1/search', {'query': 'green-falcon-284'})['episodes'])

    def test_session_switch_keeps_dependencies_scoped(self):
        p = self.provider()
        rid = self.seed_and_recall(p)
        p.on_session_switch('fresh-session')
        self.assertEqual(p.lineage.parents('fresh-session'), [])
        self.assertEqual(p.lineage.parents('session-a'), [rid])

    def test_compression_branch_inherits_lineage_but_reset_does_not(self):
        p = self.provider()
        rid = self.seed_and_recall(p)
        p.on_session_switch('compressed', parent_session_id='session-a', reset=False)
        self.assertEqual(p.lineage.parents('compressed'), [rid])
        p.sync_turn('Repeat', 'orchid-cobalt-739')
        p.outbox.flush()
        self.client.call('/v1/forget', {'record_id': rid})
        self.assertFalse(self.client.call('/v1/search', {'query': 'orchid-cobalt-739'})['episodes'])
        p.on_session_switch('new', parent_session_id='compressed', reset=True)
        self.assertEqual(p.lineage.parents('new'), [])

    def test_tool_capture_replay_keeps_original_provenance(self):
        p = self.provider()
        self.seed_and_recall(p)
        messages = [{'role': 'assistant', 'tool_calls': [{'id': 't1', 'function': {'name': 'read_file'}}]},
                    {'role': 'tool', 'tool_call_id': 't1', 'content': 'Independent fixture lunar-heron-815'}]
        p.on_session_end(messages)
        p.outbox.flush()
        first = json.loads(p.handle_tool_call('personal_memory_search', {'query': 'lunar-heron-815'}))['episodes']
        self.assertTrue(first)
        # Retrieving the observation itself must not add a self dependency on replay.
        p.on_session_end(messages)
        p.outbox.flush()
        self.assertEqual(p.outbox.health()['dead_letters'], 0)
        evidence = self.client.call('/v1/evidence', {'record_id': first[0]['id']})
        self.assertNotIn(first[0]['id'], evidence['metadata']['parent_record_ids'])

    def test_hierarchical_lineage_preserves_all_201_sources_and_forgetting(self):
        p = self.provider()
        ids=[]
        for start in range(0,201,50):
            batch=[]
            for i in range(start,min(start+50,201)):
                item=wire_record();item.update(source_id='many-'+str(i),text='Fictional source number '+str(i))
                batch.append(item)
            ids.extend(r['id'] for r in self.client.call('/v1/ingest',{'items':batch})['records'])
        p.lineage.add(p.session_id, {'record_ids':ids})
        p.sync_turn('Independent user', 'hierarchycanary72951')
        p.outbox.flush()
        rows=self.client.call('/v1/search',{'query':'hierarchycanary72951'})['episodes']
        self.assertTrue(rows)
        parents=self.client.call('/v1/evidence',{'record_id':rows[0]['id']})['metadata']['parent_record_ids']
        self.assertLessEqual(len(parents),100)
        leaves=set()
        for parent in parents:
            leaves.update(self.client.call('/v1/evidence',{'record_id':parent})['metadata']['parent_record_ids'])
        self.assertEqual(leaves,set(ids))
        self.client.call('/v1/forget',{'record_id':ids[-1]})
        self.assertFalse(self.client.call('/v1/search',{'query':'hierarchycanary72951'})['episodes'])

    def test_host_capture_withholding_is_durable_and_explicit(self):
        p=self.provider();p.lineage.block(p.session_id,'untracked source')
        ack=p.on_host_event('cron_completed',{'job_id':'fixture','run_id':'run','result':'fictional'})
        self.assertEqual(ack['state'],'withheld')
        with p.outbox.connect() as db:
            self.assertEqual(db.execute('SELECT state FROM host_event_receipts WHERE event_id=?',(ack['event_id'],)).fetchone()[0],'withheld')
        status=json.loads(p.handle_tool_call('personal_memory_status',{}))
        self.assertEqual(status['host_event_receipts'][0]['state'],'withheld')

    def test_native_state_current_pointer_replay_and_forgetting(self):
        p=self.provider()
        path=p.home/'memories/MEMORY.md';path.parent.mkdir(exist_ok=True);path.write_text('Fictional state alpha')
        p.on_memory_write('add','memory','Fictional state alpha')
        p.outbox.flush()
        first=self.client.call('/v1/native-state',{})['objects'][0]
        path.write_text('Fictional state beta')
        p.on_memory_write('replace','memory','Fictional state beta')
        p.outbox.flush()
        second=self.client.call('/v1/native-state',{})['objects'][0]
        self.assertNotEqual(first['record_id'],second['record_id'])
        p.on_memory_write('add','memory','Fictional state alpha')
        p.outbox.flush()
        self.assertEqual(self.client.call('/v1/native-state',{})['objects'][0]['record_id'],second['record_id'])
        self.client.call('/v1/forget',{'record_id':second['record_id']})
        latest=self.client.call('/v1/native-state',{})['objects'][0]
        self.assertEqual(latest['state'],'unknown_forgotten')
        self.assertIsNone(latest['observation'])

    def test_typed_health_import_is_idempotent_and_source_dependent(self):
        from personal_memory.importers import health_csv, typed_health
        from personal_memory.ingestion import adapt_existing
        from personal_memory.common import now
        path=__import__('pathlib').Path(self.tmp.name)/'health.csv'
        path.write_text('timestamp,metric,value,unit\n2026-01-01T10:00:00Z,weight,60000,g\n')
        raw=next(health_csv(path))
        item=adapt_existing(raw,connector_id='fixture.health',connector_version='1',source_locator='fixture://health',observed_at=now())
        rid=self.client.call('/v1/ingest',{'items':[item]})['records'][0]['id']
        entity=self.client.call('/v1/entity',{'kind':'person','label':'Fictional subject'})['id']
        typed_health(self.client,raw,rid,entity);typed_health(self.client,raw,rid,entity)
        args={'subject_id':entity,'metric':'weight','unit':'kg','after':'2026-01-01T00:00:00Z','before':'2026-01-02T00:00:00Z'}
        aggregate=self.client.call('/v1/aggregate',args)
        self.assertEqual(aggregate['count'],1);self.assertEqual(aggregate['mean'],60)
        self.client.call('/v1/forget',{'record_id':rid})
        self.assertEqual(self.client.call('/v1/aggregate',args)['count'],0)

    def test_reset_fences_old_agents_and_preserves_source_tombstones(self):
        p=self.provider();item=wire_record();item['source_id']='reset-fixture'
        rid=self.client.call('/v1/ingest',{'items':[item]})['records'][0]['id']
        p.lineage.add(p.session_id,{'record_ids':[rid]})
        p.sync_turn('Fictional reset note','Fictional derived reset output');p.outbox.flush()
        response=self.client.call('/v1/reset',{'scope':'canonical'})
        self.assertEqual(response['state'],'completed')
        self.assertEqual(self.client.call('/v1/status')['records'],0)
        with self.assertRaises(Exception):p.check_session_epoch()
        other=wire_record();other['source_id']='new-old-agent-source'
        with self.assertRaises(Exception):p.client.call('/v1/ingest',{'items':[other]})
        item['revision']='next'
        with self.assertRaises(Exception):self.client.call('/v1/ingest',{'items':[item]})
        fresh=self.provider();self.assertEqual(fresh.memory_epoch,response['epoch'])
        fresh.client.call('/v1/ingest',{'items':[other]})
        self.assertEqual(self.client.call('/v1/status')['records'],1)

    def test_reset_resumes_a_committed_intent_after_failure(self):
        from personal_memory import reset
        store=self.server.store
        item=wire_record();self.client.call('/v1/ingest',{'items':[item]})
        original=store.forget_source
        def fail(*args,**kwargs):raise RuntimeError('simulated interruption')
        store.forget_source=fail
        try:
            with self.assertRaises(Exception):self.client.call('/v1/reset',{'scope':'canonical'})
            self.assertFalse(self.client.call('/v1/ready')['ready'])
            with self.assertRaises(Exception):self.client.call('/v1/search',{'query':'fictional'})
        finally:store.forget_source=original
        reset.initialize(store)
        self.assertEqual(self.client.call('/v1/status')['records'],0)
        with store.connect() as db:self.assertEqual(db.execute('SELECT state FROM memory_resets').fetchone()[0],'completed')

    def test_interrupted_capture_tracks_native_exposure_or_withholds(self):
        p=self.provider();rid=self.client.call('/v1/ingest',{'items':[wire_record()]})['records'][0]['id']
        messages=[{'role':'assistant','tool_calls':[{'id':'partial-recall','function':{'name':'session_search','arguments':'{}'}}]},
                  {'role':'tool','tool_call_id':'partial-recall','content':json.dumps({'_memory_read':{'tracked':True,'record_ids':[rid]}})},
                  {'role':'assistant','content':'fictional-interrupted-canary'}]
        ack=p.on_host_event('turn_interrupted',{'terminal':{'messages':messages,'interrupted':True}})
        self.assertEqual(ack['state'],'queued');p.outbox.flush()
        self.assertTrue(self.client.call('/v1/search',{'query':'fictional-interrupted-canary','source':'hermes-host-events'})['episodes'])
        self.client.call('/v1/forget',{'record_id':rid})
        self.assertFalse(self.client.call('/v1/search',{'query':'fictional-interrupted-canary','source':'hermes-host-events'})['episodes'])
        self.assertEqual(p.on_host_event('turn_interrupted',{'terminal':{}})['state'],'withheld')
