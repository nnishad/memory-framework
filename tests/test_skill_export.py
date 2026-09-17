"""Fix 7 regression: skill export must be portable (Windows-safe) and recoverable."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from personal_memory.service import MemoryService
from personal_memory import skill_export
from personal_memory.skill_export import export_skill, verify
from test_ingestion import item


class SkillExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.service = MemoryService(self.root, 'a' * 40, principals=[
            {'token': 'g' * 40, 'role': 'agent'}, {'token': 'e' * 40, 'role': 'evaluator'},
            {'token': 'r' * 40, 'role': 'reader'}])
        self.addCleanup(self.service.close)
        self.store = self.service.store
        self.roles = {r: self.service.authenticate('Bearer ' + t * 40)
                      for r, t in [('admin', 'a'), ('agent', 'g'), ('evaluator', 'e'), ('reader', 'r')]}
        self.rid = self.store.ingest_contract([item()])['records'][0]['id']
        self.other = self.store.ingest_contract([item('evaluation')])['records'][0]['id']
        self.home = self.root / 'hermes'

        class Client:
            def call(inner, path, data):
                return self.service.dispatch(path, data, self.roles['admin'])
        self.client = Client()

    def call(self, action, args, role='agent'):
        return self.service.dispatch('/v1/learning/' + action, args, self.roles[role])

    def outcome(self, key='task'):
        return self.call('outcome', dict(key=key, goal='Repair bicycle', action='Check brakes',
                                         result='Brake test passed', outcome='success', evidence_ids=[self.rid]))

    def proposal(self, revision=1, key=None):
        outcome = self.outcome()
        return self.call('propose', dict(key=key or f'lesson-{revision}', family='bicycle-check', revision=revision,
                                         category='procedural', lesson='Check brakes before riding',
                                         scope='bicycle maintenance', prerequisites=['Bicycle available'],
                                         exceptions=['No permission to alter brakes'],
                                         outcome_ids=[outcome['id']], evidence_ids=[]))

    def evaluation(self, candidate, passed=True, key='eval'):
        return self.call('evaluate', dict(key=key, candidate_id=candidate['id'], evidence_ids=[self.other],
                                          cases=[{'id': k, 'kind': k, 'baseline_pass': True, 'candidate_pass': passed}
                                                 for k in ['target', 'regression', 'non_applicable']]), 'evaluator')

    def promote(self, candidate, evaluation, expected=None):
        return self.call('promote', dict(candidate_id=candidate['id'], evaluation_id=evaluation['id'],
                                         expected_active_id=expected), 'admin')

    def active_candidate(self):
        candidate = self.proposal(); self.promote(candidate, self.evaluation(candidate)); return candidate

    def test_successful_export_writes_deterministic_utf8_and_verifies(self):
        candidate = self.active_candidate()
        result = export_skill(self.home, self.client, candidate['id'], 'bicycle-check')
        path = Path(result['path'])
        raw = path.read_bytes()
        self.assertEqual(raw, raw.decode('utf-8').encode('utf-8'))
        self.assertNotIn('\r\n', raw.decode('utf-8'))
        self.assertTrue(verify(self.home, self.client, path, path.read_text()))

    def test_repeated_export_is_idempotent_and_leaves_no_temp_files(self):
        candidate = self.active_candidate()
        first = export_skill(self.home, self.client, candidate['id'], 'bicycle-check')
        second = export_skill(self.home, self.client, candidate['id'], 'bicycle-check')
        self.assertEqual(first['sha256'], second['sha256'])
        path = Path(second['path'])
        self.assertTrue(verify(self.home, self.client, path, path.read_text()))
        leftovers = [p.name for p in path.parent.iterdir() if p.name != 'SKILL.md']
        self.assertEqual(leftovers, [])

    def test_locally_edited_skill_is_not_overwritten(self):
        candidate = self.active_candidate()
        path = Path(export_skill(self.home, self.client, candidate['id'], 'bicycle-check')['path'])
        path.write_text(path.read_text() + '\nlocal edit\n')
        with self.assertRaises(ValueError):
            export_skill(self.home, self.client, candidate['id'], 'bicycle-check')

    def test_registry_publication_failure_leaves_no_authoritative_skill(self):
        candidate = self.active_candidate()
        target = self.home / 'skills' / 'bicycle-check' / 'SKILL.md'
        with mock.patch.object(skill_export, 'atomic_json', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                export_skill(self.home, self.client, candidate['id'], 'bicycle-check')
        self.assertFalse(target.exists())
        self.assertFalse(verify(self.home, self.client, target, 'anything'))

    def test_file_replacement_failure_cannot_authorize_unverified_skill(self):
        # The registry is committed first; if the skill file is then not published
        # the incomplete export must not verify, and no temp file may leak.
        candidate = self.active_candidate()
        target = self.home / 'skills' / 'bicycle-check' / 'SKILL.md'
        real_replace = os.replace

        def selective(src, dst, *args, **kwargs):
            if str(dst).endswith('SKILL.md'):
                raise OSError('replacement failed')
            return real_replace(src, dst, *args, **kwargs)

        with mock.patch('personal_memory.common.os.replace', side_effect=selective):
            with self.assertRaises(OSError):
                export_skill(self.home, self.client, candidate['id'], 'bicycle-check')
        self.assertFalse(target.exists())
        self.assertFalse(verify(self.home, self.client, target, 'stale'))
        leftovers = [p.name for p in target.parent.iterdir() if p.name != 'SKILL.md']
        self.assertEqual(leftovers, [])

    def test_atomic_write_directory_sync_only_when_supported(self):
        from personal_memory.common import atomic_write
        target = self.root / 'out' / 'note.md'
        opened_flags = []
        real_open = os.open

        def spy_open(path, flags, *args, **kwargs):
            opened_flags.append(flags)
            return real_open(path, flags, *args, **kwargs)

        with mock.patch('personal_memory.common.os.open', side_effect=spy_open):
            atomic_write(target, b'deterministic bytes')
        self.assertEqual(target.read_bytes(), b'deterministic bytes')
        if not hasattr(os, 'O_DIRECTORY'):
            self.assertTrue(opened_flags, 'export must still fsync via the file handle')


if __name__ == '__main__':
    unittest.main()
