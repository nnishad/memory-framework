from pathlib import Path
import test_memory as fixtures
from personal_memory.native_files import sync_files


class NativeFilesTests(fixtures.HTTPFixture):
    def test_chunked_migration_edit_delete_and_explicit_forgetting(self):
        home=Path(self.tmp.name)/'profile';path=home/'memories/MEMORY.md';path.parent.mkdir(parents=True)
        path.write_text('oldnativecanary92341\n'+'padding '*9000)
        first=sync_files(home,self.client,kinds=('builtin_memory',))
        root=first['files'][0]['record_id']
        self.assertGreater(first['files'][0]['parts'],1)
        self.assertTrue(self.client.call('/v1/search',{'query':'oldnativecanary92341'})['episodes'])
        path.write_text('newnativecanary82341')
        second=sync_files(home,self.client,kinds=('builtin_memory',),parents=[root])
        self.assertFalse(self.client.call('/v1/search',{'query':'oldnativecanary92341'})['episodes'])
        self.assertTrue(self.client.call('/v1/search',{'query':'oldnativecanary92341','include_history':True})['episodes'])
        self.assertTrue(self.client.call('/v1/search',{'query':'newnativecanary82341'})['episodes'])
        path.unlink();sync_files(home,self.client,kinds=('builtin_memory',))
        self.assertFalse(self.client.call('/v1/search',{'query':'newnativecanary82341'})['episodes'])
        self.client.call('/v1/forget',{'record_id':root})
        self.assertFalse(self.client.call('/v1/search',{'query':'newnativecanary82341','include_history':True})['episodes'])

    def test_outside_symlink_rejected(self):
        home=Path(self.tmp.name)/'profile';path=home/'memories/MEMORY.md';path.parent.mkdir(parents=True)
        outside=Path(self.tmp.name)/'outside';outside.write_text('fictional')
        path.symlink_to(outside)
        with self.assertRaises(ValueError):sync_files(home,self.client,kinds=('builtin_memory',))
