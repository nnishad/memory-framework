"""Pinned patch apply/reapply/rollback/local-edit rejection acceptance fixture."""
import argparse,hashlib,json,subprocess,sys,tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--original-root',type=Path,required=True);p.add_argument('--anchor-root',type=Path,required=True);args=p.parse_args()
project=Path(__file__).resolve().parents[1];manifest=json.loads((project/'host-patch/manifest.json').read_text());checks=[]
with tempfile.TemporaryDirectory(prefix='memory-patch-') as tmp:
 root=Path(tmp)
 for rel,entry in manifest['files'].items():
  if entry['before'] is None:continue
  data=(args.original_root/rel).read_bytes();assert hashlib.sha256(data).hexdigest()==entry['before']
  path=root/rel;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
 for rel,expected in manifest['anchors'].items():
  data=(args.anchor_root/rel).read_bytes();assert hashlib.sha256(data).hexdigest()==expected
  path=root/rel;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
 def run(action,okay=True):
  result=subprocess.run([sys.executable,str(project/'scripts/manage_hermes_host_patch.py'),action,'--hermes-root',str(root)],capture_output=True,text=True)
  assert (result.returncode==0)==okay,result.stderr+result.stdout
  if okay:checks.append({'action':action,**json.loads(result.stdout)})
 for action in ['check','apply','apply','rollback','rollback']:run(action)
 edited=root/next(name for name,e in manifest['files'].items() if e['before'])
 edited.write_text(edited.read_text()+'\n# operator edit\n');before=edited.read_bytes();run('apply',False);assert edited.read_bytes()==before
 checks.append({'local_edits_rejected_without_changes':True})
report={'passed':True,'version':manifest['framework_version'],'patch_sha256':manifest['patch_sha256'],'checks':checks}
(project/'docs/HOST_PATCH_CHECK.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
