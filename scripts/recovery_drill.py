"""Create and verify an encrypted backup without changing the live service."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.recovery import backup,restore
p=argparse.ArgumentParser();p.add_argument('--hermes-home',required=True);p.add_argument('--key-file',required=True);p.add_argument('--destination',required=True)
a=p.parse_args();dest=Path(a.destination)
# Refuse to reuse a previous drill directory or overwrite its evidence.
dest.mkdir(mode=0o700,parents=True,exist_ok=False);start=time.monotonic()
b=backup(a.hermes_home,dest/'snapshot.enc',a.key_file)
r=restore(dest/'snapshot.enc',a.key_file,dest/'restored')
report={'encrypted_backup':b,'isolated_restore':r,'duration_seconds':round(time.monotonic()-start,2),
        'live_database_modified':False,'production_certified':False,
        'remaining':'Validate expected counts, latest deletion overlay, Hermes reconnect and archive-specific RPO/RTO on the deployment host.'}
(dest/'report.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
