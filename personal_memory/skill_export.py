"""Explicit administrator export of an active evaluated procedural lesson."""
import hashlib
import json
import re
from pathlib import Path
from .common import atomic_json,atomic_write,digest


def _export_skill(home,client,candidate_id,name):
    if not re.fullmatch(r'[a-z][a-z0-9-]{2,63}',name):raise ValueError('Use a simple lowercase skill name')
    candidate=client.call('/v1/learning/active',{'candidate_id':candidate_id})
    payload=candidate['payload']
    if payload.get('category')!='procedural':raise ValueError('Only active procedural lessons can be exported')
    import yaml
    front=yaml.safe_dump({'name':name,'description':payload['family'], 'memory-provider-managed':True},sort_keys=False)
    content='---\n'+front+'---\n\n# Evaluated memory procedure\n\n'
    content+='This lesson is advisory within the scope below. Existing permissions and current user instructions remain controlling.\n\n'
    content+='Scope: '+payload['scope']+'\n\n'+payload['lesson']+'\n\n'
    content+='Prerequisites: '+json.dumps(payload['prerequisites'],ensure_ascii=False)+'\n\nExceptions: '+json.dumps(payload['exceptions'],ensure_ascii=False)+'\n'
    home=Path(home).resolve();target=home/'skills'/name/'SKILL.md'
    registry=home/'personal-memory/skill-exports.json'
    entries=json.loads(registry.read_text()) if registry.exists() else {}
    key=str(target.relative_to(home));previous=entries.get(key)
    if not target.resolve().is_relative_to(home) or target.is_symlink():raise ValueError('Unsafe skill export path')
    if target.exists() and (not previous or hashlib.sha256(target.read_bytes()).hexdigest() not in {previous['sha256'],previous.get('previous_sha256')}):
        raise ValueError('Existing skill has local edits or is not managed; export refused')
    if previous and previous['family']!=payload['family']:raise ValueError('Export cannot replace another lesson family')
    target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    # Registry first: a crash before replacing the file makes the loader deny
    # the mismatched version rather than execute an untracked procedure.
    entries[key]={'candidate_id':candidate_id,'candidate_digest':digest(payload),'family':payload['family'],
                  'sha256':hashlib.sha256(content.encode()).hexdigest(),'previous_sha256':previous['sha256'] if previous else None}
    atomic_json(registry,entries)
    # Deterministic UTF-8 bytes published atomically; directory durability is
    # only attempted on platforms that support it (skipped on Windows).
    atomic_write(target,content.encode('utf-8'))
    return {'path':str(target),'candidate_id':candidate_id,'sha256':entries[key]['sha256'],'authority':'explicit operator installation; existing permissions unchanged'}


def verify(home,client,path,content):
    home=Path(home).resolve();path=Path(path).resolve()
    if not path.is_relative_to(home):return False
    registry=home/'personal-memory/skill-exports.json'
    if not registry.exists():return False
    entry=json.loads(registry.read_text()).get(str(path.relative_to(home)))
    if not entry or hashlib.sha256(content.encode()).hexdigest()!=entry['sha256']:return False
    candidate=client.call('/v1/learning/active',{'candidate_id':entry['candidate_id']})
    return digest(candidate['payload'])==entry['candidate_digest']



def export_skill(home,client,candidate_id,name):
    from contextlib import closing
    from .asgi import ProcessLease
    with closing(ProcessLease(Path(home)/'personal-memory/skill-export.lock')):
        return _export_skill(home,client,candidate_id,name)
