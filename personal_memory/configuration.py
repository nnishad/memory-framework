"""Private bootstrap credentials plus Hermes's native flat-JSON behavior settings."""
import json
from pathlib import Path

PUBLIC_KEYS={'port','prefetch_wait_ms','session_access','retrieval'}

def load_settings(home):
    state=Path(home).expanduser()/'personal-memory'
    cfg=json.loads((state/'settings.json').read_text())
    cfg['_hermes_home']=str(Path(home).expanduser().resolve())
    public=state/'config.json'
    if public.exists():
        values=json.loads(public.read_text())
        if not isinstance(values,dict) or set(values)-PUBLIC_KEYS:raise ValueError('Unsupported public memory configuration fields')
        cfg.update(values)
        if 'port' in values:
            port=values['port']
            if type(port) is not int or not 1024<=port<=65535:raise ValueError('Invalid service port')
            cfg['url']=f'http://127.0.0.1:{port}'
    from .access import session_allowed
    session_allowed(cfg.get('session_access',{}),{'platform':'cli'})
    from .hindsight_runtime import normalize
    return normalize(cfg)
