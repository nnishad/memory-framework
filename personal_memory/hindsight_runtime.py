"""Managed Hindsight 0.9.2 lifecycle for the supported Hermes service path."""
import hashlib
import os
import re
import shutil
from pathlib import Path


PROFILE_PREFIX = "hermes-personal-memory-"


def default_config(data_dir):
    """Return a zero-configuration, private local Hindsight deployment."""
    identity=hashlib.sha256(str(Path(data_dir).expanduser().resolve()).encode()).hexdigest()[:12]
    profile=f"hermes-personal-memory-{identity}"
    return {"managed":True,"profile":profile,"bank_id":"hermes-personal-memory",
            "sources":["*"],"backend_id":"embedded:"+profile}


def normalize(settings):
    """Make Hindsight mandatory while preserving safe operational overrides."""
    retrieval=settings.setdefault("retrieval",{})
    if not isinstance(retrieval,dict):raise ValueError("retrieval must be an object")
    supplied=retrieval.get("hindsight",{})
    if not isinstance(supplied,dict):raise ValueError("retrieval.hindsight must be an object")
    defaults=default_config(settings["data_dir"])
    # `enabled` was the pre-rc8 opt-in. It is deliberately ignored now.
    allowed={"managed","profile","bank_id","sources","backend_id","url","token",
             "llm_provider","llm_model","llm_base_url","database_url","retain_timeout","recall_timeout"}
    unknown=set(supplied)-allowed-{"enabled"}
    if unknown:raise ValueError("Unsupported Hindsight configuration fields: "+", ".join(sorted(unknown)))
    merged={**defaults,**{k:v for k,v in supplied.items() if k!="enabled"}}
    merged["enabled"]=True
    if type(merged["managed"]) is not bool:raise ValueError("Hindsight managed must be boolean")
    for name in ("profile","bank_id","backend_id"):
        if not isinstance(merged.get(name),str) or not merged[name].strip():raise ValueError("Invalid Hindsight "+name)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}",merged["profile"]):
        raise ValueError("Invalid Hindsight profile")
    if len(merged["bank_id"])>200 or len(merged["backend_id"])>1000:raise ValueError("Hindsight identifier too long")
    if not isinstance(merged.get("sources"),list) or not merged["sources"] or any(not isinstance(v,str) or not v for v in merged["sources"]):
        raise ValueError("Hindsight sources must be a non-empty string list")
    if len(merged["sources"])>100 or any(len(value)>200 for value in merged["sources"]):raise ValueError("Hindsight source scope too large")
    for name in ("token","llm_provider","llm_model","llm_base_url","database_url"):
        if name in merged and (not isinstance(merged[name],str) or len(merged[name])>4000):raise ValueError("Invalid Hindsight "+name)
    for name in ("retain_timeout","recall_timeout"):
        if name in merged and (type(merged[name]) not in (int,float) or not 0<merged[name]<=600):raise ValueError("Invalid Hindsight "+name)
    if not merged["managed"]:
        if not isinstance(merged.get("url"),str) or not merged["url"].startswith(("http://","https://")):
            raise ValueError("External Hindsight requires an http(s) url")
        if "backend_id" not in supplied:merged["backend_id"]="external:"+merged["url"]
    retrieval["hindsight"]=merged
    from . import extraction
    retrieval["attachment_extraction"]=extraction.normalize_config(retrieval.get("attachment_extraction"))
    return settings


def _automatic_provider(hermes_home=None):
    # Explicit Hindsight environment configuration is inherited as-is.
    if any(name.startswith("HINDSIGHT_API_LLM_") for name in os.environ):return None
    # Hermes Portal is already the user's authenticated model path. Hindsight
    # officially supports it without copying a credential into our config.
    hermes_home=Path(hermes_home or os.environ.get("HERMES_HOME",Path.home()/".hermes")).expanduser()
    if (hermes_home/"auth.json").is_file():return "nous"
    if os.environ.get("OPENAI_API_KEY"):return None
    # Fully local chunk+hybrid retrieval; no model or API key is required.
    return "none"


class Runtime:
    def __init__(self,settings):
        self.settings=settings;self.manager=None;self.profile=None

    def start(self):
        cfg=self.settings.get("retrieval",{}).get("hindsight")
        if not cfg or not cfg.get("managed"):return self
        try:
            from hindsight_embed import get_embed_manager
        except ImportError:
            raise RuntimeError("Managed Hindsight requires hindsight-embed==0.9.2; reinstall the production package") from None
        provider=cfg.get("llm_provider",_automatic_provider(self.settings.get("_hermes_home")))
        daemon={"HINDSIGHT_EMBED_DAEMON_IDLE_TIMEOUT":"0","HINDSIGHT_API_LOG_LEVEL":"warning"}
        for source,target in ((provider,"HINDSIGHT_API_LLM_PROVIDER"),(cfg.get("llm_model"),"HINDSIGHT_API_LLM_MODEL"),
                              (cfg.get("llm_base_url"),"HINDSIGHT_API_LLM_BASE_URL"),
                              (cfg.get("database_url"),"HINDSIGHT_EMBED_API_DATABASE_URL")):
            if source is not None:daemon[target]=str(source)
        # The slim supported runtime avoids Torch/MLX. It downloads one small
        # ONNX embedding model and preserves Hindsight's fused RRF ordering.
        if not any(name.startswith("HINDSIGHT_API_EMBEDDINGS_") for name in os.environ):
            daemon.update({"HINDSIGHT_API_EMBEDDINGS_PROVIDER":"onnx",
                "HINDSIGHT_API_EMBEDDINGS_ONNX_MODEL_ID":"sentence-transformers/all-MiniLM-L6-v2",
                "HINDSIGHT_API_EMBEDDINGS_ONNX_DIMENSIONS":"384",
                "HINDSIGHT_API_EMBEDDINGS_ONNX_QUERY_PREFIX":"",
                "HINDSIGHT_API_EMBEDDINGS_ONNX_PASSAGE_PREFIX":""})
        if not any(name.startswith("HINDSIGHT_API_RERANKER_") for name in os.environ):
            daemon["HINDSIGHT_API_RERANKER_PROVIDER"]="rrf"
        self.manager=get_embed_manager();self.profile=cfg["profile"]
        if not self.manager.ensure_running(daemon,self.profile):
            self.manager.stop(self.profile);self.manager=None
            raise RuntimeError("Hindsight embedded daemon failed to start")
        cfg["url"]=self.manager.get_url(self.profile)
        cfg["runtime"]={"mode":"embedded","profile":cfg["profile"],"llm_provider":provider or "environment"}
        return self

    def close(self):
        if self.manager is not None:
            self.manager.stop(self.profile);self.manager=None

    def __enter__(self):return self.start()
    def __exit__(self,*_):self.close()


def _instance_roots(home=None):
    """Filesystem roots the managed backend uses for embedded instances and profiles."""
    home=Path(home or os.environ.get("HINDSIGHT_HOME", Path.home()/".hindsight")).expanduser()
    pg0=Path(os.environ.get("PG0_HOME", Path.home()/".pg0")).expanduser()
    return pg0/"instances", home/"profiles"


def stale_instances(data_dir, home=None):
    """Framework-owned Hindsight instances/profiles on disk that do not match the active profile.

    The profile identity is derived from ``data_dir``, so a data_dir change (or an older
    deployment) leaves an orphaned embedded Postgres instance under ``~/.pg0/instances`` and
    profile files under ``~/.hindsight/profiles`` that are never garbage-collected - a disk leak
    that can also retain stale memory data from a previous era. Only entries carrying the
    framework's own ``hermes-personal-memory-`` prefix are considered, and the active profile is
    always excluded, so unrelated Hindsight data is never touched.
    """
    active=default_config(data_dir)["profile"]
    instances_dir, profiles_dir=_instance_roots(home)
    stale, stems = [], set()
    if instances_dir.is_dir():
        for path in sorted(instances_dir.iterdir()):
            name=path.name
            if name.startswith("hindsight-embed-"+PROFILE_PREFIX) and name!="hindsight-embed-"+active:
                stale.append(path); stems.add(name[len("hindsight-embed-"):])
    if profiles_dir.is_dir():
        # A stale profile leaves sibling control files (.env/.lock/.log); reclaim them with the
        # instance so the profile directory does not accumulate dangling files.
        for path in sorted(profiles_dir.iterdir()):
            stem=path.name.split(".")[0]
            if stem.startswith(PROFILE_PREFIX) and stem!=active:
                stems.add(stem); stale.append(path)
    return {"active_profile":active,"stale":[str(p) for p in stale],"stale_profiles":sorted(stems)}


def prune_stale_instances(data_dir, home=None, apply=False):
    """Report (and with ``apply=True`` remove) stale framework-owned Hindsight instances.

    Dry-run by default so an operator can inspect exactly what would be reclaimed before any
    deletion. Removal is scoped to the framework's own profile prefix and never touches the
    active instance; it should run while the service is stopped so no daemon holds the files.
    """
    found=stale_instances(data_dir, home)
    removed, errors, reclaimed = [], [], 0
    if apply:
        for raw in found["stale"]:
            path=Path(raw)
            try:
                size=sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) if path.is_dir() else path.stat().st_size
                if path.is_dir(): shutil.rmtree(path)
                else: path.unlink()
                removed.append(raw); reclaimed+=size
            except Exception as error:
                errors.append({"path":raw,"error":type(error).__name__})
    return {"active_profile":found["active_profile"],"stale":found["stale"],"stale_profiles":found["stale_profiles"],
            "applied":apply,"removed":removed,"reclaimed_bytes":reclaimed,"errors":errors}
