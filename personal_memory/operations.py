"""Read-only deployment diagnostics. Readiness does not mean personal-data completeness."""
import importlib.util
import json
import os
from pathlib import Path

from .client import Client
from .recovery import inspect_database


# Embedding/ML stack exercised by this release. A major-version jump here can silently change
# embeddings (a pooling change, or an API rename such as sentence-transformers'
# get_sentence_embedding_dimension -> get_embedding_dimension that Hindsight 0.9.2 still calls)
# and invalidate recall without any error at ingest time. doctor therefore surfaces major-version
# drift instead of letting it pass unnoticed. Only the public major is compared, so a host-specific
# torch CUDA build (cu128 on Blackwell, cu130 on Ampere) or a patch upgrade does not false-fail.
TESTED_EMBEDDING_STACK={"sentence-transformers":"6.0.1","numpy":"2.4.6","onnxruntime":"1.30.0","torch":"2.14.0"}


def _major(version):
    try:return int(str(version).split("+",1)[0].split(".",1)[0])
    except Exception:return None


def doctor(home,offline=False):
    home=Path(home).expanduser();path=home/"personal-memory/settings.json"
    from .configuration import load_settings
    cfg=load_settings(home);checks=[]
    def add(name,passed,detail):checks.append({"check":name,"passed":bool(passed),"detail":detail})
    add("private_settings",os.name!="posix" or not (path.stat().st_mode&0o077),"Settings must only be readable by the service owner")
    public=home/"personal-memory/config.json"
    if public.exists():add("private_behavior_config",os.name!="posix" or not (public.stat().st_mode&0o077),"Owner identities and service behavior are owner-managed")
    from . import __version__
    plugin=home/"plugins/personal-memory"
    try:
        import yaml
        manifest=yaml.safe_load((plugin/"plugin.yaml").read_text())
        add("installed_provider_version",str(manifest.get("version"))==__version__,"Installed provider must match the framework; rerun setup after upgrading")
        add("installed_investigation_module",(plugin/"personal_memory/investigate.py").is_file(),"Parallel investigation is included in the copied provider")
    except Exception as error:add("installed_provider_version",False,type(error).__name__)
    try:
        import hashlib
        runtime=json.loads((home/'personal-memory/host-runtime.json').read_text())
        contract=json.loads((Path(__file__).parent/'host_contract.json').read_text())
        root=Path(runtime['root']).resolve()
        intact=runtime['api']==2 and all(hashlib.sha256((root/name).read_bytes()).hexdigest()==entry['after'] for name,entry in contract['files'].items()) and all(hashlib.sha256((root/name).read_bytes()).hexdigest()==expected for name,expected in contract['anchors'].items())
        add('pinned_host_patch',intact,'Native memory boundaries require the complete pinned host patch')
    except Exception:add('pinned_host_patch',False,'Start patched Hermes once and rerun doctor; an absent or edited host patch is not qualified')
    data=Path(cfg["data_dir"]).expanduser().resolve()
    add("native_backup_path",data.is_relative_to(home.resolve()) or data.is_relative_to(Path.home().resolve()),"Native Hermes backup skips external paths outside the OS home; encrypted framework backups support the configured data directory")
    for module in ("uvicorn","cryptography","hindsight_api","hindsight_embed"):
        add("dependency_"+module,importlib.util.find_spec(module) is not None,"Required production dependency")
    import importlib.metadata
    drift=[]
    for package,tested in TESTED_EMBEDDING_STACK.items():
        try:installed=importlib.metadata.version(package)
        except Exception:continue  # optional on this path (e.g. a CPU-only build without torch)
        if _major(installed)!=_major(tested):drift.append(f"{package} {installed} (tested {tested})")
    add("embedding_stack",not drift,
        "Installed embedding/ML stack is within the tested major versions" if not drift
        else "Major-version drift can silently change embeddings and break recall; re-pin or re-test: "+", ".join(drift))
    database=Path(cfg["data_dir"])/"memory.db"
    try:
        inspect_database(database);add("database_integrity",True,"SQLite integrity and foreign keys passed")
    except Exception as error:add("database_integrity",False,type(error).__name__)
    outbox=home/"personal-memory/outbox.db"
    if outbox.exists():
        import sqlite3
        with sqlite3.connect(outbox.resolve().as_uri()+"?mode=ro",uri=True) as db:
            table=db.execute("SELECT 1 FROM sqlite_master WHERE name='dead_letters'").fetchone()
            dead=db.execute("SELECT count(*) FROM dead_letters").fetchone()[0] if table else 0
            native_table=db.execute("SELECT 1 FROM sqlite_master WHERE name='native_history_retirements'").fetchone()
            retirements=db.execute("SELECT count(*) FROM native_history_retirements").fetchone()[0] if native_table else 0
        add("capture_dead_letters",dead==0,f"{dead} records require repair/replay")
        add("native_history_retirements",retirements==0,f"{retirements} obsolete revisions await native-history sync retry")
    if not offline:
        client=Client(cfg["url"],cfg["token"],timeout=5)
        try:
            ready=client.call("/v1/ready");add("service_ready",ready["ready"],ready["problems"])
            status=client.call("/v1/status")
            add("live_service_version",status.get("framework_version")==__version__,"Restart the memory service after setup so its version matches the copied provider")
            hindsight=status.get("hindsight",{})
            add("hindsight_default",hindsight.get("enabled") is True and not status.get("initialization_errors",{}).get("hindsight"),
                "Hindsight must be active by default; it is not an opt-in retrieval engine")
            add("parallel_investigation",status.get("investigation_contract",{}).get("version")=="1.0" and "parallel_investigation" in status.get("capabilities",[]),"Live service must support the parallel-plan contract")
        except Exception as error:add("service_ready",False,type(error).__name__)
    return {"checks_passed":all(c["passed"] for c in checks),"checks":checks,
            "production_certified":False,
            "external_gates":["Live Hermes acceptance","User archive recall/identity evaluation","Host load and recovery drill","Storage encryption and credential/backup-key custody"]}
