"""Read-only deployment diagnostics. Readiness does not mean personal-data completeness."""
import importlib.util
import json
import os
from pathlib import Path

from .client import Client
from .recovery import inspect_database


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
