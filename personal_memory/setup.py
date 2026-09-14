import json
import os
import secrets
import shutil
import tempfile
from pathlib import Path

from .common import atomic_json, digest
from . import __version__


def install(home, port=8766, exclusive=False):
    try:
        import yaml
    except ImportError:
        raise RuntimeError("Setup requires PyYAML; run in Hermes's Python environment or install the [setup] extra") from None
    home = Path(home).expanduser().resolve()
    home.mkdir(parents=True, exist_ok=True)
    config_path = home / "config.yaml"
    config = yaml.safe_load(config_path.read_text()) if config_path.exists() else {}
    config = config or {}
    if not isinstance(config, dict):
        raise ValueError("Hermes config must be a YAML mapping")
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("port must be 1024..65535")
    if not isinstance(config.get("memory", {}), dict):
        raise ValueError("memory config must be a mapping")
    disabled = config.get("agent", {}).get("disabled_toolsets", [])
    if "memory" in disabled:
        raise ValueError("Hermes disables the memory toolset. Enable it explicitly before setup.")
    state = home / "personal-memory"
    state.mkdir(mode=0o700, exist_ok=True)
    settings_path = state / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {
        "url": f"http://127.0.0.1:{port}", "token": secrets.token_urlsafe(32),
        "port": port, "data_dir": str(state / "data")}
    if "agent_token" not in settings:
        settings["agent_token"]=secrets.token_urlsafe(32)
        settings.setdefault("principals",[]).append({"token":settings["agent_token"],"role":"agent"})
    rollback_path = state / "rollback.json"
    if rollback_path.exists():
        rollback = json.loads(rollback_path.read_text())
    else:
        rollback = {"config_existed": config_path.exists(), "previous_memory": config.get("memory"),
                    "original_yaml": config_path.read_text() if config_path.exists() else ""}
    destination = home / "plugins" / "personal-memory"
    if destination.exists() and not (destination / ".personal-memory-managed").is_file():
        raise ValueError("Refusing to overwrite an unmanaged plugin directory")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".personal-memory-stage-", dir=destination.parent))
    try:
        shutil.copytree(Path(__file__).parent, stage / "personal_memory",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        (stage / "__init__.py").write_text("from .personal_memory.provider import PersonalMemoryProvider\n\ndef register(ctx):\n    ctx.register_memory_provider(PersonalMemoryProvider())\n")
        (stage / "plugin.yaml").write_text(f'name: personal-memory\nkind: exclusive\nversion: {__version__}\ndescription: "Evidence-linked hybrid personal memory service"\nhooks: []\n')
        shutil.copyfile(Path(__file__).parent/"dashboard_schema.py",stage/"config_schema.py")
        (stage / ".personal-memory-managed").touch()
        backup = destination.with_name(".personal-memory-previous")
        if backup.exists():
            shutil.rmtree(backup)
        if destination.exists():
            destination.rename(backup)
        stage.rename(destination)
        if backup.exists():
            shutil.rmtree(backup)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    atomic_json(settings_path, settings)
    public_path=state/"config.json"
    if not public_path.exists():
        from .hindsight_runtime import default_config
        atomic_json(public_path,{"port":settings["port"],"prefetch_wait_ms":settings.get("prefetch_wait_ms",200),
                                "session_access":settings.get("session_access",{"owners":{}}),
                                "retrieval":{"hindsight":default_config(settings["data_dir"])}})
    else:
        # Upgrade old opt-in/disabled installations to the mandatory default.
        public=json.loads(public_path.read_text())
        from .hindsight_runtime import normalize
        merged=dict(settings);merged.update(public);normalize(merged)
        public["retrieval"]=merged["retrieval"]
        public["retrieval"]["hindsight"].pop("enabled",None)
        atomic_json(public_path,public)
    memory = dict(config.get("memory", {}))
    memory["provider"] = "personal-memory"
    if exclusive:
        # The provider now owns Hermes's native memory surface. Keep the native
        # tool enabled, but prevent MEMORY.md/USER.md from becoming a second truth.
        memory["store"] = "provider"
        memory["memory_enabled"] = True
        memory["user_profile_enabled"] = True
    config["memory"] = memory
    rollback["installed_memory"] = memory
    atomic_json(rollback_path, rollback)
    # Save via temp+replace. YAML comments may be reformatted; original bytes are in rollback.
    temporary = config_path.with_suffix(".personal-memory.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        yaml.safe_dump(config, stream, sort_keys=False)
        stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, config_path)
    launcher = state / "run_service.py"
    launcher.write_text("import sys\nsys.path.insert(0, " + repr(str(destination)) + ")\n"
                        "from personal_memory.__main__ import main\n"
                        "main(['serve', '--hermes-home', " + repr(str(home)) + "])\n")
    return {"provider": "personal-memory", "hermes_home": str(home), "launcher": str(launcher),
            "url": settings["url"], "exclusive": exclusive,"version":__version__,
            "required_restarts":["memory service", "Hermes"],
            "next": "Start or restart the memory service, restart Hermes, run doctor, then check personal_memory_status advertises parallel_investigation."}


def rollback(home):
    import yaml
    home = Path(home).expanduser().resolve()
    saved = json.loads((home / "personal-memory" / "rollback.json").read_text())
    path = home / "config.yaml"
    config = yaml.safe_load(path.read_text()) or {}
    if config.get("memory") != saved["installed_memory"]:
        raise ValueError("Memory settings changed after setup; restore manually from rollback.json to avoid overwriting them")
    if saved["previous_memory"] is None:
        config.pop("memory", None)
    else:
        config["memory"] = saved["previous_memory"]
    temp = path.with_suffix(".rollback.tmp")
    temp.write_text(yaml.safe_dump(config, sort_keys=False)); temp.chmod(0o600)
    os.replace(temp, path)
    return {"restored": True, "data_preserved": True, "note": "Stop the memory service if no longer needed. Restart Hermes."}
