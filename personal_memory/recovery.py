"""Authenticated encrypted backups and isolated, integrity-checked restores."""
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import tarfile
import tempfile
from pathlib import Path

from .common import atomic_json,now

MAGIC=b"HPMBACKUP1\n"


def create_key(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,"wb") as f:f.write(secrets.token_bytes(32));f.flush();os.fsync(f.fileno())
    return str(path)


def _key(path):
    raw=Path(path).read_bytes()
    if len(raw)!=32:raise ValueError("Backup key must contain exactly 32 bytes")
    if os.name=="posix" and Path(path).stat().st_mode & 0o077:raise ValueError("Backup key permissions must be owner-only")
    return raw


def inspect_database(path):
    with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True) as db:
        integrity=[r[0] for r in db.execute("PRAGMA integrity_check")]
        foreign=[list(r) for r in db.execute("PRAGMA foreign_key_check")]
        if integrity!=["ok"] or foreign:raise ValueError("Database integrity verification failed")
        return {"integrity":"ok","schema_version":db.execute("PRAGMA user_version").fetchone()[0]}


def _hash(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as f:
        while chunk:=f.read(1024*1024):h.update(chunk)
    return h.hexdigest()


def backup(home,destination,key_file):
    from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
    home=Path(home);destination=Path(destination)
    if destination.exists():raise ValueError("Backup destination already exists")
    cfg=json.loads((home/"personal-memory/settings.json").read_text())
    destination.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    key=_key(key_file)
    with tempfile.TemporaryDirectory(dir=destination.parent,prefix=".backup-") as tmp:
        tmp=Path(tmp);manifest={"format":1,"cutoff":now(),"databases":{},"files":{}}
        registry=home/"personal-memory/skill-exports.json"
        if registry.exists():
            raw=registry.read_bytes();json.loads(raw)
            (tmp/"skill-exports.json").write_bytes(raw)
            manifest["files"]["skill-exports.json"]={"sha256":_hash(tmp/"skill-exports.json")}
        # Snapshot outbox first: every pre-cutoff item is queued or committed in
        # the later memory snapshot. Replays are idempotent after restoration.
        for name,source in (("outbox.db",home/"personal-memory/outbox.db"),
                            ("memory.db",Path(cfg["data_dir"])/"memory.db"),
                            ("memory.deletions.db",Path(cfg["data_dir"])/"memory.deletions.db")):
            if not source.exists():
                if name=="memory.db":raise ValueError("Memory database does not exist")
                continue
            target=tmp/name
            with sqlite3.connect(source.resolve().as_uri()+"?mode=ro",uri=True) as src,sqlite3.connect(target) as dst:
                src.backup(dst,pages=256,sleep=.05)
            target.chmod(0o600)
            manifest["databases"][name]={**inspect_database(target),"sha256":_hash(target)}
        atomic_json(tmp/"manifest.json",manifest)
        archive=tmp/"snapshot.tar"
        with tarfile.open(archive,"w") as tar:
            for name in [*manifest["databases"],*manifest["files"],"manifest.json"]:tar.add(tmp/name,arcname=name)
        nonce=secrets.token_bytes(12);encryptor=Cipher(algorithms.AES(key),modes.GCM(nonce)).encryptor()
        encryptor.authenticate_additional_data(MAGIC)
        encrypted=tmp/"encrypted"
        with archive.open("rb") as src,encrypted.open("wb") as dst:
            dst.write(MAGIC+nonce)
            while block:=src.read(1024*1024):dst.write(encryptor.update(block))
            dst.write(encryptor.finalize());dst.write(encryptor.tag);dst.flush();os.fsync(dst.fileno())
        encrypted.chmod(0o600)
        # Atomic create, refusing concurrent overwrite.
        os.link(encrypted,destination)
        if os.name=="posix":
            fd=os.open(destination.parent,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
    return {"backup":str(destination),"sha256":_hash(destination),"cutoff":manifest["cutoff"],"encrypted":True}


def restore(archive,key_file,destination,deletion_ledger=None):
    from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
    archive=Path(archive);destination=Path(destination)
    if destination.exists():raise ValueError("Restore destination must be a new directory")
    destination.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with tempfile.TemporaryDirectory(dir=destination.parent,prefix=".restore-") as tmp:
        tmp=Path(tmp);plain=tmp/"snapshot.tar"
        with archive.open("rb") as src:
            if src.read(len(MAGIC))!=MAGIC:raise ValueError("Unknown backup format")
            nonce=src.read(12);size=archive.stat().st_size-len(MAGIC)-12-16
            if size<0:raise ValueError("Truncated backup")
            src.seek(-16,2);tag=src.read(16);src.seek(len(MAGIC)+12)
            decryptor=Cipher(algorithms.AES(_key(key_file)),modes.GCM(nonce,tag)).decryptor()
            decryptor.authenticate_additional_data(MAGIC)
            with plain.open("wb") as dst:
                while size:
                    block=src.read(min(size,1024*1024))
                    if not block:raise ValueError("Truncated backup")
                    size-=len(block);dst.write(decryptor.update(block))
                dst.write(decryptor.finalize())  # Authenticate before inspecting/extracting.
        restored=tmp/"verified";restored.mkdir(mode=0o700)
        with tarfile.open(plain) as tar:
            names=set()
            for member in tar:
                if member.name not in {"memory.db","outbox.db","memory.deletions.db","skill-exports.json","manifest.json"} or not member.isfile() or member.name in names:
                    raise ValueError("Invalid backup member")
                names.add(member.name)
                with tar.extractfile(member) as src,(restored/member.name).open("wb") as dst:shutil.copyfileobj(src,dst)
                (restored/member.name).chmod(0o600)
        manifest=json.loads((restored/"manifest.json").read_text())
        if manifest.get("format")!=1 or "memory.db" not in manifest["databases"]:raise ValueError("Invalid manifest")
        for name,info in manifest["databases"].items():
            if name not in {"memory.db","outbox.db","memory.deletions.db"}:raise ValueError("Invalid database name")
            if _hash(restored/name)!=info["sha256"]:raise ValueError("Backup digest mismatch")
            inspect_database(restored/name)
        extra=manifest.get('files',{})
        if set(extra)-{'skill-exports.json'}:raise ValueError('Invalid backup metadata file')
        if ('skill-exports.json' in names)!=('skill-exports.json' in extra):raise ValueError('Unmanifested backup metadata')
        for name,info in extra.items():
            if _hash(restored/name)!=info['sha256']:raise ValueError('Backup metadata digest mismatch')
            json.loads((restored/name).read_text())
        from .deletions import DeletionLedger
        if deletion_ledger is not None:
            if not Path(deletion_ledger).is_file():raise ValueError("Current deletion ledger not found")
            inspect_database(deletion_ledger)
            DeletionLedger(restored/"memory.deletions.db").merge(DeletionLedger(deletion_ledger))
        # Replay even an included ledger: it may contain intents committed
        # after the canonical snapshot but before the ledger snapshot.
        from .store import Store
        Store(restored/"memory.db")
        with sqlite3.connect(restored/"memory.db") as db:db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        for name in ("memory.db","outbox.db","memory.deletions.db"):
            if (restored/name).exists():manifest["databases"][name]={**inspect_database(restored/name),"sha256":_hash(restored/name)}
        manifest["current_deletion_ledger_applied"]=deletion_ledger is not None
        atomic_json(restored/"manifest.json",manifest)
        restored.rename(destination)
    return {"restored":str(destination),"verified":True,"cutoff":manifest["cutoff"],"current_deletion_ledger_applied":deletion_ledger is not None,
            "note":"Isolated restore. Stop the service before installing databases; credentials/model settings and backup key are maintained separately."}
