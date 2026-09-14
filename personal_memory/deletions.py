"""Content-free deletion intents committed before canonical deletion."""
import sqlite3
import os
from pathlib import Path
from .common import now,required_text

class DeletionLedger:
    def __init__(self,path):
        self.path=Path(path)
    def ids(self):
        if not self.path.exists():return set()
        with sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True) as db:
            return {r[0] for r in db.execute('SELECT record_id FROM deletion_intents')}
    def contains(self,record_id):
        if not self.path.exists():return False
        with sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True) as db:
            return db.execute('SELECT 1 FROM deletion_intents WHERE record_id=?',(record_id,)).fetchone() is not None
    def contains_many(self, record_ids):
        record_ids = list(set(record_ids))
        if not record_ids or not self.path.exists(): return set()
        found = set()
        with sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro', uri=True) as db:
            for start in range(0, len(record_ids), 100):
                batch = record_ids[start:start+100]
                found.update(row[0] for row in db.execute('SELECT record_id FROM deletion_intents WHERE record_id IN (' + ','.join('?' for _ in batch) + ')', batch))
        return found
    def append(self,ids):
        ids=[required_text(i,'record_id',100) for i in ids]
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with sqlite3.connect(self.path,timeout=10) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS deletion_intents(record_id TEXT PRIMARY KEY,requested_at TEXT NOT NULL)')
            db.executemany('INSERT OR IGNORE INTO deletion_intents VALUES(?,?)',[(i,now()) for i in ids])
        self.path.chmod(0o600)
        fd=os.open(self.path.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    def merge(self,other):self.append(other.ids())
