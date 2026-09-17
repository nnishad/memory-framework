"""Host-side protected storage for adapter credentials.

Databases, connection rows and adapter specs hold only secret:// references;
values live in one operator-owned file that is never committed, exported or
logged. This is deliberately boring infrastructure, not a KMS: on hosts that
provide a real credential manager, point the reference at it instead.
"""
import json

from .common import atomic_json, required_text

PREFIX = "secret://"


class SecretStore:
    def __init__(self, path):
        from pathlib import Path
        self.path = Path(path)

    def _load(self):
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    @staticmethod
    def _name(name):
        required_text(name, "secret name", 200)
        if any(character in name for character in "/\\ \t\n") or name.startswith(PREFIX):
            raise ValueError("Secret names must be plain identifiers")
        return name

    def put(self, name, value):
        name = self._name(name)
        if not isinstance(value, str) or not value or len(value) > 65536:
            raise ValueError("Secret values must be nonempty text within 64 KiB")
        data = self._load()
        data[name] = value
        atomic_json(self.path, data)
        try:
            self.path.chmod(0o600)
        except (OSError, AttributeError):
            pass  # Windows ACLs are owned by the directory's protection instead
        return PREFIX + name

    def resolve(self, reference):
        if not isinstance(reference, str) or not reference.startswith(PREFIX):
            raise KeyError(f"Expected a {PREFIX}<name> reference")
        value = self._load().get(reference[len(PREFIX):])
        if value is None:
            raise KeyError(f"Secret {reference[len(PREFIX):]!r} is not stored on this host")
        return value

    def resolver(self):
        """The callable handed to a ConnectionContext; adapters see values only
        for the duration of one bounded operation and can never enumerate them."""
        return self.resolve

    def names(self):
        return sorted(self._load())

    def delete(self, name):
        name = self._name(name)
        data = self._load()
        data.pop(name, None)
        atomic_json(self.path, data)

    def __repr__(self):
        return f"SecretStore({self.path.name}, {len(self._load())} entries)"
