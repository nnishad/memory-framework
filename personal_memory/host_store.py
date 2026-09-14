"""Hermes MemoryStore-compatible proxy backed by canonical curated-memory APIs."""
import copy
import json

from .common import digest


HEADERS = {"memory": "MEMORY (your personal notes)",
           "user": "USER PROFILE (who the user is)"}


class CanonicalMemoryStore:
    """Per-agent live view plus an immutable prompt snapshot.

    Instances never share mutable entry lists. Concurrent instances coordinate through
    the service's expected-version contract and receive a conflict instead of overwriting.
    """
    framework_backed = True

    def __init__(self, provider, memory_char_limit=2200, user_char_limit=1375, *,
                 memory_enabled=True, user_profile_enabled=True):
        self.provider = provider
        self.memory_char_limit, self.user_char_limit = memory_char_limit, user_char_limit
        self.memory_enabled, self.user_profile_enabled = memory_enabled, user_profile_enabled
        self.memory_entries, self.user_entries = [], []
        self._versions = {"memory": 0, "user": 0}
        self._system_prompt_snapshot = {"memory": "", "user": ""}
        self._consolidation_failures = 0
        self._loaded = False

    def target_enabled(self, target):
        return self.user_profile_enabled if target == "user" else self.memory_enabled

    def reset_consolidation_failures(self):
        self._consolidation_failures = 0

    def _entries_for(self, target):
        return self.user_entries if target == "user" else self.memory_entries

    def _set_entries(self, target, entries):
        setattr(self, "user_entries" if target == "user" else "memory_entries", entries)

    def _char_limit(self, target):
        return self.user_char_limit if target == "user" else self.memory_char_limit

    def _char_count(self, target):
        return len("\n§\n".join(self._entries_for(target)))

    def _usage(self, target):
        return f"{self._char_count(target):,}/{self._char_limit(target):,}"

    def _usage_pct(self, target):
        count, limit = self._char_count(target), self._char_limit(target)
        return f"{min(100, int(count * 100 / limit)) if limit else 0}% — {count:,}/{limit:,} chars"

    def _render_block(self, target, entries):
        if not entries:
            return ""
        separator = "═" * 46
        content = "\n§\n".join(entries)
        limit = self._char_limit(target)
        usage = f"{min(100, int(len(content) * 100 / limit)) if limit else 0}% — {len(content):,}/{limit:,} chars"
        return f"{separator}\n{HEADERS[target]} [{usage}]\n{separator}\n{content}"

    @staticmethod
    def _sanitize(entries, target):
        try:
            from tools.threat_patterns import scan_for_threats
        except Exception:
            return list(entries)
        result = []
        for entry in entries:
            findings = scan_for_threats(entry, scope="strict") if entry else []
            result.append(entry if not findings else
                          f"[BLOCKED: canonical {target} entry contained threat patterns: "
                          f"{', '.join(findings)}. Remove it through the memory tool.]")
        return result

    def load_from_disk(self):
        """Compatibility name: load canonical state and freeze this instance's prompt."""
        if self._loaded:
            # Hermes reloads file stores at compression boundaries. Canonical stores
            # refresh the live edit view there but preserve this conversation's prefix.
            self.refresh_live()
            return self
        payload = self.provider.client.call("/v1/curated/read", {})
        for target in ("memory", "user"):
            state = payload["stores"][target]
            entries = [entry["text"] for entry in state["entries"]]
            self._set_entries(target, entries)
            self._versions[target] = state["version"]
            self._system_prompt_snapshot[target] = self._render_block(
                target, self._sanitize(entries, target))
            if self.provider.lineage:
                self.provider.lineage.add(self.provider.session_id, {
                    "record_ids": [rid for entry in state["entries"]
                                   for rid in entry.get("evidence_ids", [])]})
        self._loaded = True
        return self

    def refresh_live(self, target=None):
        payload = self.provider.client.call("/v1/curated/read", {"target": target} if target else {})
        for name, state in payload["stores"].items():
            self._set_entries(name, [entry["text"] for entry in state["entries"]])
            self._versions[name] = state["version"]
        return payload

    def format_for_system_prompt(self, target):
        return self._system_prompt_snapshot.get(target) or None

    def fork(self):
        """Return an isolated live view with its own frozen snapshot."""
        return CanonicalMemoryStore(
            self.provider, self.memory_char_limit, self.user_char_limit,
            memory_enabled=self.memory_enabled,
            user_profile_enabled=self.user_profile_enabled).load_from_disk()

    def _scan(self, operations):
        try:
            from tools.memory_tool import _scan_memory_content
        except Exception:
            return None
        for operation in operations:
            if operation.get("action") in {"add", "replace"}:
                content = operation.get("content") or operation.get("new_text") or ""
                if error := _scan_memory_content(content):
                    return error
        return None

    def apply_batch(self, target, operations):
        operations = copy.deepcopy(operations)
        for operation in operations:
            if "content" not in operation and operation.get("new_text") is not None:
                operation["content"] = operation.pop("new_text")
        if error := self._scan(operations):
            return {"success": False, "error": error}
        try:
            evidence_ids = self.provider.curated_write_evidence()
            request_id = "hermes-native/" + digest([
                self.provider.session_id, target, self._versions[target], operations])
            result = self.provider.client.call("/v1/curated/apply", {
                "target": target, "expected_version": self._versions[target],
                "request_id": request_id, "operations": operations,
                "evidence_ids": evidence_ids, "epoch": self.provider.memory_epoch})
        except Exception as error:
            status = getattr(error, "status", None)
            if status == 409:
                self.refresh_live(target)
                return {"success": False, "conflict": True, "done": True,
                        "error": "Memory changed in another session. Review current_entries before retrying.",
                        "current_entries": list(self._entries_for(target)),
                        "version": self._versions[target], "usage": self._usage(target)}
            return {"success": False, "done": True,
                    "error": "Canonical memory write failed; the underlying conversation may continue.",
                    "error_type": type(error).__name__}
        self._set_entries(target, [entry["text"] for entry in result["entries"]])
        self._versions[target] = result["version"]
        self.provider._invalidate()
        return {"success": True, "done": True, "target": target,
                "version": result["version"], "entry_count": len(self._entries_for(target)),
                "usage": self._usage_pct(target),
                "note": "Write saved canonically. This conversation's frozen prompt snapshot is unchanged; do not repeat it."}

    def add(self, target, content):
        return self.apply_batch(target, [{"action": "add", "content": content}])

    def replace(self, target, old_text, content):
        return self.apply_batch(target, [{"action": "replace", "old_text": old_text,
                                          "content": content}])

    def remove(self, target, old_text):
        return self.apply_batch(target, [{"action": "remove", "old_text": old_text}])

    def clear(self, target):
        return self.apply_batch(target, [{"action": "clear"}])
