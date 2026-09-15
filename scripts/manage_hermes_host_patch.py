#!/usr/bin/env python3
"""Explicit, hash-pinned host patch installation. Never called by plugin setup."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def find_entry_point(root):
    """The Hermes command as `hermes doctor` looks for it, or None.

    doctor_platform._check_command_installation probes exactly these two layouts, so an
    agent installed into a virtual environment outside the clone always reads as broken.
    """
    for name in ('venv', '.venv'):
        candidate = root / name / 'bin' / 'hermes'
        if candidate.exists():
            return candidate
    return None


def ensure_entry_point(root, venv):
    """Expose an external agent venv where Hermes's doctor looks for it.

    Only `.venv` is linked because that is the name Hermes gitignores; `venv` is not, and a stray
    untracked directory there would move the patched tree out of the state the bundle pins. The
    requested venv is always validated, so a typo cannot pass silently, and nothing is ever
    replaced or re-pointed: an entry point that is already correct is reported and left alone.
    """
    venv = Path(venv).expanduser().resolve()
    if not (venv / 'bin' / 'hermes').is_file():
        raise SystemExit('The given agent venv has no hermes entry point: ' + str(venv))
    link = root / '.venv'
    if link.is_symlink():
        if link.resolve() != venv:
            raise SystemExit('.venv already links elsewhere: ' + str(Path(link.readlink())))
        return {'state': 'linked', 'entry_point': str(link / 'bin' / 'hermes'), 'venv': str(venv)}
    if link.exists() and not (link / 'bin' / 'hermes').exists():
        raise SystemExit('Refusing to replace a real .venv path: ' + str(link))
    existing = find_entry_point(root)
    if existing is not None:
        return {'state': 'present', 'entry_point': str(existing)}
    link.symlink_to(venv, target_is_directory=True)
    return {'state': 'created', 'entry_point': str(link / 'bin' / 'hermes'), 'venv': str(venv)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['check', 'apply', 'rollback', 'link-entry-point'])
    parser.add_argument('--hermes-root', type=Path, required=True)
    parser.add_argument('--agent-venv', type=Path,
                        help='virtual environment holding the hermes command, when it is outside the clone')
    args = parser.parse_args()
    bundle = Path(__file__).resolve().parents[1] / 'host-patch'
    manifest = json.loads((bundle / 'manifest.json').read_text())
    patch = bundle / ('hermes-' + manifest['release'] + '.patch')
    if sha(patch) != manifest['patch_sha256']:
        raise SystemExit('Patch integrity check failed')
    root = args.hermes_root.resolve()
    for name, expected in manifest['anchors'].items():
        if sha(root / name) != expected:
            raise SystemExit('Pinned host contract mismatch: ' + name)
    if args.action == 'link-entry-point':
        # Deliberately independent of the patch state: it touches no tracked file, so it is valid
        # before, after or without `apply`, and `rollback` has no reason to undo it.
        if args.agent_venv is None:
            raise SystemExit('link-entry-point requires --agent-venv')
        print(json.dumps(ensure_entry_point(root, args.agent_venv))); return
    states = []
    for name, hashes in manifest['files'].items():
        target = root / name
        if not target.resolve().is_relative_to(root) or target.is_symlink():
            raise SystemExit('Unsafe patch target: ' + name)
        actual = sha(target)
        states.append('original' if actual == hashes['before'] else 'patched' if actual == hashes['after'] else 'modified')
    if len(set(states)) != 1 or 'modified' in states:
        raise SystemExit('Local edits or mixed patch state detected; no files changed')
    state = states[0]
    if args.action == 'check' or (args.action == 'apply' and state == 'patched') or (args.action == 'rollback' and state == 'original'):
        print(json.dumps({'state': state, 'changed': False})); return
    command = ['git', 'apply', '--whitespace=nowarn']
    if args.action == 'rollback': command.append('--reverse')
    subprocess.run(command + ['--check', str(patch)], cwd=root, check=True)
    # git apply checks all hunks and applies atomically by default (no --reject).
    subprocess.run(command + [str(patch)], cwd=root, check=True)
    key = 'after' if args.action == 'apply' else 'before'
    for name, hashes in manifest['files'].items():
        if sha(root / name) != hashes[key]:
            raise SystemExit('Post-apply hash mismatch: ' + name)
    print(json.dumps({'state': 'patched' if key == 'after' else 'original', 'changed': True}))


if __name__ == '__main__': main()
