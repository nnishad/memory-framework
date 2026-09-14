#!/usr/bin/env python3
"""Explicit, hash-pinned host patch installation. Never called by plugin setup."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['check', 'apply', 'rollback'])
    parser.add_argument('--hermes-root', type=Path, required=True)
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
