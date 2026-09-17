# Gmail historical and continuous ingestion

The Gmail adapter imports accessible historical messages and polls mailbox
history for new/changed messages. The memory service owns the worker: no agent
turn, external cron job, or per-fetch LLM call is required. Restarting the service
resumes active connections from durable checkpoints.

## Setup on the Hermes host

Install this framework version on the host and restart the memory service.
Authorize using a Google **desktop** OAuth client with the Gmail API enabled:

```sh
personal-memory gmail-authorize --client-secret /private/client_secret.json \
  --credentials-file /private/gmail-credentials.json
personal-memory gmail-connect --credentials-file /private/gmail-credentials.json
personal-memory sources-status
```

All commands accept `--hermes-home`. Authorization uses the browser and a random
loopback callback, PKCE, state validation, and Gmail read-only scope. On a headless
SSH host, authorize on your desktop and transfer the resulting credential file
over SSH to a private path on the host. The connect command sends that credential
bundle to the configured memory service; use loopback or an authenticated SSH
tunnel, not an unencrypted remote HTTP endpoint.

Refresh credentials are copied into `source-secrets.json` in the private memory
data directory. Never put either credential file in Git or model prompts. A
desktop authorization file is sensitive even though no mailbox content is in it.
An expired/revoked refresh grant requires reauthorization; the source status
reports `needs_auth` rather than repeatedly restarting the import.

Default scope: **all accessible mail, including Sent, excluding Spam and Trash**.
Use `--after YYYY-MM-DD` for a historical cutoff. Polling defaults to 300 seconds;
`--poll-seconds` accepts 60–86400. `--retention archive` is the default: imported
mail remains available after removal at Gmail. `--retention mirror` hides removed
source evidence and invalidates its dependent memories. Neither mode sends mail,
marks it read, nor changes upstream labels. Explicit framework forgetting remains
stronger than either retention mode and blocks reimport.

Gmail API limits and local processing affect actual freshness. Live and historical
cursors are independent; the worker checks live work before each historical page.
A historical page is at most 20 messages. Retrying stable identities does not
duplicate source evidence. Large emails use bounded record parts without dropping
text. Label-only changes do not re-embed the unchanged body.

```sh
personal-memory sources-control pause --connection-id sconn_...
personal-memory sources-control resume --connection-id sconn_...
personal-memory sources-control retry --connection-id sconn_...
personal-memory sources-control disconnect --connection-id sconn_...
```

Pause/resume preserves progress. Disconnect cancels work without deleting retained
evidence. Canonical reset pauses connections and cancels queued work. Resuming
after reset must be deliberate. Expired Gmail history starts a new live anchor
and historical rescan, with an explicit coverage gap: deleted upstream messages
cannot be recovered by rescanning.

## Memory and attachments

Messages enter the existing canonical store and ingestion audit. They are
immediately eligible for keyword recall; semantic indexing runs when configured.
Hindsight processes Gmail when enabled and its configured `sources` list includes
the Gmail source (or `*`). The standard setup enables Hindsight with `*`.
Automatic consolidation follows the host's
`auto_consolidate` setting and review policy. Hermes uses its existing memory
provider to retrieve evidence. Source status is
available through `personal_memory_knowledge` with `operation: sources`.
Model-backed consolidation requests structured summaries and quoted proposals, validates
quotes against the exact email spans sent to the model, and keeps results pending for review.
An invalid proposed fact is discarded without losing a valid quoted summary. The default
extractive adapter only quotes source text; it does not infer facts.

Ingestion does not change local model endpoints, automatically promote inferred
facts, or override existing consolidation review settings. Message thread/header
coordinates and observed participants are preserved; cross-account person identity
is not guessed. Source capture completion is distinct from semantic indexing and
memory formation readiness.

When the optional change journal is enabled, historical pages are classified as
historical. Gmail `messagesAdded` history after the authorization anchor is
classified as a live arrival and carries its thread ID for grouping. Label-only
updates remain metadata changes and do not wake a model. Awareness consumers and
the Hermes background worker are configured separately from Gmail ingestion.

Attachments are queued durably, downloaded read-only and stored as hashed blobs
linked to the owning evidence revision. Failed downloads retry with an eventual
quarantine state. This integration stores original attachment bytes; **PDF/OCR/
audio content extraction is not included**. Email plain-text/HTML bodies are
normalized and indexed. No remote images or executable attachment content are run.

Source-management endpoints are admin-only. Agent/reader credentials can inspect
`/v1/sources/status`; they cannot connect accounts or access OAuth credentials.
Private `settings.json` may set `"sources": {"enabled": false}` to disable the
background source worker. Existing installations without connections make no
Gmail requests. The service must stay running for continuous ingestion.

## Windows live test without deploying Hermes

Use a private directory outside the repository. First run `gmail-authorize` as
above, then:

```sh
python scripts/test_gmail_live.py --credentials-file /private/gmail-credentials.json \
  --data-dir /private/gmail-test --pages 2 --polls 2
```

This starts an isolated loopback reference service, imports a bounded historical
sample, polls actual Gmail history, verifies evidence/search over HTTP, and pauses
the connection before stopping. It persists a content-free `live-test-report.json`
and private sample evidence. It does not run host LLMs, download embedding models,
or deploy/restart production Hermes. New-mail arrival is only demonstrated if the
mailbox receives changes during the test; controlled fixtures cover that case.

For a full production import, use the normal `gmail-connect` workflow on the
running host. Do not mistake the bounded trial for a completed mailbox import.

## Validation

`python -m unittest tests.test_gmail tests.test_source_sdk tests.test_source_sync
tests.test_source_email tests.test_source_delivery` covers adapter normalization,
large history pages, empty polls followed by updates, concurrent historical/live
delivery, revision visibility, reset fencing, attachment storage, and admin-only
source management. Existing ingestion/retrieval tests remain compatibility gates.
