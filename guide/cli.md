# Command Line (`tandem-cli`)

---

## 0. Who this page is for

You want to look at the outbox and act on it from a terminal or a shell script, without writing HTTP
calls by hand. `tandem-cli` does exactly what the [Admin API](admin-api.md) does, with typed flags,
readable output and exit codes a script can branch on.

!!! question "The question this page answers"
    **How do I install the CLI, point it at my application, and run each command?**

The CLI is a frontend to the Admin API and never a second way into the database. Everything it can
do, the API can do, and the concepts (the statuses of a row, replay against discard, `SINGLE` against
`LEASE`) are explained once, in [Admin API §1](admin-api.md#1-concepts). Read that first if the
words are new.

---

## 1. Concepts

### 1.1 What you need

- **The Admin API turned on and reachable** from where you run the CLI. See
  [Admin API §1.2](admin-api.md#12-turning-it-on).
- **The `tandem-cli` binary.** It is a single self-contained executable with no runtime to install.

The CLI has its own version, independent of the library's, because what it depends on is the Admin
API's major version (`/v1`), not the library release. `tandem-cli --version` prints both.

### 1.2 Installing it

Download the archive for your platform from the project's
[Releases](https://github.com/alirux/tandem-transactional-outbox-kafka/releases) page. CLI releases
are the ones tagged `cli-v<version>`, and the archives are named
`tandem-cli_<version>_<os>_<arch>` (`.tar.gz`, or `.zip` on Windows), with a `checksums.txt` next to
them. Unpack it and put `tandem-cli` on your `PATH`.

To build it from a checkout instead, you need Go:

```bash
cd tandem-cli && make build     # produces tandem-cli/bin/tandem-cli
```

Check it:

```bash
tandem-cli --version
```

### 1.3 Connecting to your application

Every command needs to know where the Admin API is. Flags and environment variables are equivalent,
and a flag wins over its variable.

| Setting | Flag | Environment variable | Notes |
|---|---|---|---|
| Base URL | `--base-url` | `TANDEM_ADMIN_URL` | **Required.** Include the full prefix, `/tandem/admin/v1` by default. |
| Bearer token | `--token` | `TANDEM_ADMIN_TOKEN` | Sent as `Authorization: Bearer <token>`. |
| API key | `--api-key` | `TANDEM_ADMIN_API_KEY` | Sent as `X-API-Key`. |
| Any other header | `--header 'Name: value'` | | Repeatable. For a scheme the two above do not cover, such as Basic authentication. |
| CA certificate | `--ca-cert <file>` | `TANDEM_ADMIN_CA_CERT` | PEM bundle to verify the server against, for a private certificate authority. |
| Skip TLS verification | `--insecure` | | Prints a warning every time it is used. |
| Request timeout | `--timeout <duration>` | | Per request, `60s` by default; `0` disables it. |

The simplest setup is two variables in your shell:

```bash
export TANDEM_ADMIN_URL=http://localhost:8080/tandem/admin/v1
export TANDEM_ADMIN_TOKEN=<token>          # only if your application requires one
```

**Credentials are optional.** The CLI never enforces an authentication policy of its own: with no
credential given it sends the request as it is and lets the server decide. A deployment with no
authentication, or one isolated on an internal network, works without any.

### 1.4 Reading the output

`--output human` is the default: tables for lists, `name: value` blocks for a single thing, with
colour on a terminal for the values worth a second look (a `DOWN` relay, an uncovered bucket).
`NO_COLOR` turns colour off.

`--output json` prints the API's response **unchanged**, ready for `jq`:

```bash
tandem-cli --output json outbox summary | jq '.counts.FAILED'
```

A list shows at most one page. When there is a next one, the human output ends with
`next page: --cursor=<value>` and the JSON keeps `nextCursor`; pass the value back with `--cursor`.
The CLI never follows pages on its own.

Human output is meant for people and may be reworded between releases. **In a script, use
`--output json`.**

### 1.5 Confirmations

Two commands are hard to undo and ask before they act:

- **`outbox discard`** is irreversible. On a terminal it asks you to confirm; anywhere else (a pipe,
  a CI job) it refuses unless you pass **`--yes`**, which also stands for the acknowledgement the API
  requires. It also always needs **`--reason`**.
- **`outbox replay-bulk`** can touch many rows. On a terminal it first counts what matches and asks;
  anywhere else it needs **`--yes`**, or **`--dry-run`** to only count.

`--yes` is a flag of every command and is ignored by the ones that never ask.

### 1.6 Exit codes

A script can tell failures apart by the exit code.

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Unexpected error, or a failure the CLI does not classify separately. |
| `2` | Usage error: a bad flag or argument, a missing base URL, `discard` without `--reason`. |
| `3` | Unauthorized (`401`). |
| `4` | Not found (`404`). |
| `5` | Invalid parameter (`400`), for example an unknown status. |
| `6` | Conflict (`409`): the row is not in a state that allows the action, or the command needs `LEASE`. |
| `7` | Confirmation required or declined: `--yes` missing where it was needed, or the acknowledgement refused. |
| `8` | Could not reach the API at all: DNS, connection, TLS, or the timeout. |

The codes are part of the CLI's promise: an existing code never changes meaning, and a new failure
mode gets a new one.

### 1.7 Help and completion

Every command describes itself:

```bash
tandem-cli --help
tandem-cli outbox search --help
```

Tab completion for your shell is generated by `tandem-cli completion bash`, `zsh`, `fish` or
`powershell`; each has its own `--help` explaining how to load it.

### 1.8 Commands that need `LEASE`

The commands about buckets and workers, and `pause` or `resume` with `--bucket`, need the relay to
run under `LEASE` coordination. Under the default `SINGLE` they fail with exit code `6`, because
there is nothing per-bucket to report. The CLI does not check the mode itself: it sends the request
and reports what the API answered. Sections below mark them.

---

## 2. The commands, by function

The examples assume `TANDEM_ADMIN_URL` (and a token, if you need one) are set, as in §1.3. The output
shown comes from a running application, trimmed in places; the examples with ids 7 and 42 stand in for a failed row your own outbox will have, and yours will have its own ids and times.

| Function | Command | Needs `LEASE` |
|---|---|---|
| [Health](#21-health-of-the-outbox) | `tandem-cli outbox summary` | |
| [Finding messages](#22-finding-messages) | `tandem-cli outbox search` | |
| | `tandem-cli outbox get <id>` | |
| [Recovering messages](#23-recovering-messages) | `tandem-cli outbox replay <id>` | |
| | `tandem-cli outbox replay-bulk` | |
| | `tandem-cli outbox discard <id>` | |
| [Relay state and control](#24-relay-state-and-control) | `tandem-cli relay status` | |
| | `tandem-cli relay pause` | Only with `--bucket` |
| | `tandem-cli relay resume` | Only with `--bucket` |
| [Buckets and workers](#25-buckets-and-workers-lease-only) | `tandem-cli relay buckets [bucket]` | Yes |
| | `tandem-cli relay release-bucket <bucket>` | Yes |
| | `tandem-cli relay workers` | Yes |

### 2.1 Health of the outbox

#### `tandem-cli outbox summary`

How many rows are in each status and how far behind delivery is. It is the first command to run when
something feels wrong.

```console
$ tandem-cli outbox summary
pending:       0
inFlight:      0
done:          2
failed:        0
discarded:     0
lagCount:      0
lagAgeSeconds: 0
```

| Flag | Meaning |
|---|---|
| `--watch` | Keep refreshing instead of reading once, until you press Ctrl+C. |
| `--interval <duration>` | Refresh period with `--watch`, `2s` by default. |

With `--watch` on a terminal the command draws a live dashboard that redraws in place, with a bar for
`PENDING`, `IN_FLIGHT` and `FAILED`. A growing `FAILED` bar is meant to catch the eye. A request that
fails during the watch keeps the loop going rather than exiting.

```console
$ tandem-cli outbox summary --watch --interval 1s
Tandem outbox — 2026-09-20 14:05:01 (every 1s, Ctrl+C to stop)

PENDING    ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0
IN_FLIGHT  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0
FAILED     ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0

done:           2
discarded:      0
lagCount:       0
lagAgeSeconds:  0
```

### 2.2 Finding messages

#### `tandem-cli outbox search`

Lists messages in id order, oldest first, one page at a time. Every filter is optional and they combine.
The list leaves out payloads and headers; use `get` for those.

| Flag | Meaning |
|---|---|
| `--status <status>` | `PENDING`, `IN_FLIGHT`, `DONE`, `FAILED` or `DISCARDED`. |
| `--aggregate-id <id>` | Exact aggregate id. |
| `--aggregate-type <type>` | Exact aggregate type. |
| `--type <type>` | The event type, for example `com.acme.order.placed`. |
| `--correlation-id <id>` | Every message of one business operation. Combine with `--status`. |
| `--created-from`, `--created-to` | Time bounds, RFC 3339 (`2026-09-20T12:00:00Z`). |
| `--limit <n>` | Page size, 1 to 500. |
| `--cursor <value>` | Continue from the previous page's cursor. |

```console
$ tandem-cli outbox search --status DONE --limit 1
ID  AGGREGATE_ID   AGGREGATE_TYPE  SEQ  STATUS  ATTEMPTS  CREATED_AT
1   order-lease-1  Order           1    DONE    0         2026-09-20T12:01:22Z
next page: --cursor=1
```

#### `tandem-cli outbox get <id>`

One message in full, with its payload and headers and, for a row that failed, `lastError`. Use it to
find out why a row is `FAILED`.

```console
$ tandem-cli outbox get 1
id:            1
aggregateId:   order-lease-1
aggregateType: Order
status:        DONE
attempts:      0
createdAt:     2026-09-20T12:01:22Z
seq:           1
replays:       1
type:          OrderPlaced
payload:       {"event":"OrderPlaced","order":"order-lease-1"}
headers:       {"content-type":"application/json"}
```

An unknown id exits with `4`.

### 2.3 Recovering messages

#### `tandem-cli outbox replay <id>`

Resets one `DONE` or `FAILED` message to `PENDING`, so the relay publishes it again. Replaying a
`DONE` row publishes a duplicate, so consumers must be idempotent. A row in any other status exits
with `6`. The output is the updated message.

```console
$ tandem-cli outbox replay 1
id:            1
aggregateId:   order-lease-1
aggregateType: Order
status:        PENDING
...
replays:       2
```

#### `tandem-cli outbox replay-bulk`

Replays every `DONE` or `FAILED` message that matches a selector. **At least one selector is
required**; without any it fails, so it can never replay the whole outbox by accident.

| Flag | Meaning |
|---|---|
| `--aggregate-id <id>` | Selector: one aggregate. |
| `--aggregate-type <type>` | Selector: every row of a type. |
| `--from-id <n>`, `--to-id <n>` | Selector: an id range. |
| `--status <status>` | Restrict to `DONE` or `FAILED`. Repeat the flag for both; with none, both are eligible. |
| `--dry-run` | Count what would be replayed without changing anything. |
| `--yes` | Confirm without being asked (needed outside a terminal). |

**Count first.**

```console
$ tandem-cli outbox replay-bulk --aggregate-type Order --status FAILED --dry-run
matched:  3
replayed: 0
dryRun:   true

$ tandem-cli outbox replay-bulk --aggregate-type Order --status FAILED --yes
matched:  3
replayed: 3
dryRun:   false
```

#### `tandem-cli outbox discard <id> --reason <text>`

Abandons a `FAILED` message for good, so its aggregate stops waiting behind it.

!!! warning "Discarding skips an event"
    The aggregate's consumers never receive it, so per-aggregate ordering is broken for that
    aggregate, permanently, and there is no way back. Replay whenever the cause can be fixed.

`--reason` is required and is kept for the audit trail. Without `--yes`, on a terminal you are asked
to confirm; outside one the command refuses with exit code `7`.

```console
$ tandem-cli outbox discard 7 --reason "payload rejected by the broker, event obsolete" --yes
id:            7
aggregateId:   order-42
status:        DISCARDED
...
```

Only a `FAILED` row can be discarded; anything else exits with `6`.

### 2.4 Relay state and control

#### `tandem-cli relay status`

Whether the relay is running and whether the buckets are covered. It works under both coordination
modes.

```console
$ tandem-cli relay status
state:            RUNNING
bucketCount:      256
uncoveredBuckets: 0
workers:          1
```

`state` is `RUNNING`, `PAUSED` or `DOWN`. `DOWN` means no relay instance has reported in for a few
seconds. Under `SINGLE`, `workers` and `uncoveredBuckets` are always `0`, which does not mean the
relay is down: read `state`.

#### `tandem-cli relay pause` and `tandem-cli relay resume`

Pause stops the relay from publishing; resume continues. Rows keep being written and wait as
`PENDING`. The change reaches every relay instance within a few seconds, and a worker in the middle
of a send finishes what it claimed. The output is the relay's status.

```console
$ tandem-cli relay pause
state:            PAUSED
bucketCount:      256
uncoveredBuckets: 0
workers:          1

$ tandem-cli relay resume
state:            RUNNING
...
```

| Flag | Meaning |
|---|---|
| `--bucket <n>` | Act on a single bucket. Needs `LEASE`; under `SINGLE` it exits with `6`. |

Neither command asks for confirmation: each undoes the other.

### 2.5 Buckets and workers (LEASE only)

Work is divided into virtual buckets and, under `LEASE`, each has one owner at a time. Under
`SINGLE` these commands exit with `6`.

#### `tandem-cli relay buckets [bucket]`

With no argument, lists every bucket; with one, shows that bucket. Owner, lease expiry and backlog
for each.

| Flag | Meaning |
|---|---|
| `--uncovered-only` | List only the buckets that have pending rows and no live owner: the stalled ones. Only for the list form. |

```console
$ tandem-cli relay buckets 0
bucket:       0
covered:      true
paused:       false
pendingCount: 0
owner:        tandem-host-16809-90b7
leaseUntil:   2026-09-20T12:02:44Z
```

`covered` says whether the bucket has a live owner. `paused` says whether it was paused on purpose:
a paused bucket keeps its owner, so this is how you tell a deliberate pause from a stall.

#### `tandem-cli relay release-bucket <bucket>`

Clears a bucket's ownership so another worker can claim it. Use it when the owner is a zombie: its
heartbeat is stale, the bucket has pending rows and nothing moves.

```console
$ tandem-cli relay release-bucket 1
bucket:       1
covered:      false
paused:       false
pendingCount: 0
```

#### `tandem-cli relay workers`

The live relay instances, with their last heartbeat and how many buckets each owns.

```console
$ tandem-cli relay workers
WORKER_ID               BUCKET_COUNT  LAST_HEARTBEAT
tandem-host-16809-90b7  256           2026-09-20T12:02:14Z
```

---

## 3. In a script

Use `--output json` for the data and the exit code for the outcome.

**Alert when something failed:**

```bash
failed=$(tandem-cli --output json outbox summary | jq '.counts.FAILED')
if [ "$failed" -gt 0 ]; then
  echo "$failed outbox rows have failed" >&2
  exit 1
fi
```

**Read every page of a search:**

```bash
cursor=""
while :; do
  page=$(tandem-cli --output json outbox search --status FAILED --limit 100 --cursor "$cursor")
  echo "$page" | jq -r '.items[] | "\(.id)\t\(.aggregateId)\t\(.type)"'
  cursor=$(echo "$page" | jq -r '.nextCursor // empty')
  [ -z "$cursor" ] && break
done
```

**Branch on the failure:**

```bash
tandem-cli outbox get "$id" > /dev/null
case $? in
  0) echo "found" ;;
  4) echo "no such message" ;;
  8) echo "cannot reach the Admin API" ;;
  *) echo "unexpected failure" ;;
esac
```

---

## 4. A worked case: recovering a failed event

An alert says `FAILED` is above zero.

```bash
# 1. How bad is it?
tandem-cli outbox summary

# 2. Which rows, and why?
tandem-cli outbox search --status FAILED
tandem-cli outbox get 7            # read lastError

# 3. Fix the cause outside Tandem (create the missing topic, correct the broker setting),
#    then count and replay everything that failed for that reason.
tandem-cli outbox replay-bulk --aggregate-type Order --status FAILED --dry-run
tandem-cli outbox replay-bulk --aggregate-type Order --status FAILED --yes

# 4. Confirm the relay drained them.
tandem-cli outbox summary --watch
```

Only if one specific event can never be delivered and consumers can live without it, discard that
row with a reason, as in §2.3.

---

## 5. Compatibility

The CLI's promise covers the **command and subcommand names, the flag names and their meaning, and
the exit codes**. It does not cover the human output, which may be reworded, or the shape of the JSON
output, because that is the Admin API's own and follows the API's contract
([Admin API §6](admin-api.md#6-compatibility)).

A CLI release is compatible with any application whose Admin API is `/v1`, whatever the library
version behind it. The full command reference, generated from the tool itself, is in the repository
under
[`tandem-cli/docs/cli`](https://github.com/alirux/tandem-transactional-outbox-kafka/tree/main/tandem-cli/docs/cli/tandem-cli.md).
