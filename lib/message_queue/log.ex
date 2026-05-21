defmodule MessageQueue.Log do
  @moduledoc """
  Append-only write-ahead log for a single durable queue.

  Every state-changing operation a queue performs (publish, fetch, ack, nack,
  expire, dlq) is recorded here before the in-memory state is mutated and
  before the caller is replied to. On startup, replaying the log rebuilds the
  queue's full state — pending FIFO, in-flight map, DLQ.

  ## Design decisions (Phase 6)

  ### One file per queue
  Each durable queue writes to `priv/logs/<queue_name>.log`. Queue lifecycles
  are independent; deleting a queue is just deleting its file.

  ### Periodic fsync (group commit)

  `append/2` only writes to the OS page cache — kernel-managed RAM. The
  syscall returns in microseconds without touching disk. The OS will
  eventually flush dirty pages to disk on its own schedule (could be
  seconds away). `sync/1` forces that flush *now* and blocks until the
  disk confirms.

  **Cost of one fsync:** ~5 ms on SSD, more like 10–30 ms on HDD. This is
  mostly fixed latency for forcing a synchronous commit — the actual data
  transfer is microseconds for KB-scale payloads. The cost does NOT scale
  with the number of appends since the last fsync; it scales (slightly)
  with total bytes pending.

  **Why grouping works.** One `sync/1` call flushes ALL bytes currently
  pending in the page cache for this file, not just the most recent
  write. The OS tracks dirty pages (4 KB chunks of memory with unwritten
  data) and flushes them as a single batched I/O. 1000 small appends
  produce a handful of dirty pages, all committed together for one fsync
  cost — NOT 1000 × 5 ms.

  **Throughput math** (1 KB log entries on SSD):
    * Fsync after every append: ~200 ops/sec (5 ms per op).
    * Fsync every 100 ms (our default): tens of thousands of ops/sec —
      limited by RAM bandwidth, with ~5 ms of fsync time per 100 ms
      window (5% of wall clock spent on fsync).

  **Data-loss window.** Anything written since the last fsync lives only
  in the page cache. If the OS crashes (kernel panic, power loss) before
  the next fsync fires, those writes are gone. The fsync interval is the
  worst-case loss window — 100 ms by default. Lower it for less loss +
  more fsync overhead; raise it for the opposite. Zero-loss semantics
  require `sync/1` after every critical publish, paying the fsync cost
  per call.

  ### Truncate-on-corruption
  If `replay/3` encounters a torn entry (file ends mid-length-prefix or
  mid-payload), it truncates the file to the position immediately after the
  last good entry and continues. The system self-heals; the operator does not
  need to intervene. Entries before the torn one are intact because they were
  written and fsync'd in earlier groups.

  ### Length-prefix framing
  Each entry on disk is laid out as:

      <<size::32-big-unsigned, payload::binary-size(size)>>

  where `payload` is `:erlang.term_to_binary(entry)`. 4 bytes of length means
  individual entries can be up to ~4 GiB (in practice they're <1 KiB).

  ## Log entry shapes (consumed by `MessageQueue.Queue`)

  The Log module is generic — it round-trips any Erlang term. But in this
  project the Queue writes a fixed set of shapes, and `apply_helper/2` in
  `MessageQueue.Queue` is the only consumer that knows how to interpret them.
  Documented here so future contributors don't have to read both files to
  understand the protocol:

    * `{:publish, envelope}` — a new message arrived.
      `envelope = %{payload: term, attempt_count: non_neg_integer}`.
    * `{:fetch, tag, envelope}` — `envelope` moved from pending to in-flight
      under `tag` (a `make_ref/0` value).
    * `{:ack, tag}` — `tag` was acked; remove from in-flight.
    * `{:nack, tag, opts}` — `tag` was nacked. `opts` is a keyword list with
      `:requeue` (defaults to `true` if absent).
    * `{:expire, tag}` — visibility timeout fired for `tag`. Replay applies
      the same attempt-count + max_attempts decision the live handler made.

  Unknown-tag ack/nack/expire operations are **not logged** — the live
  handlers skip the log call in their unknown-tag branch. Why: replay would
  fail (Map.pop on a missing key returns nil envelope, downstream code
  crashes). Only state-changing operations land in the log.

  ## Reference values across restarts

  Delivery tags are `make_ref/0` values. References round-trip through
  `:erlang.term_to_binary/1` cleanly — a ref written to disk and read back
  compares equal (`==`) to the original. So a `{:fetch, tag, env}` entry and
  a later `{:ack, tag}` entry in the same log decode to the same `tag`
  value, and `Map.pop(in_flight, tag)` finds the entry as expected.

  References created by `make_ref/0` *after* a BEAM restart are guaranteed
  not to collide with refs decoded from a previous session (the BEAM bumps
  its "creation" counter on restart). So you can safely keep using
  `make_ref/0` for live tags alongside replayed tags in the same map.
  """

  @typedoc """
  Opaque file handle returned by `open/1`; passed to `append/2`, `sync/1`,
  `close/1`.

  A handle is NOT the file on disk and NOT a copy of the file in RAM.
  It's a small bookkeeping object the OS returns when a file is opened.
  Think of it like a library card for a specific book: the book sits on
  the shelf (file on disk), the card (handle) carries your identity,
  current page (cursor position), and access mode (read/write/append).
  Every read/write call passes the card so the OS knows which file and
  where in it you mean. `close/1` returns the card; the file itself is
  unaffected.

  In `:raw` mode (the mode `open/1` uses), the handle is an Erlang
  `:file_descriptor` record wrapping a direct OS file descriptor — not a
  PID. Only the process that called `open/1` can use it. Other processes
  passing this handle to `:file.write/2` would fail.
  """
  @type handle :: term()

  @typedoc """
  A log entry. The shape is the queue's choice — typically a tuple like
  `{:publish, message}` or `{:ack, delivery_tag}`.
  Whatever shape you write, `replay/3` will hand back to your fold function.
  """
  @type entry :: term()

  @logs_dir "priv/logs"
  @valid_name ~r/^[a-zA-Z0-9_-]+$/

  @doc """
  Returns the on-disk path for the named queue's log file.

  Pure function — does not touch the filesystem.

  Queue names are restricted to `[a-zA-Z0-9_-]+`. Anything else is rejected
  with `{:error, :invalid_name}`. This keeps filenames portable across
  Linux/macOS/Windows and makes path traversal (`"../../etc/passwd"`)
  structurally impossible. If multi-tenant naming like `"acct_42:jobs"`
  becomes a requirement, swap this for a hash-based scheme — callers go
  through this function only, so the change is local.
  """
  @spec path_for(String.t()) :: {:ok, Path.t()} | {:error, :invalid_name}
  def path_for(queue_name) when is_binary(queue_name) do
    if String.match?(queue_name, @valid_name) do
      {:ok, Path.join(@logs_dir, queue_name <> ".log")}
    else
      {:error, :invalid_name}
    end
  end

  @doc """
  Opens (or creates) the log file for `queue_name` in append mode.

  Creates `priv/logs/` if it doesn't exist. Opens the file with
  `[:append, :binary, :raw]`:

    * `:append` — writes always go to the end of the file.
    * `:binary` — reads/writes deal in binaries, not charlists.
    * `:raw` — bypass Erlang's file-server process. Only the calling
      process can use the returned handle, but writes are direct syscalls
      with no IPC overhead.

  The returned handle is opaque (see the `handle/0` typedoc) and should be
  kept in the queue's GenServer state for the process's lifetime.

  Returns `{:error, reason}` on failure: `{:error, :invalid_name}` if the
  queue name has illegal characters, or whatever `:file.open/2` returns
  for permission / disk / OS errors. The caller decides whether to crash
  or recover.
  """
  @spec open(String.t()) :: {:ok, handle()} | {:error, term()}
  def open(queue_name) do
    case path_for(queue_name) do
      {:error, :invalid_name} ->
        {:error, :invalid_name}

      {:ok, path} ->
        File.mkdir_p(@logs_dir)
        :file.open(path, [:append, :binary, :raw])
    end
  end

  @doc """
  Appends one entry to the log.

  Encodes `entry` with `:erlang.term_to_binary/1`, prepends its 4-byte
  big-endian length, and writes both as a single I/O operation (via an
  iolist, avoiding an extra copy).

  **Does NOT fsync.** Writes hit the OS page cache only. Call `sync/1`
  separately — typically on a timer (see the moduledoc on group commit),
  or explicitly before replying to a critical caller if you need
  zero-loss semantics for that specific operation.

  Returns `:ok` on success; `{:error, reason}` if the write fails (disk
  full, handle closed, file system error). Callers that want log-first
  durability MUST check this return value — a swallowed error means
  memory state can advance with no on-disk record. See `MessageQueue.Queue`
  for how the queue handles this (the `log_op/2` helper crashes the
  GenServer on any non-`:ok` return).
  """
  @spec append(handle(), entry()) :: :ok | {:error, term()}
  def append(handle, entry) do
    bin = :erlang.term_to_binary(entry)
    size = byte_size(bin)
    :file.write(handle, [<<size::32-big-unsigned>>, bin])
  end

  @doc """
  Forces all buffered writes for this handle to durable storage.

  Delegates to `:file.sync/1`, which invokes the OS `fsync` syscall and
  blocks until the disk confirms the write. ~5 ms on SSD, more on HDD —
  most of the cost is fixed latency for the synchronous commit, not data
  volume.

  Group writes together and call `sync/1` once per group; do not call
  after every `append/2` unless you specifically need zero-loss semantics
  for one operation. See the moduledoc's "Periodic fsync" section for the
  throughput math.

  Returns `:ok` on success; `{:error, reason}` if fsync itself fails (rare;
  typically indicates serious filesystem or hardware problems). Treat
  failure as a durability-promise violation — crashing is usually the
  right response, since continuing would mean acknowledging writes you
  can't actually guarantee.
  """
  @spec sync(handle()) :: :ok | {:error, term()}
   def sync(handle) do

    :file.sync(handle)
  end

  @doc """
  Replays every complete entry from the named queue's log, folding them into
  an accumulator.

  `fun` is called once per entry as `fun.(entry, acc)` and must return the
  new accumulator. The function signature mirrors `Enum.reduce/3`.

  If the log file does not exist (queue is new), returns `{:ok, initial}`
  unchanged — there's nothing to replay.

  If the log file ends with a torn entry (incomplete length prefix or
  truncated payload), the file is **truncated in place** to the position
  immediately after the last complete entry, and replay returns successfully
  with whatever was rebuilt up to that point.
  """
  @spec replay(String.t(), (entry(), acc -> acc), acc) :: {:ok, acc} | {:error, term()}
        when acc: term()
  def replay(queue_name, fun, initial) do
    with {:ok, path} <- path_for(queue_name),
         {:ok, fd} <- :file.open(path, [:read, :write, :raw, :binary]) do
      result = do_replay(fd, fun, initial)
      :file.close(fd)
      result
    else
      {:error, :enoent} -> {:ok, initial}
      {:error, :invalid_name} -> {:error, :invalid_name}
    end
  end

  @doc """
  Flushes any buffered writes and closes the handle.

  Call from the queue's `terminate/2` callback for a clean shutdown. After
  `close/1`, the handle is unusable.
  """
  @spec close(handle()) :: :ok
  def close(handle) do
    :file.close(handle)
  end

  # --- Helpers ---

  # Three termination conditions, all returning {:ok, acc}:
  #   - :eof from the 4-byte header read (clean end of file).
  #   - Short read on the header (torn length prefix from a crash mid-write).
  #   - Short read on the payload (torn payload — header said N bytes
  #     follow but fewer were on disk).
  #
  # The size guard on the payload clause is load-bearing. :file.read returns
  # {:ok, partial} when the file ends inside the requested range, NOT :eof.
  # Without `when byte_size(payload) == n`, that clause would match a
  # truncated binary and feed it to binary_to_term, which raises
  # ArgumentError — crashing init/1 and triggering a supervisor restart
  # loop until the budget burns out. The guard makes torn payloads fall
  # through to the catch-all instead, exiting replay cleanly with whatever
  # acc was built up to that point.
  defp do_replay(fd, fun, acc, last_good_pos \\ 0) do
    case :file.read(fd, 4) do
      {:ok, <<n::32-big-unsigned>>} ->
        case :file.read(fd, n) do
          {:ok, payload} when byte_size(payload) == n ->
            entry = :erlang.binary_to_term(payload)
            new_acc = fun.(entry, acc)
            do_replay(fd, fun, new_acc, last_good_pos + 4 + n)
          _ -> # torn payload
            truncate(fd, last_good_pos)
            {:ok, acc}
        end

      _ -> # torn header
        truncate(fd, last_good_pos)
        {:ok, acc}
    end
  end

  # send file cursor to last good position and truncate
  defp truncate(fd, pos) do
    {:ok, _} = :file.position(fd, pos)
    :ok = :file.truncate(fd)
  end
end
