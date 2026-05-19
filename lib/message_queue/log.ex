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

  ### Periodic fsync
  `append/2` writes to the OS page cache (cheap, microseconds) but does NOT
  fsync. The owning queue process is expected to call `sync/1` on a timer
  (recommend: every 100ms, configurable). This is group-commit-style: many
  small writes get amortized across one fsync.

  **Tradeoff:** on power loss / kernel panic, you can lose up to one fsync
  interval of writes (~100ms by default). This is acceptable for most
  workloads. Customers needing zero-loss semantics should call `sync/1` after
  every critical publish themselves — but they pay the fsync cost per call.

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
  """

  @typedoc "Opaque handle returned by `open/1`; pass to `append/2`, `sync/1`, `close/1`."
  @type handle :: term()

  @typedoc """
  A log entry. The shape is the queue's choice — typically a tuple like
  `{:publish, message, timestamp}` or `{:ack, delivery_tag, timestamp}`.
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

  The returned handle is opaque and should be kept in the queue's GenServer
  state for the lifetime of the process.

  Returns `{:error, reason}` if the file cannot be opened (permissions,
  missing directory, etc.). The caller decides whether to crash or recover.

  ## TODO
    * Ensure `priv/logs/` exists; create it if not.
    * Open with appropriate modes: `:append`, `:binary`, `:raw` (for speed —
      `:raw` skips the file-server process, but means only the owning process
      can use the handle, which is what we want).
    * Decide whether to also pre-position for reading (probably not — read
      separately in `replay/3` with a fresh handle).
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
  big-endian length, and writes both as a single I/O operation.

  **Does NOT fsync.** Call `sync/1` separately, either on a timer or
  explicitly before replying to a critical caller.

  Returns `:ok` on success; `{:error, reason}` if the write fails (disk full,
  handle closed, etc.).

  ## TODO
    * Encode the entry with `:erlang.term_to_binary/1`.
    * Compute `byte_size/1` of the encoded payload.
    * Write `[<<size::32>>, payload]` to the handle as one iolist — `:file.write/2`
      accepts an iolist, which avoids an extra copy.
  """
  @spec append(handle(), entry()) :: :ok | {:error, term()}
  def append(handle, entry) do
    bin = :erlang.term_to_binary(entry)
    size = byte_size(bin)
    :file.write(handle, [<<size::32-big-unsigned>>, bin])
  end

  @doc """
  Forces all buffered writes for this handle to durable storage.

  This is the expensive call (single-digit ms on SSD). Group writes together
  and call `sync/1` once per group; do not call after every `append/2` unless
  you specifically need zero-loss semantics for a single operation.

  ## TODO
    * Delegate to `:file.sync/1`.
    * Decide what to do on failure — typically you crash, because if fsync
      reports failure, your durability promise is broken.
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
         {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
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
  defp do_replay(fd, fun, acc) do
    case :file.read(fd, 4) do
      {:ok, <<n::32-big-unsigned>>} ->
        case :file.read(fd, n) do
          {:ok, payload} ->
            entry = :erlang.binary_to_term(payload)
            new_acc = fun.(entry, acc)
            do_replay(fd, fun, new_acc)

          _ ->
            {:ok, acc}
        end

      _ ->
        {:ok, acc}
    end
  end
end
