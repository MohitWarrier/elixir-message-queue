defmodule MessageQueue do
  @moduledoc """
  Public API for the in-process message queue.

  Thin pass-through to `MessageQueue.Queue`. Callers depend on this module
  only — they never need to know about the GenServer module name or PID.

  ## Lifecycle

  Queues are started on demand and supervised. Use `ensure_queue/2` to
  start one (idempotent — safe to call repeatedly), then publish/fetch
  against it by name:

      :ok = MessageQueue.ensure_queue("orders")
      :ok = MessageQueue.publish("orders", %{order_id: 42})
      {:ok, msg, tag} = MessageQueue.fetch("orders")
      :ok = MessageQueue.ack("orders", tag)

  Queue names are strings. They're restricted to `[a-zA-Z0-9_-]+` when
  durability is enabled (they become file names on disk).

  ## Durability

  Queues are in-memory by default. Pass `durable: true` to `ensure_queue/2`
  to opt into write-ahead logging:

      :ok = MessageQueue.ensure_queue("orders", durable: true)

  Durable queues survive process crashes and BEAM restarts by replaying
  their log on init. See `MessageQueue.Queue.start_link/2` for the full
  option list (`:max_attempts`, `:visibility_timeout`, `:durable`,
  `:fsync_interval`).

  ## Error semantics

  Most calls (`publish/2`, `fetch/2`, `ack/2`, `nack/3`) are `GenServer.call`
  pass-throughs. They return ordinary values (`:ok`, `{:ok, msg, tag}`,
  `:empty`, `{:error, :unknown_tag}`) for expected outcomes.

  Unexpected outcomes — disk-full write failures on a durable queue, the
  queue process being killed mid-call, configuration mismatches — cause
  the underlying `GenServer.call` to exit. Callers should be prepared for
  this; the supervisor will restart the queue, but the call that crashed
  it sees an exit signal, not a return value.

  ## API surface

      MessageQueue.ensure_queue(queue, opts \\\\ [])
      MessageQueue.publish(queue, message)
      MessageQueue.fetch(queue, opts \\\\ [])             # opts: [timeout: ms]
      MessageQueue.ack(queue, delivery_tag)
      MessageQueue.nack(queue, delivery_tag, opts \\\\ [])  # opts: [requeue: bool]
      MessageQueue.dlq_messages(queue)
  """

  @doc "Publishes a message to the named queue. See `MessageQueue.Queue.publish/2`."
  defdelegate publish(queue, message), to: MessageQueue.Queue

  @doc """
  Fetches the next message from the named queue.

  Returns `{:ok, message, delivery_tag}` or `:empty`. See
  `MessageQueue.Queue.fetch/1` for delivery and ack semantics.
  """
  defdelegate fetch(queue, opts \\ []), to: MessageQueue.Queue

  @doc "Acknowledges a delivery. See `MessageQueue.Queue.ack/2`."
  defdelegate ack(queue, delivery_tag), to: MessageQueue.Queue

  @doc """
  Negatively acknowledges a delivery. `requeue: true` (default) returns
  the message to the queue; `requeue: false` routes it to the DLQ.
  See `MessageQueue.Queue.nack/3`.
  """
  defdelegate nack(queue, delivery_tag, opts \\ []), to: MessageQueue.Queue

  @doc """
  Returns the dead-letter queue's contents as a list, oldest first.
  Read-only. See `MessageQueue.Queue.dlq_messages/1`.
  """
  defdelegate dlq_messages(queue), to: MessageQueue.Queue

  @doc """
  Starts a queue under supervision, or confirms it's already running.

  Idempotent: calling twice with the same name returns `:ok` both times
  without disturbing the existing process. Safe to call from application
  boot, request-handling code, or anywhere else you need a queue to exist.

  Returns:

    * `:ok` if the queue was started, or was already running.
    * `{:error, reason}` for other supervisor failures (rare — typically
      indicates a misconfiguration).

  ## Options

  Forwarded as-is to `MessageQueue.Queue.start_link/2`. See that function's
  docs for the full list. Most commonly:

    * `:durable` — `true` to enable write-ahead logging and crash recovery.
    * `:max_attempts` — DLQ threshold (default 5).
    * `:visibility_timeout` — milliseconds before unacked messages are
      redelivered (default 30,000).
    * `:fsync_interval` — milliseconds between disk flushes for durable
      queues (default 100).

  Options on subsequent `ensure_queue/2` calls with the same name are
  **ignored** — the first call wins. If you need to change a queue's
  options, stop the existing process and start a new one.
  """
  @spec ensure_queue(String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_queue(queue, opts \\ []) do
    case MessageQueue.QueueSupervisor.start_queue(queue, opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, other} -> {:error, other}
    end
  end
end
