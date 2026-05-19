defmodule MessageQueue do
  @moduledoc """
  Public API for the in-process message queue.

  Thin pass-through to `MessageQueue.Queue`. Callers depend on this module
  only — they never need to know about the GenServer module name or PID.

  Final shape across phases:

      MessageQueue.publish(queue, message)
      MessageQueue.fetch(queue)                        # Phase 1
      MessageQueue.fetch(queue, timeout: 30_000)       # Phase 4
      MessageQueue.ack(queue, delivery_tag)
      MessageQueue.nack(queue, delivery_tag, requeue: true)

  (Phase 5 may drop the queue name from ack/nack by embedding queue identity
  in the delivery tag. For now we pass it explicitly — simpler.)
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

  def ensure_queue(queue, opts \\ []) do
    case MessageQueue.QueueSupervisor.start_queue(queue, opts) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, other} -> {:error, other}
    end
  end
end
