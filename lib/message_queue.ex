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

  @doc "Publish a message to the named queue."
  defdelegate publish(queue, message), to: MessageQueue.Queue

  @doc """
  Fetch the next message from the named queue.

  Returns `{:ok, message, delivery_tag}` or `:empty`.
  """
  defdelegate fetch(queue), to: MessageQueue.Queue

  @doc "Ack a delivery. The message is permanently removed."
  defdelegate ack(queue, delivery_tag), to: MessageQueue.Queue

  @doc "Nack a delivery. `requeue: true` (default) puts the message back."
  defdelegate nack(queue, delivery_tag, opts \\ []), to: MessageQueue.Queue
end
