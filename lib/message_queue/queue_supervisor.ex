defmodule MessageQueue.QueueSupervisor do
  @moduledoc """
  Dynamic supervisor for queue processes.

  Queue names aren't known at compile time, so queues are started on demand
  via `start_queue/2`. If a queue process crashes, this supervisor restarts
  it (with empty state — durability lands in Phase 6).

  Other queues are unaffected when one crashes: `:one_for_one` means only the
  crashed child gets restarted.
  """

  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # max_restarts is the supervisor's restart budget across all its children
    # combined, over max_seconds (default 5). Default is 3; we use 10 to give
    # tests that intentionally kill several queues in sequence enough room to
    # complete without tripping the budget.
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 10)
  end

  @doc """
  Starts a queue under supervision.

  Returns:
    * `{:ok, pid}` on success.
    * `{:error, {:already_started, pid}}` if a queue with this name is
      already running.
    * `{:error, reason}` for other failures.
  """
  def start_queue(name, opts \\ []) when is_binary(name) do
    spec = %{
      id: {MessageQueue.Queue, name},
      start: {MessageQueue.Queue, :start_link, [name, opts]},
      restart: :permanent
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
