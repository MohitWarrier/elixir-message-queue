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
    # max_restarts caps how many child restarts the supervisor will tolerate
    # in any 5-second window (max_seconds defaults to 5). If exceeded, the
    # supervisor itself gives up and crashes — that's the "this child is
    # genuinely broken, stop trying" signal.
    #
    # Why 50 and not the default 3, or 10, or 10_000:
    #
    #   - 3 (default) is way too low for our test suite. Phase 5 and 6
    #     tests intentionally kill queues to verify restart behavior; the
    #     budget is shared across ALL queues under the supervisor, so a
    #     handful of crash-and-restart tests blow past 3 immediately.
    #   - 10 was also empirically too low — the durability test suite
    #     pushed past it once the test count grew.
    #   - 50 is "comfortably above legitimate test churn." Empirical
    #     headroom, not a calculated number.
    #   - 10_000 would defeat the point of having a budget at all. A
    #     queue that legitimately crashes 50+ times in 5 seconds is
    #     broken in a way restarting won't fix (bad config, corrupt log,
    #     persistent bug), and the supervisor giving up at that point is
    #     the right behavior. 10_000 hides those failures.
    #
    # The right value is workload-dependent. For test + small-prod, 50.
    # If a real workload sees more than ~10 legitimate crashes per 5s,
    # something is wrong upstream, not in the supervisor budget.
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 50)
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
