defmodule MessageQueue.Queue do
  @moduledoc """
  A GenServer representing one work queue.

  Each queue owns its own in-memory FIFO of pending messages, a map of
  in-flight (delivered but unacked) messages, and a dead-letter queue for
  messages that have exhausted their retry budget.

  Delivery is at-least-once: a fetched message stays in-flight until the
  consumer acks or nacks it, or until the visibility timeout expires and
  it is redelivered to another consumer.

  ## Durability (Phase 6)

  When started with `durable: true`, the queue writes every state-changing
  operation to `priv/logs/<name>.log` (via `MessageQueue.Log`) before
  mutating in-memory state. On restart, `init/1` replays the log to rebuild
  state. See `MessageQueue.Log` for the on-disk format and entry shapes.

  ### Log-first ordering (invariant)

  **State-changing handlers MUST call `Log.append/2` before mutating
  state.** This is the write-ahead-log invariant; reordering it silently
  breaks crash recovery.

  Why: memory is volatile, disk is the source of truth. If the process dies
  between `append` and mutation, replay restores the op from disk on
  restart. If the process dies between mutation and `append`, the in-memory
  change is lost forever — disk has no record of it.

  Affected handlers: `publish`, `fetch` (success branch only), `ack`, `nack`,
  and the `expire` info handler. Each routes its append through the
  `log_op/2` helper, which is a no-op for non-durable queues (where
  `state.log` is `nil`) and a `:ok = Log.append(...)` for durable ones.

  ### Crash recovery is a configurable data-loss window

  `Log.append/2` writes to the OS page cache. The fsync timer flushes that
  cache to disk every `:fsync_interval` ms (default 100; see `start_link/2`
  options). On an ungraceful crash (kill, OOM, power loss), up to one
  interval of recent writes can be lost. `terminate/2` handles graceful
  shutdown by syncing before closing — but `terminate/2` does NOT run on
  `:kill` or BEAM crashes, which is why the timer exists.

  ### Append failures crash the handler

  Each handler pattern-matches `:ok = Log.append(...)`. Why: log-first
  ordering only works if a failed append aborts BEFORE the in-memory
  mutation. A swallowed `{:error, reason}` would let state advance with no
  on-disk record — silent durability violation. Crashing the handler
  surfaces the failure (caller sees an exit) and lets the supervisor
  decide whether to restart.
  """

  use GenServer

  #  --- Public API ---
  # These wrap GenServer.call / GenServer.cast so callers never touch a PID.

  @doc """
  Starts a queue process under the given string `name`.

  Options:

    * `:visibility_timeout` — milliseconds before an unacked in-flight
      message is automatically requeued for another consumer. Default
      `30_000`.
    * `:max_attempts` — number of delivery attempts before a message is
      routed to the dead-letter queue. Default `5`.
    * `:durable` — when `true`, all state-changing operations are written
      to `priv/logs/<name>.log` before being applied to in-memory state,
      and the queue replays the log on init to recover state from prior
      runs. Default `false` (purely in-memory).
    * `:fsync_interval` — milliseconds between forced flushes of the log
      file's OS page cache to disk. Higher = better throughput, larger
      data-loss window on ungraceful crash. Default `100`. Ignored when
      `:durable` is `false`.
  """
  def start_link(name, opts \\ []) when is_binary(name) do
    GenServer.start_link(__MODULE__, {name, opts}, name: via(name))
  end

  @doc """
  Publishes `message` to the named queue. Returns `:ok`.

  The payload is opaque to the broker — any Elixir term is accepted.
  Producers and consumers agree on the format between themselves.
  """
  def publish(name, message) do
    GenServer.call(via(name), {:publish, message})
  end

  @doc """
  Fetches the next message from the named queue.

  Returns `{:ok, message, delivery_tag}` if a message is available, or
  `:empty` otherwise.

  The consumer owns the message until it calls `ack/2` or `nack/3` with
  the returned `delivery_tag`. If neither happens before the queue's
  visibility timeout elapses, the message is automatically requeued for
  another consumer.

  The `delivery_tag` identifies a specific *delivery attempt*, not the
  message itself. A redelivery of the same message is issued under a
  new tag.
  """
  def fetch(name, opts \\ []) do
    GenServer.call(via(name), {:fetch, opts}, :infinity)
  end

  @doc """
  Acknowledges a delivery. The message is permanently removed from the queue.

  Returns `:ok`, or `{:error, :unknown_tag}` if the tag is not currently
  in-flight — e.g. it was already acked/nacked, or its visibility timeout
  expired and the message was redelivered under a new tag.
  """
  def ack(name, delivery_tag) do
    GenServer.call(via(name), {:ack, delivery_tag})
  end

  @doc """
  Negatively acknowledges a delivery.

  Options:

    * `:requeue` — when `true` (default), the message is put back on the
      tail of the queue with its attempt count bumped. When `false`, the
      message is routed straight to the dead-letter queue regardless of
      attempt count.

  When `requeue: true` would push the attempt count to or past the queue's
  `max_attempts`, the message is routed to the dead-letter queue instead
  of being requeued.

  Returns `:ok`, or `{:error, :unknown_tag}` if the tag is not in-flight.
  """
  def nack(name, delivery_tag, opts \\ []) do
    GenServer.call(via(name), {:nack, delivery_tag, opts})
  end

  @doc "Returns the number of messages currently in the pending queue."
  def size(name) do
    GenServer.call(via(name), :size)
  end

  @doc """
  Returns the dead-letter queue's contents as a list, oldest first.

  Read-only: messages are not removed from the DLQ. Intended for
  inspection from `iex` or operational tooling.
  """
  def dlq_messages(name) do
    GenServer.call(via(name), :dlq_messages)
  end

  #  --- GenServer callbacks ---

  # Boot sequence (durable path):
  #   1. Replay the log on top of an empty state map. Log.replay/3 opens
  #      the file in read+write mode (so it can truncate torn entries),
  #      walks it via apply_helper/2, and closes its handle before
  #      returning. Crash on error — broken durability is louder than
  #      silently running in-memory.
  #   2. Open the long-lived append handle via Log.open/1. By the time
  #      this runs, replay's handle is already closed — so only ONE
  #      handle to the file exists at any moment (Windows-safe).
  #   3. Schedule the first fsync. The recurring timer is what protects
  #      against ungraceful crashes (kill, OOM, power loss); terminate/2
  #      only covers graceful shutdown.
  #   4. Re-schedule :expire timers for every tag the replay restored to
  #      in_flight. The original timers died with the previous process.
  #      Without this, fetched-but-not-acked messages from before the
  #      crash would be stuck in_flight forever. The new timers use the
  #      full visibility_timeout — we have no way to know how much had
  #      already elapsed before the crash, so we give the consumer a
  #      fresh window.
  #
  # Note: max_attempts and visibility_timeout come from opts, not from the
  # log. Restarting with different values affects how future ops behave
  # but does not change replayed state — the live handler resolves
  # requeue-vs-DLQ before writing the entry, so replay just applies the
  # recorded decision. This was the config-drift bug we fixed earlier.
  @impl true
  def init({name, opts}) do
    durable? = Keyword.get(opts, :durable, false)
    fsync_interval = Keyword.get(opts, :fsync_interval, 100)

    # Build the empty state with log: nil. For durable queues we'll fill
    # in the log handle AFTER replay finishes (see below).
    empty_state = %{
      pending: :queue.new(),
      dlq: :queue.new(),
      log: nil,
      max_attempts: Keyword.get(opts, :max_attempts, 5),
      in_flight: %{},
      visibility_timeout: Keyword.get(opts, :visibility_timeout, 30_000),
      fsync_interval: fsync_interval,
      waiters: :queue.new()
    }

    if durable? do
      # Order matters: replay → open → schedule timers.
      #
      # Why this order: Log.replay/3 opens the file in :read+:write mode
      # so it can truncate torn entries. Log.open/1 opens the same file
      # in :append mode for the live process. On Linux, having both
      # handles open simultaneously is fine. On Windows, the second open
      # can fail with :eacces depending on share-mode flags. By running
      # replay first (open-use-close inside that single call) and only
      # then opening the long-lived append handle, only ONE handle to the
      # file exists at any moment. Cross-platform safe.
      #
      # Bonus: if do_replay truncates a torn entry, the file shrinks
      # BEFORE the append handle opens — no risk of the append handle
      # caching a stale view of file length.
      {:ok, rebuilt_state} = MessageQueue.Log.replay(name, &apply_helper/2, empty_state)
      {:ok, log} = MessageQueue.Log.open(name)

      Process.send_after(self(), :fsync, fsync_interval)

      # Replay restored messages into in_flight (fetched-but-not-acked
      # before the crash), but no expire timers exist for them — those
      # timers died with the previous process. Without rescheduling, those
      # messages would sit in in_flight forever, never timing out for
      # redelivery. We can't know how much time had already passed before
      # the crash, so each gets a fresh full visibility_timeout window.
      for {tag, _envelope} <- rebuilt_state.in_flight do
        Process.send_after(self(), {:expire, tag}, rebuilt_state.visibility_timeout)
      end

      {:ok, %{rebuilt_state | log: log}}
    else
      {:ok, empty_state}
    end
  end

  @impl true
  def handle_call({:publish, message}, _from, state) do
    envelope = %{payload: message, attempt_count: 0}

    :ok = log_op(state.log, {:publish, envelope})

    case :queue.out(state.waiters) do
      {:empty, _} ->
        new_state = %{state | pending: :queue.in(envelope, state.pending)}
        {:reply, :ok, new_state}

      {{:value, waiter}, rest} ->
        # A consumer was already waiting — hand the envelope to them
        # directly. The envelope skips pending and goes into in_flight
        # right away, under a fresh delivery tag.
        #
        # We've already logged {:publish, envelope} above. But the live
        # code is now ALSO doing what a fetch would do (move envelope into
        # in_flight under a tag), so we have to log that too. Without this
        # extra entry, replay would see only the publish, drop the envelope
        # into pending, and then crash on the next ack/nack/expire entry
        # because it references a tag replay never put in in_flight.
        tag = make_ref()
        :ok = log_op(state.log, {:fetch, tag, envelope})

        new_state = %{state | waiters: rest, in_flight: Map.put(state.in_flight, tag, envelope)}
        Process.send_after(self(), {:expire, tag}, state.visibility_timeout)
        GenServer.reply(waiter, {:ok, envelope.payload, tag})
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:fetch, opts}, from, state) do
    timeout = Keyword.get(opts, :timeout, 0)

    case :queue.out(state.pending) do
      {:empty, _} ->
        # long polling
        if timeout <= 0 do
          {:reply, :empty, state}
        else
          Process.send_after(self(), {:waiter_timeout, from}, timeout)
          {:noreply, %{state | waiters: :queue.in(from, state.waiters)}}
        end

      {{:value, envelope}, rest} ->
        # make_ref/0 is durable-safe: refs round-trip through term_to_binary
        # and compare equal. So a {:fetch, tag, env} entry written here will
        # decode to the same tag value during replay, and a later
        # {:ack, tag} entry will match it via Map.pop in apply_helper.
        tag = make_ref()

        :ok = log_op(state.log, {:fetch, tag, envelope})

        new_state =
          %{state | pending: rest, in_flight: Map.put(state.in_flight, tag, envelope)}

        Process.send_after(self(), {:expire, tag}, state.visibility_timeout)
        {:reply, {:ok, envelope.payload, tag}, new_state}
    end
  end

  @impl true
  def handle_call({:ack, tag}, _from, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _map} ->
        # The tag isn't in in_flight. Nothing actually changed, so don't
        # write a log entry — there's no state change to record. (If we
        # did, replay would later try to ack a tag that's not there and
        # either no-op silently or crash, depending on which entry it is.)
        {:reply, {:error, :unknown_tag}, state}

      {_envelope, rest} ->
        :ok = log_op(state.log, {:ack, tag})
        {:reply, :ok, %{state | in_flight: rest}}
    end
  end

  @impl true
  def handle_call({:nack, tag, opts}, _from, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _rest} ->
        # Tag not in in_flight — no state change, no log entry. Same
        # reasoning as in the ack handler.
        {:reply, {:error, :unknown_tag}, state}

      {envelope, rest} ->
        new_state = %{state | in_flight: rest}

        case Keyword.get(opts, :requeue, true) do
          true ->
            envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

            if envelope.attempt_count >= state.max_attempts do
              :ok = log_op(state.log, {:dlq, tag, envelope})
              {:reply, :ok, %{new_state | dlq: :queue.in(envelope, new_state.dlq)}}
            else
              :ok = log_op(state.log, {:requeue, tag, envelope})
              {:reply, :ok, %{new_state | pending: :queue.in(envelope, new_state.pending)}}
            end

          false ->
            :ok = log_op(state.log, {:dlq, tag, envelope})
            {:reply, :ok, %{new_state | dlq: :queue.in(envelope, new_state.dlq)}}
        end
    end
  end

  @impl true
  def handle_call(:size, _from, state) do
    {:reply, :queue.len(state.pending), state}
  end

  @impl true
  def handle_call(:dlq_messages, _from, state) do
    {:reply, :queue.to_list(state.dlq), state}
  end

  @impl true
  def handle_info({:expire, tag}, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _rest} ->
        # The timer fired but the consumer already acked or nacked this
        # tag before the timeout — so it's no longer in in_flight.
        # Common and harmless. No state change, no log entry.
        {:noreply, state}

      {envelope, rest} ->
        new_state = %{state | in_flight: rest}
        envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

        if envelope.attempt_count >= state.max_attempts do
          :ok = log_op(state.log, {:dlq_from_expire, tag, envelope})
          {:noreply, %{new_state | dlq: :queue.in(envelope, new_state.dlq)}}
        else
          :ok = log_op(state.log, {:requeue_from_expire, tag, envelope})
          {:noreply, %{new_state | pending: :queue.in(envelope, new_state.pending)}}
        end
    end
  end

  @impl true
  def handle_info({:waiter_timeout, from}, state) do
    if not :queue.member(from, state.waiters) do
      {:noreply, state}
    else
      new_waiters = :queue.filter(fn x -> x != from end, state.waiters)
      GenServer.reply(from, :empty)
      {:noreply, %{state | waiters: new_waiters}}
    end
  end

  @impl true
  def handle_info(:fsync, state) do
    # `:ok = ...` is load-bearing. If :file.sync fails (disk full, hardware
    # error), Log.sync returns {:error, reason} and the match crashes the
    # process. Why crash: a failed fsync means the OS couldn't flush page
    # cache to disk, so any "successful" append since the last good sync
    # might be lost. Continuing to ack publishes would be lying. Crashing
    # surfaces the failure to the supervisor and to publishers (their
    # GenServer.call returns an exit). Same crash-on-failure stance as
    # log_op/2 uses for Log.append.
    :ok = MessageQueue.Log.sync(state.log)
    Process.send_after(self(), :fsync, state.fsync_interval)
    {:noreply, state}
  end

  # terminate/2 runs on graceful shutdown only:
  #   - supervisor-initiated stop
  #   - GenServer returns {:stop, reason, state}
  #   - linked parent dies with :trap_exit set
  #
  # It does NOT run on Process.exit(pid, :kill), BEAM crashes, OOM kills,
  # or power loss. Those paths rely on the periodic fsync (handle_info/2
  # :fsync clause) having flushed recent writes. Do not delete the fsync
  # timer thinking just terminate is sufficient.
  @impl true
  def terminate(_reason, state) do
    if state.log != nil do
      MessageQueue.Log.sync(state.log)
      MessageQueue.Log.close(state.log)
    end

    :ok
  end

  #  --- Helpers ---

  defp via(name) do
    {:via, Registry, {MessageQueue.Registry, name}}
  end

  # The one place we write entries to the log file. Two jobs:
  #
  #   1. If the queue isn't durable (no log file), do nothing.
  #   2. If it IS durable and the write fails, crash this process.
  #
  # The `:ok = ...` is what makes #2 happen. Log.append/2 returns either
  # `:ok` (write succeeded) or `{:error, reason}` (disk full, file gone,
  # etc.). Using `=` as a pattern match means: if the right side isn't
  # exactly `:ok`, the match fails and the process dies with MatchError.
  #
  # Why crash instead of just ignoring the error: if we ignored it, the
  # handler would then go ahead and update the in-memory queue state.
  # Now memory has a change that disk has no record of. On crash and
  # restart, replay rebuilds from disk — so the change is gone. The whole
  # point of "log first, then mutate" is to never let memory get ahead of
  # disk. Crashing the handler keeps that promise: if we couldn't write,
  # we don't mutate.
  defp log_op(nil, _entry), do: :ok
  defp log_op(handle, entry), do: :ok = MessageQueue.Log.append(handle, entry)

  # Replay's rule-book. Called by Log.replay/3 once per log entry while
  # rebuilding state on boot. For each kind of entry, this says how to
  # update the in-memory state.
  #
  # Same shape as the live handlers above, minus three things they do:
  #
  #   - Replying to callers. There is no caller during replay — we're
  #     reading historical events off disk, not handling live requests.
  #   - Scheduling timers. The original timers already fired (or didn't)
  #     before the crash. Replay is not "running the queue again," it's
  #     just rebuilding state. Fresh expire timers for messages still in
  #     in_flight at the end of replay are scheduled by init/1 itself
  #     (see the for-loop after the replay call).
  #   - Touching waiters. Waiters exist only while the queue is running.
  #     During boot, nobody is parked yet.
  #
  # Replay starts from the empty state that init/1 just built. So
  # waiters, max_attempts, visibility_timeout, and fsync_interval all
  # come from opts passed to start_link — never from the log.
  defp apply_helper(entry, state) do
    case entry do
      {:publish, envelope} ->
        %{state | pending: :queue.in(envelope, state.pending)}

      {:fetch, tag, envelope} ->
        # Defensive: this pin (`^envelope`) does nothing under correct
        # operation. The live fetch handler always pops the same envelope
        # the publish handler put in pending, so the log entries match the
        # state precisely. The pin is here to catch FUTURE breakage:
        #
        #   - If a new bug elsewhere causes an entry to land in the log
        #     without matching state changes (a la the publish-to-waiter
        #     bug we fixed), the pending head can drift from the logged
        #     envelope. Without the pin, replay silently produces a wrong
        #     state and the bug surfaces much later in mysterious ways.
        #   - If the log file is corrupted (bit flip, hand-editing, bad
        #     disk), one entry's envelope can disagree with another's.
        #
        # The pin makes any such drift crash here with MatchError instead
        # of being absorbed quietly. Cheap insurance, no runtime cost.
        {{:value, ^envelope}, rest} = :queue.out(state.pending)
        %{state | pending: rest, in_flight: Map.put(state.in_flight, tag, envelope)}

      {:ack, tag} ->
        %{state | in_flight: Map.delete(state.in_flight, tag)}

      # two seperate versions for requeue and dlq for auditability only. they behave identically
      {:dlq, tag, envelope} ->
        %{state | in_flight: Map.delete(state.in_flight, tag), dlq: :queue.in(envelope,state.dlq)}

      {:requeue, tag, envelope} ->
        %{state | in_flight: Map.delete(state.in_flight, tag), pending: :queue.in(envelope, state.pending)}

      {:dlq_from_expire, tag, envelope} ->
        %{state | in_flight: Map.delete(state.in_flight, tag), dlq: :queue.in(envelope,state.dlq)}

      {:requeue_from_expire, tag, envelope} ->
        %{state | in_flight: Map.delete(state.in_flight, tag), pending: :queue.in(envelope, state.pending)}

      # handle unexpected shapes by crashing loudly with error info
      other -> raise "MessageQueue.Queue: unknown log entry during replay: #{inspect(other)}"
    end
  end
end
