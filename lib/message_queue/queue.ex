defmodule MessageQueue.Queue do
  @moduledoc """
  A GenServer representing one work queue.

  Each queue owns its own in-memory FIFO of pending messages, a map of
  in-flight (delivered but unacked) messages, and a dead-letter queue for
  messages that have exhausted their retry budget.

  Delivery is at-least-once: a fetched message stays in-flight until the
  consumer acks or nacks it, or until the visibility timeout expires and
  it is redelivered to another consumer.
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

  @impl true
  def init({name, opts}) do
    {:ok, log} =
      case Keyword.get(opts, :durable, false) do
        true -> MessageQueue.Log.open(name)
        false -> {:ok, nil}
      end

    if log != nil, do: Process.send_after(self(), :fsync, 100)

    empty_state = %{
      pending: :queue.new(),
      dlq: :queue.new(),
      log: log,
      max_attempts: Keyword.get(opts, :max_attempts, 5),
      in_flight: %{},
      visibility_timeout: Keyword.get(opts, :visibility_timeout, 30_000),
      waiters: :queue.new()
    }

    if log == nil do
      {:ok, empty_state}
    else
      {:ok, rebuilt_state} = MessageQueue.Log.replay(name, &apply_helper/2, empty_state)
      {:ok, rebuilt_state}
    end
  end

  @impl true
  def handle_call({:publish, message}, _from, state) do
    envelope = %{payload: message, attempt_count: 0}

    if state.log != nil, do: MessageQueue.Log.append(state.log, {:publish, envelope})

    case :queue.out(state.waiters) do
      {:empty, _} ->
        new_state = %{state | pending: :queue.in(envelope, state.pending)}
        {:reply, :ok, new_state}

      {{:value, waiter}, rest} ->
        tag = make_ref()
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
        tag = make_ref()

        if state.log != nil, do: MessageQueue.Log.append(state.log, {:fetch, tag, envelope})

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
        {:reply, {:error, :unknown_tag}, state}

      {_envelope, rest} ->
        if state.log != nil, do: MessageQueue.Log.append(state.log, {:ack, tag})
        {:reply, :ok, %{state | in_flight: rest}}
    end
  end

  @impl true
  def handle_call({:nack, tag, opts}, _from, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _rest} ->
        {:reply, {:error, :unknown_tag}, state}

      {envelope, rest} ->
        if state.log != nil, do: MessageQueue.Log.append(state.log, {:nack, tag, opts})
        new_state = %{state | in_flight: rest}

        case Keyword.get(opts, :requeue, true) do
          true ->
            envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

            if envelope.attempt_count >= state.max_attempts do
              {:reply, :ok, %{new_state | dlq: :queue.in(envelope, new_state.dlq)}}
            else
              {:reply, :ok, %{new_state | pending: :queue.in(envelope, new_state.pending)}}
            end

          false ->
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
        {:noreply, state}

      {envelope, rest} ->
        if state.log != nil, do: MessageQueue.Log.append(state.log, {:expire, tag})
        new_state = %{state | in_flight: rest}
        envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

        if envelope.attempt_count >= state.max_attempts do
          {:noreply, %{new_state | dlq: :queue.in(envelope, new_state.dlq)}}
        else
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
    MessageQueue.Log.sync(state.log)
    Process.send_after(self(), :fsync, 100)
    {:noreply, state}
  end

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

  defp apply_helper(entry, state) do
    case entry do
      {:publish, envelope} ->
        %{state | pending: :queue.in(envelope, state.pending)}

      {:fetch, tag, envelope} ->
        {{:value, _envelope}, rest} = :queue.out(state.pending)
        %{state | pending: rest, in_flight: Map.put(state.in_flight, tag, envelope)}

      {:ack, tag} ->
        %{state | in_flight: Map.delete(state.in_flight, tag)}

      {:nack, tag, opts} ->
        {envelope, rest} = Map.pop(state.in_flight, tag)
        new_state = %{state | in_flight: rest}

        case Keyword.get(opts, :requeue, true) do
          true ->
            envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

            if envelope.attempt_count >= state.max_attempts do
              %{new_state | dlq: :queue.in(envelope, new_state.dlq)}
            else
              %{new_state | pending: :queue.in(envelope, new_state.pending)}
            end

          false ->
            %{new_state | dlq: :queue.in(envelope, new_state.dlq)}
        end

      {:expire, tag} ->
        {envelope, rest} = Map.pop(state.in_flight, tag)
        new_state = %{state | in_flight: rest}
        envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)

        if envelope.attempt_count >= state.max_attempts do
          %{new_state | dlq: :queue.in(envelope, new_state.dlq)}
        else
          %{new_state | pending: :queue.in(envelope, new_state.pending)}
        end
    end
  end
end
