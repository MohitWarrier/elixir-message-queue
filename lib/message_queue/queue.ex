defmodule MessageQueue.Queue do
  @moduledoc """
  A GenServer representing one work queue. Owns the in-memory FIFO and the
  in-flight map for that queue.

  Phase 1: in-memory only. No visibility timeout (Phase 2), no DLQ (Phase 3),
  no long-polling (Phase 4), no supervision (Phase 5), no durability (Phase 6).
  """

  use GenServer

  #  --- Public API ---
  # These wrap GenServer.call / GenServer.cast so callers never touch a PID.

  @doc """
  Start a queue process with the given string name.

  Decision to make:
    - How is the process registered so callers can address it by string name?
      Two reasonable options for Phase 1:
        a) atom name: `name: String.to_atom("queue_" <> name)` — easy, but
           atoms aren't garbage-collected. Fine while learning.
        b) Registry-based `{:via, Registry, ...}` — the eventual right answer,
           but you don't have a Registry running yet. Phase 5 sets it up.
      Pick (a) for now; you'll migrate in Phase 5.
  """
  def start_link(name, opts \\ []) when is_binary(name) do
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  @doc """
  Publish a message. Returns `:ok`.
  """
  def publish(name, message) do
    GenServer.call(via(name), {:publish, message})
  end

  @doc """
  Fetch the next message. Returns `{:ok, message, delivery_tag}` or `:empty`.

  Notes:
    - `delivery_tag` uniquely identifies *this delivery attempt* — not the
      message. If the same message is redelivered later (Phase 2), it gets
      a new tag. `make_ref/0` is the idiomatic source.
    - The message stays "owned" by the consumer until they ack or nack it.
      In Phase 1, that's forever if they don't — feel the pain.
  """
  def fetch(name) do
    GenServer.call(via(name), :fetch)
  end

  @doc """
  Ack a delivery. The message is gone for good.
  Returns {:error, :unknown_tag} if tag doesnt exist
  """
  def ack(name, delivery_tag) do
    GenServer.call(via(name), {:ack, delivery_tag})
  end

  @doc """
  Nack a delivery.
       on retry message send to tail
    - `requeue: false` → drop the message. (Phase 3: route to DLQ instead.)
  """
  def nack(name, delivery_tag, opts \\ []) do
    GenServer.call(via(name), {:nack, delivery_tag, opts})
  end

  @doc "Current size of the pending queue (handy for tests and iex)."
  def size(name) do
    GenServer.call(via(name), :size)
  end

  #  --- GenServer callbacks ---

  @impl true
  def init(opts) do
    {:ok,
     %{
       pending: :queue.new(),
       in_flight: %{},
       visibility_timeout: Keyword.get(opts, :visibility_timeout, 30_000)
     }}
  end

  @impl true
  def handle_call({:publish, message}, _from, state) do
    envelope = %{payload: message, attempt_count: 0}
    new_state = %{state | pending: :queue.in(envelope, state.pending)}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:fetch, _from, state) do
    case :queue.out(state.pending) do
      {:empty, _} ->
        {:reply, :empty, state}

      {{:value, envelope}, rest} ->
        tag = make_ref()

        new_state =
          %{state | pending: rest, in_flight: Map.put(state.in_flight, tag, envelope)}

        Process.send_after(self(), {:expire, tag}, state.visibility_timeout)
        {:reply, {:ok, envelope.payload, tag}, new_state}
    end
  end

  @impl true
  def handle_call({:ack, tag}, _from, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _map} -> {:reply, {:error, :unknown_tag}, state}
      {_envelope, rest} -> {:reply, :ok, %{state | in_flight: rest}}
    end
  end

  @impl true
  def handle_call({:nack, tag, opts}, _from, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _rest} ->
        {:reply, {:error, :unknown_tag}, state}

      {envelope, rest} ->
        new_state = %{state | in_flight: rest}

        case Keyword.get(opts, :requeue, true) do
          true ->
            envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)
            {:reply, :ok, %{new_state | pending: :queue.in(envelope, new_state.pending)}}

          false ->
            {:reply, :ok, new_state}
        end
    end
  end

  @impl true
  def handle_call(:size, _from, state) do
    {:reply, :queue.len(state.pending), state}
  end

  @impl true
  def handle_info({:expire, tag}, state) do
    case Map.pop(state.in_flight, tag) do
      {nil, _rest} ->
        {:noreply, state}

      {envelope, rest} ->
        new_state = %{state | in_flight: rest}
        envelope = Map.update!(envelope, :attempt_count, fn val -> val + 1 end)
        {:noreply, %{new_state | pending: :queue.in(envelope, new_state.pending)}}
    end
  end

  #  --- Helpers ---

  defp via(name) do
    String.to_atom("queue" <> name)
  end
end
