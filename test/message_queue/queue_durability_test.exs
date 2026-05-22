defmodule MessageQueue.QueueDurabilityTest do
  @moduledoc """
  End-to-end durability tests for Phase 6.

  Each test:
    1. Starts a durable queue under the supervisor (`ensure_queue/2`).
    2. Performs some operations.
    3. Restarts the queue process (graceful or via kill).
    4. Asserts that the recovered state matches what the live state would have been.

  ## Graceful vs ungraceful restart

  - **Graceful** (`graceful_restart/1`): calls `GenServer.stop/2` which runs
    `terminate/2`. This flushes the log and closes the file cleanly.
    Use this for most tests — it's deterministic, no sleep needed.
  - **Ungraceful** (`kill_restart/1`): `Process.exit(pid, :kill)`, no
    `terminate/2`. Relies on the periodic fsync (100 ms) having flushed
    recent writes to disk. Test waits past the fsync interval before killing.
  """

  use ExUnit.Case, async: false
  # async: false — tests share priv/logs/ and the supervisor's restart budget.

  setup do
    File.mkdir_p!("priv/logs")

    name = "dur_q_" <> Integer.to_string(System.unique_integer([:positive]))
    {:ok, path} = MessageQueue.Log.path_for(name)

    # Wipe any stale log left over from a previous run with the same name —
    # System.unique_integer/1 resets across BEAM restarts so collisions happen.
    File.rm(path)

    on_exit(fn ->
      case Registry.lookup(MessageQueue.Registry, name) do
        [{pid, _}] -> graceful_stop(pid)
        _ -> :ok
      end

      File.rm(path)
    end)

    %{name: name, path: path}
  end

  describe "non-durable queue" do
    test "default (no durable option) does not create a log file", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name)
      refute File.exists?(path)
    end

    test "durable: false explicitly does not create a log file", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: false)
      refute File.exists?(path)
    end
  end

  describe "durable queue file lifecycle" do
    test "durable: true creates the log file on start", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      assert File.exists?(path)
    end

    test "publishing writes bytes to the log file", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :hello)

      # Fsync interval is 100 ms — wait past it so OS page cache flushes.
      Process.sleep(150)

      assert File.stat!(path).size > 0
    end
  end

  describe "publish + restart" do
    test "a single published message survives a graceful restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :survives)

      graceful_restart(name)

      assert {:ok, :survives, _tag} = MessageQueue.fetch(name)
    end

    test "FIFO order is preserved across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :a)
      :ok = MessageQueue.publish(name, :b)
      :ok = MessageQueue.publish(name, :c)

      graceful_restart(name)

      assert {:ok, :a, _} = MessageQueue.fetch(name)
      assert {:ok, :b, _} = MessageQueue.fetch(name)
      assert {:ok, :c, _} = MessageQueue.fetch(name)
      assert MessageQueue.fetch(name) == :empty
    end

    test "publishing after restart appends to the restored queue", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :before)

      graceful_restart(name)

      :ok = MessageQueue.publish(name, :after)

      assert {:ok, :before, _} = MessageQueue.fetch(name)
      assert {:ok, :after, _} = MessageQueue.fetch(name)
    end
  end

  describe "ack + restart" do
    test "acked messages do not reappear after restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :work)
      {:ok, :work, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.ack(name, tag)

      graceful_restart(name)

      assert MessageQueue.fetch(name) == :empty
    end

    test "ack only removes the acked message; other in-flight messages survive", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :acked)
      :ok = MessageQueue.publish(name, :pending)

      {:ok, :acked, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.ack(name, tag)

      graceful_restart(name)

      # :acked is gone (acked); :pending is back in pending.
      assert {:ok, :pending, _} = MessageQueue.fetch(name)
      assert MessageQueue.fetch(name) == :empty
    end
  end

  describe "nack + restart" do
    test "nack(requeue: true) puts the message back in pending across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :retry_me)

      {:ok, :retry_me, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag, requeue: true)

      graceful_restart(name)

      assert {:ok, :retry_me, _} = MessageQueue.fetch(name)
    end

    test "nack(requeue: false) routes the message to DLQ across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :poison)

      {:ok, :poison, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag, requeue: false)

      graceful_restart(name)

      assert MessageQueue.fetch(name) == :empty
      assert [%{payload: :poison}] = MessageQueue.dlq_messages(name)
    end

    test "default nack (no opts) requeues across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :default_nack)

      {:ok, :default_nack, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag)

      graceful_restart(name)

      assert {:ok, :default_nack, _} = MessageQueue.fetch(name)
    end
  end

  describe "DLQ + restart" do
    test "messages routed to DLQ via max_attempts persist across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, max_attempts: 2)
      :ok = MessageQueue.publish(name, :doomed)

      for _ <- 1..2 do
        {:ok, :doomed, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      graceful_restart(name)

      assert MessageQueue.fetch(name) == :empty
      assert [%{payload: :doomed, attempt_count: 2}] = MessageQueue.dlq_messages(name)
    end

    test "DLQ FIFO order preserved across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)

      for msg <- [:first, :second, :third] do
        :ok = MessageQueue.publish(name, msg)
        {:ok, ^msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: false)
      end

      graceful_restart(name)

      assert [
               %{payload: :first},
               %{payload: :second},
               %{payload: :third}
             ] = MessageQueue.dlq_messages(name)
    end

    test "attempt_count is preserved across restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, max_attempts: 5)
      :ok = MessageQueue.publish(name, :flaky)

      # Two failed attempts before restart.
      for _ <- 1..2 do
        {:ok, :flaky, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      graceful_restart(name)

      # Three more nacks after restart push attempt_count to 5 = max_attempts → DLQ.
      for _ <- 1..3 do
        {:ok, :flaky, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      assert [%{payload: :flaky, attempt_count: 5}] = MessageQueue.dlq_messages(name)
    end
  end

  describe "ungraceful crash (Process.exit :kill)" do
    test "messages written more than one fsync interval ago survive a kill", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :survives_kill)

      # Wait past the 100 ms fsync interval so OS page cache hits disk.
      Process.sleep(150)

      [{pid, _}] = Registry.lookup(MessageQueue.Registry, name)
      Process.exit(pid, :kill)
      wait_for_restart(name, pid)

      assert {:ok, :survives_kill, _} = MessageQueue.fetch(name)
    end

    test "DLQ contents survive a kill past the fsync interval", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true)
      :ok = MessageQueue.publish(name, :poison)
      {:ok, :poison, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag, requeue: false)

      Process.sleep(150)

      [{pid, _}] = Registry.lookup(MessageQueue.Registry, name)
      Process.exit(pid, :kill)
      wait_for_restart(name, pid)

      assert [%{payload: :poison}] = MessageQueue.dlq_messages(name)
    end
  end

  describe "fixed Phase 6 edge cases" do
    test "in-flight messages get fresh visibility timers after restart", %{name: name} do
      # init/1 walks the restored in_flight and schedules a fresh expire
      # timer for each tag, using the queue's visibility_timeout. Without
      # this, a fetched-but-not-acked message from before the crash would
      # stay in in_flight forever after restart.
      :ok = MessageQueue.ensure_queue(name, durable: true, visibility_timeout: 50)
      :ok = MessageQueue.publish(name, :stuck)
      {:ok, :stuck, _tag} = MessageQueue.fetch(name)

      graceful_restart(name)

      # After the fresh visibility_timeout fires post-restart, the message
      # should be back in pending and fetchable.
      Process.sleep(120)
      assert {:ok, :stuck, _new_tag} = MessageQueue.fetch(name)
    end

    test "publish-directly-to-parked-waiter is replayed correctly", %{name: name} do
      # The publish handler's waiter branch logs BOTH {:publish, env} and
      # {:fetch, tag, env}, so replay reconstructs the same state the live
      # queue had — envelope in in_flight under tag, not stranded in pending.
      :ok = MessageQueue.ensure_queue(name, durable: true)

      task = Task.async(fn -> MessageQueue.fetch(name, timeout: 500) end)
      Process.sleep(20)

      :ok = MessageQueue.publish(name, :direct)
      {:ok, :direct, tag} = Task.await(task)
      :ok = MessageQueue.ack(name, tag)

      graceful_restart(name)

      # Envelope was acked before restart — should be gone, not back in pending.
      assert MessageQueue.fetch(name) == :empty
    end
  end

  # --- Helpers ---

  defp graceful_restart(name) do
    [{pid, _}] = Registry.lookup(MessageQueue.Registry, name)
    GenServer.stop(pid, :shutdown)
    wait_for_restart(name, pid)
  end

  defp wait_for_restart(name, old_pid, attempts \\ 50)

  defp wait_for_restart(name, old_pid, 0),
    do: flunk("Queue #{name} did not restart in time (old pid: #{inspect(old_pid)})")

  defp wait_for_restart(name, old_pid, attempts) do
    case Registry.lookup(MessageQueue.Registry, name) do
      [{new_pid, _}] when new_pid != old_pid ->
        new_pid

      _ ->
        Process.sleep(20)
        wait_for_restart(name, old_pid, attempts - 1)
    end
  end

  defp graceful_stop(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 1_000)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end
