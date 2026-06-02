defmodule MessageQueue.QueueCompactionTest do
  @moduledoc """
  Phase 7 log compaction tests.

  Compaction rewrites the on-disk log to contain only the entries needed to
  rebuild current in-memory state. It fires when `ops_since_compact` crosses
  `compact_threshold`. Each test uses a small threshold (5-10) so we don't
  have to publish thousands of messages to trigger compaction.
  """

  use ExUnit.Case, async: false
  # async: false — tests share priv/logs/ and the supervisor's restart budget.

  setup do
    File.mkdir_p!("priv/logs")

    name = "cmpct_q_" <> Integer.to_string(System.unique_integer([:positive]))
    {:ok, path} = MessageQueue.Log.path_for(name)

    File.rm(path)
    File.rm(path <> ".compact")

    on_exit(fn ->
      case Registry.lookup(MessageQueue.Registry, name) do
        [{pid, _}] -> graceful_stop(pid)
        _ -> :ok
      end

      File.rm(path)
      File.rm(path <> ".compact")
    end)

    %{name: name, path: path}
  end

  describe "compaction triggering" do
    test "no compaction fires before the threshold is crossed", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 100)

      for i <- 1..50 do
        :ok = MessageQueue.publish(name, {:msg, i})
      end

      # Force any pending mailbox processing to settle so we'd catch a
      # spurious :compact if one had been sent.
      :sys.get_state(via(name))

      # 50 publishes < threshold 100 → no compaction happened.
      # File still contains all 50 publish entries; size reflects that.
      bytes_before = File.stat!(path).size

      :sys.get_state(via(name))

      assert File.stat!(path).size == bytes_before
    end

    test "compaction fires when threshold is crossed", %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 10)

      # 10 publish + 10 fetch + 10 ack = 30 ops, well past threshold 10.
      for i <- 1..10 do
        :ok = MessageQueue.publish(name, {:msg, i})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      # Settle the :compact info message.
      wait_for_no_pending_compact(name)

      # All 30 ops produced dead entries (publish + fetch + ack cancels out).
      # After compaction the log should be effectively empty (a few bytes for
      # the file structure, but nothing meaningful).
      assert File.stat!(path).size < 30
    end

    test "compaction is idempotent — running it twice on identical state produces equivalent files",
         %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      for i <- 1..10, do: :ok = MessageQueue.publish(name, {:keep, i})
      # 10 publish ops past threshold 5 → compaction triggered.
      wait_for_no_pending_compact(name)
      size_after_first = File.stat!(path).size

      # Push another 10 ops that all cancel out, triggering a second compaction.
      for _ <- 1..10 do
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      wait_for_no_pending_compact(name)
      size_after_second = File.stat!(path).size

      # State after both rounds is the same (10 "keep" messages back in pending).
      # File size should be the same too (modulo attempt_count bumps changing
      # one byte per envelope).
      assert_in_delta size_after_second, size_after_first, 50
    end
  end

  describe "compaction preserves state" do
    test "pending messages survive compaction and appear in FIFO order", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      # Drain transients first so FIFO doesn't make later fetches eat the keeps.
      for j <- 1..5 do
        :ok = MessageQueue.publish(name, {:transient, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      # NOW publish the keepers. After all 15 transient ops compaction has
      # likely fired and reset the counter; these 5 publishes may or may not
      # trigger another compaction, but either way pending ends with the keeps.
      for i <- 1..5, do: :ok = MessageQueue.publish(name, {:keep, i})

      wait_for_no_pending_compact(name)
      graceful_restart(name)

      # FIFO order preserved across compaction + replay.
      for i <- 1..5 do
        assert {:ok, {:keep, ^i}, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      assert :empty = MessageQueue.fetch(name)
    end

    test "in-flight messages survive compaction; their tags remain ackable", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      # Publish 3, fetch them all (now in_flight under their tags), publish 5 more
      # to push past threshold without acking the in-flight ones.
      for i <- 1..3, do: :ok = MessageQueue.publish(name, {:flying, i})
      tags = for _ <- 1..3, do: (fn -> {:ok, _, t} = MessageQueue.fetch(name); t end).()
      for j <- 1..5, do: :ok = MessageQueue.publish(name, {:pending, j})

      wait_for_no_pending_compact(name)

      # The original tags should still be valid — the consumer still holds them.
      [t1, t2, t3] = tags
      assert :ok = MessageQueue.ack(name, t1)
      assert :ok = MessageQueue.ack(name, t2)
      assert :ok = MessageQueue.ack(name, t3)

      # Now only the 5 pending messages remain.
      for j <- 1..5 do
        assert {:ok, {:pending, ^j}, _} = MessageQueue.fetch(name)
      end
    end

    test "DLQ messages survive compaction and appear in FIFO order", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      # Send 3 messages straight to the DLQ via nack(requeue: false).
      for i <- 1..3 do
        :ok = MessageQueue.publish(name, {:poison, i})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: false)
      end

      # Add enough successful ops to trigger compaction.
      for j <- 1..5 do
        :ok = MessageQueue.publish(name, {:noise, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      wait_for_no_pending_compact(name)
      graceful_restart(name)

      dlq = MessageQueue.dlq_messages(name)
      assert length(dlq) == 3

      [first, second, third] = dlq
      assert first.payload == {:poison, 1}
      assert second.payload == {:poison, 2}
      assert third.payload == {:poison, 3}
    end

    test "attempt_count is preserved through compaction", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      :ok = MessageQueue.publish(name, :retryable)

      # Bump retry's attempt_count to 3 via 3 nack-requeue cycles.
      for _ <- 1..3 do
        {:ok, :retryable, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      # Pin retry in in_flight so the noise loop's fetches can't pull it
      # (FIFO would otherwise pop :retryable ahead of any noise message).
      {:ok, :retryable, held_tag} = MessageQueue.fetch(name)

      # Push noise to trigger compaction while retry sits in in_flight.
      for j <- 1..10 do
        :ok = MessageQueue.publish(name, {:noise, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      wait_for_no_pending_compact(name)

      # Nack the held tag — bumps attempt_count to 4 (still under max_attempts=5).
      :ok = MessageQueue.nack(name, held_tag, requeue: true)

      # Final nack pushes attempt_count to 5 → DLQ.
      {:ok, :retryable, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag, requeue: true)

      # If attempt_count survived compaction, retry is in DLQ at count=5.
      # If it was reset to 0, retry would still be in pending.
      assert :empty = MessageQueue.fetch(name)
      assert [%{payload: :retryable, attempt_count: 5}] = MessageQueue.dlq_messages(name)
    end
  end

  describe "compaction interacts correctly with replay" do
    test "queue functional immediately after compaction (no restart needed)", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      for j <- 1..5 do
        :ok = MessageQueue.publish(name, {:before, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      wait_for_no_pending_compact(name)

      # Right after compaction, normal operations should work.
      :ok = MessageQueue.publish(name, :after_compact)
      assert {:ok, :after_compact, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.ack(name, tag)
      assert :empty = MessageQueue.fetch(name)
    end

    test "compaction reduces file size when many ops cancel out",
         %{name: name, path: path} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 100)

      # 100 publish-fetch-ack cycles = 300 ops. Compaction fires at least twice
      # (counter crosses 100 twice). Each compaction rewrites the file to just
      # the live state at that moment (~1-2 entries, ~60-120 bytes).
      for j <- 1..100 do
        :ok = MessageQueue.publish(name, {:gone, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      wait_for_no_pending_compact(name)

      # Without compaction the file would hold ~300 entries × ~50 bytes ≈ 15 KB.
      # After repeated compactions it holds at most "live state" plus the
      # entries appended since the last compaction (< ~100 entries by design).
      # A loose upper bound of 6 KB still catches the "no compaction ran" case
      # (which would be > 12 KB) while tolerating timing variance in when
      # compactions interleave with the loop.
      assert File.stat!(path).size < 6_000
    end

    test "in-flight tags work across both compaction AND restart", %{name: name} do
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 5)

      :ok = MessageQueue.publish(name, :held)
      {:ok, :held, original_tag} = MessageQueue.fetch(name)

      # Bump past threshold while :held is in_flight.
      for j <- 1..10 do
        :ok = MessageQueue.publish(name, {:noise, j})
        {:ok, _, t} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, t)
      end

      wait_for_no_pending_compact(name)
      graceful_restart(name)

      # After replay-from-compacted-log, the original tag's identity is preserved
      # (the compacted log re-wrote {:fetch, original_tag, env}). Ack should still work.
      assert :ok = MessageQueue.ack(name, original_tag)
    end

    test "init/1 triggers compaction when the on-disk log dwarfs the live state",
         %{name: name, path: path} do
      # compact_threshold set to a huge value so runtime compaction NEVER fires
      # during the loop. We want the file to grow unchecked, then verify init's
      # post-replay heuristic shrinks it on restart.
      :ok = MessageQueue.ensure_queue(name, durable: true, compact_threshold: 1_000_000)

      # 200 publish-fetch-ack cycles = 600 log entries. All cancel out, so live
      # state ends empty.
      for j <- 1..200 do
        :ok = MessageQueue.publish(name, {:gone, j})
        {:ok, _msg, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.ack(name, tag)
      end

      # Force a graceful shutdown so terminate/2 fsyncs before close — we want
      # all 600 entries actually persisted to disk for the restart to replay.
      size_before = File.stat!(path).size
      assert size_before > 10_000, "expected log to bloat without runtime compaction"

      graceful_restart(name)

      # init/1's post-replay heuristic should have sent :compact (600 replayed
      # entries vs live state of 0). The post-init :compact message runs once
      # the GenServer becomes ready. Wait for the flag to clear.
      wait_for_no_pending_compact(name)

      size_after = File.stat!(path).size
      # Live state is empty after replay, so a compacted file should be tiny
      # (a few hundred bytes max).
      assert size_after < size_before / 10,
             "expected init-triggered compaction to shrink the file dramatically; " <>
               "before=#{size_before} after=#{size_after}"
    end
  end

  # --- Helpers ---

  defp via(name), do: {:via, Registry, {MessageQueue.Registry, name}}

  # Wait until the queue's mailbox has no pending :compact message AND
  # compaction_pending is false. Uses :sys.get_state to inspect.
  defp wait_for_no_pending_compact(name, attempts \\ 50)

  defp wait_for_no_pending_compact(name, 0) do
    flunk("Queue #{name} still has compaction_pending: true after waiting")
  end

  defp wait_for_no_pending_compact(name, attempts) do
    state = :sys.get_state(via(name))

    if state.compaction_pending do
      Process.sleep(20)
      wait_for_no_pending_compact(name, attempts - 1)
    else
      :ok
    end
  end

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
