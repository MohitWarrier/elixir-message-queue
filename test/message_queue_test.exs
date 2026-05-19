defmodule MessageQueueTest do
  use ExUnit.Case, async: false

  # async: false — Phase 1 registers queues by atom name. Parallel tests
  # using the same name would collide. Phase 5 (Registry) makes this safer.

  # Each test gets a fresh queue with a unique name. No teardown needed —
  # the process dies with the test by default once we move to supervised
  # queues; for now an unlinked process just lingers harmlessly.
  setup do
    name = "test_q_" <> Integer.to_string(System.unique_integer([:positive]))
    {:ok, _pid} = MessageQueue.Queue.start_link(name)
    %{q: name}
  end

  describe "publish + fetch" do
    test "fetch on an empty queue returns :empty", %{q: q} do
      assert MessageQueue.fetch(q) == :empty
    end

    test "publish then fetch returns the message and a delivery tag", %{q: q} do
      :ok = MessageQueue.publish(q, %{hello: "world"})

      assert {:ok, %{hello: "world"}, tag} = MessageQueue.fetch(q)
      assert is_reference(tag)
    end

    test "FIFO order: messages come out in publish order", %{q: q} do
      :ok = MessageQueue.publish(q, :first)
      :ok = MessageQueue.publish(q, :second)
      :ok = MessageQueue.publish(q, :third)

      assert {:ok, :first, _} = MessageQueue.fetch(q)
      assert {:ok, :second, _} = MessageQueue.fetch(q)
      assert {:ok, :third, _} = MessageQueue.fetch(q)
      assert MessageQueue.fetch(q) == :empty
    end

    test "delivery tags are unique across fetches", %{q: q} do
      :ok = MessageQueue.publish(q, :a)
      :ok = MessageQueue.publish(q, :b)

      {:ok, _, tag1} = MessageQueue.fetch(q)
      {:ok, _, tag2} = MessageQueue.fetch(q)

      refute tag1 == tag2
    end

    test "fetched messages are not visible to subsequent fetches", %{q: q} do
      :ok = MessageQueue.publish(q, :work)

      {:ok, :work, _tag} = MessageQueue.fetch(q)

      # Without ack/nack, the message is in-flight — not in the pending queue.
      assert MessageQueue.fetch(q) == :empty
    end
  end

  describe "ack" do
    test "ack removes the in-flight entry; the message is gone for good", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.ack(q, tag)

      assert MessageQueue.fetch(q) == :empty
    end

    test "ack-ing one delivery doesn't disturb another in-flight delivery", %{q: q} do
      :ok = MessageQueue.publish(q, :a)
      :ok = MessageQueue.publish(q, :b)

      {:ok, :a, tag_a} = MessageQueue.fetch(q)
      {:ok, :b, tag_b} = MessageQueue.fetch(q)

      :ok = MessageQueue.ack(q, tag_a)

      # tag_b is still in-flight; nack-ing it should still work.
      assert :ok = MessageQueue.nack(q, tag_b, requeue: true)
      assert {:ok, :b, _} = MessageQueue.fetch(q)
    end

    test "ack with an unknown tag returns {:error, :unknown_tag}", %{q: q} do
      # Strict policy: an unknown tag is a protocol error, surfaced to caller.
      assert {:error, :unknown_tag} = MessageQueue.ack(q, make_ref())
    end

    test "double-ack: second ack on the same tag returns {:error, :unknown_tag}", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.ack(q, tag)
      # First ack consumed the in-flight entry; second ack sees nothing.
      assert {:error, :unknown_tag} = MessageQueue.ack(q, tag)
    end
  end

  describe "nack" do
    test "nack with requeue: true puts the message back, with a new tag", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.nack(q, tag, requeue: true)

      assert {:ok, :work, new_tag} = MessageQueue.fetch(q)
      refute new_tag == tag
    end

    test "nack with requeue: false drops the message", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.nack(q, tag, requeue: false)

      assert MessageQueue.fetch(q) == :empty
    end

    test "nack defaults to requeue: true when no opts are given", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.nack(q, tag)

      # Default is requeue: true → message should be fetchable again.
      assert {:ok, :work, _} = MessageQueue.fetch(q)
    end

    test "nack with an unknown tag returns {:error, :unknown_tag}", %{q: q} do
      # Strict policy mirrors ack.
      assert {:error, :unknown_tag} = MessageQueue.nack(q, make_ref())
    end

    test "nack with requeue puts the message at the tail (not head)", %{q: q} do
      # You picked tail. This pins that choice so a future refactor to head
      # makes itself known via a failing test.
      :ok = MessageQueue.publish(q, :first)
      :ok = MessageQueue.publish(q, :second)

      {:ok, :first, tag} = MessageQueue.fetch(q)
      :ok = MessageQueue.nack(q, tag, requeue: true)

      # If requeued at the head: :first would come back next.
      # If requeued at the tail: :second comes first, then :first.
      assert {:ok, :second, _} = MessageQueue.fetch(q)
      assert {:ok, :first, _} = MessageQueue.fetch(q)
    end
  end

  describe "size" do
    test "reflects pending messages, not in-flight ones", %{q: q} do
      assert MessageQueue.Queue.size(q) == 0
      :ok = MessageQueue.publish(q, :a)
      :ok = MessageQueue.publish(q, :b)

      assert MessageQueue.Queue.size(q) == 2

      {:ok, :a, _tag} = MessageQueue.fetch(q)
      # :a is in-flight now, not pending
      assert MessageQueue.Queue.size(q) == 1
    end
  end

  describe "visibility timeout / redelivery" do
    # These tests start their own queue with a short visibility timeout
    # so the suite stays fast. Sleeps are 2–3× the timeout to absorb
    # scheduler jitter without flaking.
    setup do
      name = "vt_q_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name, visibility_timeout: 50)
      %{q: name}
    end

    test "an unacked message becomes fetchable again after the timeout", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, _tag} = MessageQueue.fetch(q)

      # Before the timeout fires, the queue looks empty.
      assert MessageQueue.fetch(q) == :empty

      Process.sleep(120)

      assert {:ok, :work, _new_tag} = MessageQueue.fetch(q)
    end

    test "the redelivered tag differs from the original", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag1} = MessageQueue.fetch(q)

      Process.sleep(120)

      {:ok, :work, tag2} = MessageQueue.fetch(q)
      refute tag1 == tag2
    end

    test "the original tag is invalidated after expiry", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      Process.sleep(120)

      # The message has been redelivered under a new tag; the old one
      # is no longer in-flight.
      assert {:error, :unknown_tag} = MessageQueue.ack(q, tag)
    end

    test "acking before expiry prevents redelivery", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.ack(q, tag)

      # Even after the timer fires, the message should not come back —
      # the in-flight entry is already gone, so handle_info no-ops.
      Process.sleep(120)
      assert MessageQueue.fetch(q) == :empty
    end

    test "nacking before expiry takes the requeue path, not the expiry path", %{q: q} do
      :ok = MessageQueue.publish(q, :work)
      {:ok, :work, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.nack(q, tag, requeue: true)

      # Message comes back immediately via nack — not 50ms later via expiry.
      assert {:ok, :work, _} = MessageQueue.fetch(q)

      # And the stale timer firing later is a no-op.
      Process.sleep(120)
    end
  end

  describe "dead-letter queue" do
    setup do
      name = "dlq_q_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name, max_attempts: 3)
      %{q: name}
    end

    test "fresh queue has an empty DLQ", %{q: q} do
      assert MessageQueue.dlq_messages(q) == []
    end

    test "nack(requeue: false) routes the message straight to the DLQ", %{q: q} do
      :ok = MessageQueue.publish(q, :poison)
      {:ok, :poison, tag} = MessageQueue.fetch(q)

      :ok = MessageQueue.nack(q, tag, requeue: false)

      assert MessageQueue.fetch(q) == :empty
      assert [%{payload: :poison}] = MessageQueue.dlq_messages(q)
    end

    test "after max_attempts nacks with requeue, message is in DLQ", %{q: q} do
      # max_attempts: 3 → 3 delivery attempts, then DLQ on the 3rd nack.
      :ok = MessageQueue.publish(q, :poison)

      for _ <- 1..3 do
        {:ok, :poison, tag} = MessageQueue.fetch(q)
        :ok = MessageQueue.nack(q, tag, requeue: true)
      end

      # 4th fetch finds nothing — the message has been DLQ'd.
      assert MessageQueue.fetch(q) == :empty
      assert [%{payload: :poison, attempt_count: 3}] = MessageQueue.dlq_messages(q)
    end

    test "after max_attempts expiries, message is in DLQ", _ctx do
      # Combine short visibility with low max_attempts on a fresh queue.
      name = "dlq_expire_" <> Integer.to_string(System.unique_integer([:positive]))

      {:ok, _pid} =
        MessageQueue.Queue.start_link(name, visibility_timeout: 30, max_attempts: 2)

      :ok = MessageQueue.publish(name, :ghost)

      # Fetch and ghost the consumer twice — let each expire.
      {:ok, :ghost, _} = MessageQueue.fetch(name)
      Process.sleep(80)

      {:ok, :ghost, _} = MessageQueue.fetch(name)
      Process.sleep(80)

      assert MessageQueue.fetch(name) == :empty
      assert [%{payload: :ghost, attempt_count: 2}] = MessageQueue.dlq_messages(name)
    end

    test "DLQ is read-only — inspecting it doesn't drain it", %{q: q} do
      :ok = MessageQueue.publish(q, :poison)
      {:ok, :poison, tag} = MessageQueue.fetch(q)
      :ok = MessageQueue.nack(q, tag, requeue: false)

      assert [%{payload: :poison}] = MessageQueue.dlq_messages(q)
      # Calling again returns the same thing — DLQ wasn't drained.
      assert [%{payload: :poison}] = MessageQueue.dlq_messages(q)
    end

    test "DLQ preserves FIFO order across multiple poisoned messages", %{q: q} do
      for msg <- [:first, :second, :third] do
        :ok = MessageQueue.publish(q, msg)
        {:ok, ^msg, tag} = MessageQueue.fetch(q)
        :ok = MessageQueue.nack(q, tag, requeue: false)
      end

      assert [
               %{payload: :first},
               %{payload: :second},
               %{payload: :third}
             ] = MessageQueue.dlq_messages(q)
    end
  end

  describe "configuration" do
    test "default max_attempts is 5 (verified by behaviour, not introspection)" do
      name = "cfg_q_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)

      :ok = MessageQueue.publish(name, :poison)

      # 4 nacks should keep the message in rotation; the 5th sends it to DLQ.
      for _ <- 1..4 do
        {:ok, :poison, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      # Still fetchable on attempt 5.
      {:ok, :poison, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.nack(name, tag, requeue: true)

      assert MessageQueue.fetch(name) == :empty
      assert [%{payload: :poison, attempt_count: 5}] = MessageQueue.dlq_messages(name)
    end

    test "default visibility_timeout is 30s (we don't wait — we just verify the timer was set)" do
      # Hard to assert "30 seconds" without making the suite slow. We at least
      # verify that the default does NOT cause sub-second redelivery: a fetched
      # message stays in-flight across a 200ms sleep.
      name = "cfg_vt_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)

      :ok = MessageQueue.publish(name, :work)
      {:ok, :work, _tag} = MessageQueue.fetch(name)

      Process.sleep(200)
      assert MessageQueue.fetch(name) == :empty
    end
  end

  describe "long polling — fetch without timeout (default behaviour)" do
    setup do
      name = "lp_default_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)
      %{q: name}
    end

    test "fetch without opts returns :empty immediately (no parking)", %{q: q} do
      # Default timeout is 0 — long-polling is opt-in.
      assert MessageQueue.fetch(q) == :empty
    end

    test "fetch with timeout: 0 returns :empty immediately even with a delayed publish", %{q: q} do
      # Schedule a publish 30ms from now.
      me = self()

      spawn(fn ->
        Process.sleep(30)
        :ok = MessageQueue.publish(q, :late)
        send(me, :published)
      end)

      # timeout: 0 should NOT wait for the publish.
      assert MessageQueue.fetch(q, timeout: 0) == :empty

      # Confirm the publish does eventually happen — message lands in pending.
      assert_receive :published, 200
      assert {:ok, :late, _} = MessageQueue.fetch(q)
    end
  end

  describe "long polling — parked fetch wakes on publish" do
    setup do
      name = "lp_wake_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)
      %{q: name}
    end

    test "parked fetch returns the message when a publish arrives", %{q: q} do
      # Spawn a consumer that parks for up to 500ms.
      task = Task.async(fn -> MessageQueue.fetch(q, timeout: 500) end)

      # Give the task a moment to actually park inside the GenServer.
      Process.sleep(20)

      :ok = MessageQueue.publish(q, :hello)

      assert {:ok, :hello, tag} = Task.await(task, 1_000)
      assert is_reference(tag)
    end

    test "waker's delivery becomes in-flight (visibility timer applies)", %{q: q} do
      # The message handed to a waiter still needs to be acked; it's not magically
      # consumed. Verify by NOT acking and checking it comes back after expiry.
      name = "lp_inflight_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name, visibility_timeout: 50)

      task = Task.async(fn -> MessageQueue.fetch(name, timeout: 500) end)
      Process.sleep(20)
      :ok = MessageQueue.publish(name, :work)

      assert {:ok, :work, _tag} = Task.await(task, 1_000)

      # Don't ack. Wait past visibility timeout. Message should be fetchable again.
      Process.sleep(120)
      assert {:ok, :work, _new_tag} = MessageQueue.fetch(name)

      # silence unused
      _ = q
    end

    test "publish skips pending entirely when a waiter is parked", %{q: q} do
      task = Task.async(fn -> MessageQueue.fetch(q, timeout: 500) end)
      Process.sleep(20)

      :ok = MessageQueue.publish(q, :direct)

      assert {:ok, :direct, _} = Task.await(task, 1_000)

      # pending should be empty — the message went to the waiter, not the queue.
      assert MessageQueue.Queue.size(q) == 0
    end
  end

  describe "long polling — parked fetch times out" do
    setup do
      name = "lp_timeout_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)
      %{q: name}
    end

    test "parked fetch returns :empty after the timeout elapses", %{q: q} do
      # No publisher ever arrives. fetch should return :empty after ~50ms.
      started = System.monotonic_time(:millisecond)
      assert MessageQueue.fetch(q, timeout: 50) == :empty
      elapsed = System.monotonic_time(:millisecond) - started

      # At least the requested timeout, with a generous upper bound for jitter.
      assert elapsed >= 50
      assert elapsed < 500
    end

    test "timing out frees the waiter slot for future fetches", %{q: q} do
      # First fetch times out.
      assert MessageQueue.fetch(q, timeout: 30) == :empty

      # Second fetch with a publish in-between should succeed normally.
      task = Task.async(fn -> MessageQueue.fetch(q, timeout: 200) end)
      Process.sleep(20)
      :ok = MessageQueue.publish(q, :back)

      assert {:ok, :back, _} = Task.await(task, 1_000)
    end

    test "publish before timeout wakes the waiter; the stale timer does nothing", %{q: q} do
      task = Task.async(fn -> MessageQueue.fetch(q, timeout: 200) end)
      Process.sleep(20)
      :ok = MessageQueue.publish(q, :early)

      assert {:ok, :early, _} = Task.await(task, 1_000)

      # Sleep past when the timer would have fired. Nothing should crash or
      # double-reply (would surface as a process exit / mailbox surprise).
      Process.sleep(250)

      # Queue should still be healthy and responsive.
      assert MessageQueue.fetch(q) == :empty
    end
  end

  describe "long polling — multiple parked waiters" do
    setup do
      name = "lp_multi_" <> Integer.to_string(System.unique_integer([:positive]))
      {:ok, _pid} = MessageQueue.Queue.start_link(name)
      %{q: name}
    end

    test "waiters are served in FIFO order (first parked = first served)", %{q: q} do
      # Park three consumers. Index them so we can verify ordering.
      t1 = Task.async(fn -> {1, MessageQueue.fetch(q, timeout: 500)} end)
      Process.sleep(10)
      t2 = Task.async(fn -> {2, MessageQueue.fetch(q, timeout: 500)} end)
      Process.sleep(10)
      t3 = Task.async(fn -> {3, MessageQueue.fetch(q, timeout: 500)} end)
      Process.sleep(10)

      # Publish three distinct messages. They should go to t1, t2, t3 in order.
      :ok = MessageQueue.publish(q, :a)
      :ok = MessageQueue.publish(q, :b)
      :ok = MessageQueue.publish(q, :c)

      assert {1, {:ok, :a, _}} = Task.await(t1, 1_000)
      assert {2, {:ok, :b, _}} = Task.await(t2, 1_000)
      assert {3, {:ok, :c, _}} = Task.await(t3, 1_000)
    end

    test "served waiters' stale timers don't affect later, still-parked waiters", %{q: q} do
      # A parks with a long timeout, B parks with a long timeout.
      # A is served by a publish. B remains parked. Verify B eventually times out.
      task_a = Task.async(fn -> MessageQueue.fetch(q, timeout: 500) end)
      Process.sleep(10)
      task_b = Task.async(fn -> MessageQueue.fetch(q, timeout: 80) end)
      Process.sleep(10)

      :ok = MessageQueue.publish(q, :for_a)

      assert {:ok, :for_a, _} = Task.await(task_a, 1_000)
      # B was never served; its timer should fire and reply :empty.
      assert :empty = Task.await(task_b, 1_000)
    end
  end

  describe "supervision — ensure_queue idempotency" do
    test "ensure_queue starts a new queue and returns :ok" do
      name = "sup_new_" <> Integer.to_string(System.unique_integer([:positive]))
      assert :ok = MessageQueue.ensure_queue(name)
      assert [{_pid, _}] = Registry.lookup(MessageQueue.Registry, name)
    end

    test "ensure_queue is idempotent: calling twice keeps the same process" do
      name = "sup_idem_" <> Integer.to_string(System.unique_integer([:positive]))

      :ok = MessageQueue.ensure_queue(name)
      [{pid1, _}] = Registry.lookup(MessageQueue.Registry, name)

      :ok = MessageQueue.ensure_queue(name)
      [{pid2, _}] = Registry.lookup(MessageQueue.Registry, name)

      # Same process — second call was a no-op.
      assert pid1 == pid2
    end

    test "ensure_queue accepts options on the first call" do
      name = "sup_opts_" <> Integer.to_string(System.unique_integer([:positive]))
      :ok = MessageQueue.ensure_queue(name, max_attempts: 2, visibility_timeout: 30)

      # Verify the options actually took effect: a poison message should hit DLQ
      # after 2 nacks (not the default 5).
      :ok = MessageQueue.publish(name, :poison)

      for _ <- 1..2 do
        {:ok, :poison, tag} = MessageQueue.fetch(name)
        :ok = MessageQueue.nack(name, tag, requeue: true)
      end

      assert MessageQueue.fetch(name) == :empty
      assert [%{payload: :poison}] = MessageQueue.dlq_messages(name)
    end
  end

  describe "supervision — crash recovery" do
    # Helper: wait for the Registry to hold a NEW pid (not the killed one)
    # under `name`. Avoids flaky fixed-sleep timing on slow machines.
    defp wait_for_restart(name, old_pid, attempts \\ 50)

    defp wait_for_restart(name, old_pid, 0),
      do: flunk("Queue #{name} did not restart in time (last known pid: #{inspect(old_pid)})")

    defp wait_for_restart(name, old_pid, attempts) do
      case Registry.lookup(MessageQueue.Registry, name) do
        [{new_pid, _}] when new_pid != old_pid ->
          new_pid

        _ ->
          Process.sleep(20)
          wait_for_restart(name, old_pid, attempts - 1)
      end
    end

    test "a crashed queue is restarted by the supervisor with a new PID" do
      name = "sup_crash_" <> Integer.to_string(System.unique_integer([:positive]))
      :ok = MessageQueue.ensure_queue(name)

      [{old_pid, _}] = Registry.lookup(MessageQueue.Registry, name)
      Process.exit(old_pid, :kill)

      new_pid = wait_for_restart(name, old_pid)
      assert Process.alive?(new_pid)
    end

    test "restarted queue starts with empty state (in-memory contract)" do
      name = "sup_state_" <> Integer.to_string(System.unique_integer([:positive]))
      :ok = MessageQueue.ensure_queue(name)

      :ok = MessageQueue.publish(name, :will_be_lost)
      assert MessageQueue.Queue.size(name) == 1

      [{pid, _}] = Registry.lookup(MessageQueue.Registry, name)
      Process.exit(pid, :kill)
      wait_for_restart(name, pid)

      # After restart, pending is empty — the message is gone with the old process.
      assert MessageQueue.Queue.size(name) == 0
      assert MessageQueue.fetch(name) == :empty
    end

    test "queue is functional immediately after restart" do
      name = "sup_func_" <> Integer.to_string(System.unique_integer([:positive]))
      :ok = MessageQueue.ensure_queue(name)

      [{pid, _}] = Registry.lookup(MessageQueue.Registry, name)
      Process.exit(pid, :kill)
      wait_for_restart(name, pid)

      # New process should accept publish/fetch normally.
      :ok = MessageQueue.publish(name, :post_crash)
      assert {:ok, :post_crash, _tag} = MessageQueue.fetch(name)
    end
  end

  describe "supervision — crash isolation" do
    test "crashing one queue does not affect another" do
      name_a = "sup_iso_a_" <> Integer.to_string(System.unique_integer([:positive]))
      name_b = "sup_iso_b_" <> Integer.to_string(System.unique_integer([:positive]))

      :ok = MessageQueue.ensure_queue(name_a)
      :ok = MessageQueue.ensure_queue(name_b)

      [{pid_a, _}] = Registry.lookup(MessageQueue.Registry, name_a)
      [{pid_b_before, _}] = Registry.lookup(MessageQueue.Registry, name_b)

      # Put something in B that should survive A's crash.
      :ok = MessageQueue.publish(name_b, :survives)

      Process.exit(pid_a, :kill)
      # Wait until A's restart finishes so the supervisor settles before we check B.
      wait_for_restart(name_a, pid_a)

      # B was not touched.
      [{pid_b_after, _}] = Registry.lookup(MessageQueue.Registry, name_b)
      assert pid_b_before == pid_b_after
      assert Process.alive?(pid_b_after)
      assert {:ok, :survives, _} = MessageQueue.fetch(name_b)
    end
  end
end
