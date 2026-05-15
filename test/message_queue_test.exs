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
end
