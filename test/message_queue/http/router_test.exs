defmodule MessageQueue.HTTP.RouterTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias MessageQueue.HTTP.Router
  alias MessageQueue.HTTP.Tag

  @opts Router.init([])

  setup do
    name = "test_queue_#{System.unique_integer([:positive])}"
    MessageQueue.ensure_queue(name)
    {:ok, queue: name}
  end

  defp call(conn), do: Router.call(conn, @opts)

  defp post_json(path, body) do
    conn(:post, path, Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
  end

  describe "POST /queues/:name/messages (publish)" do
    test "returns 201 and stores the message", %{queue: queue} do
      conn = post_json("/queues/#{queue}/messages", %{"task" => "hello"}) |> call()
      assert conn.status == 201

      assert {:ok, %{"task" => "hello"}, _tag} = MessageQueue.fetch(queue, timeout: 100)
    end
  end

  describe "POST /queues/:name/fetch" do
    test "returns 204 on empty queue", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch?timeout=100") |> call()
      assert conn.status == 204
    end

    test "returns 200 with message and tag when available", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "ping"})

      conn = conn(:post, "/queues/#{queue}/fetch?timeout=100") |> call()
      assert conn.status == 200

      body = Jason.decode!(conn.resp_body)
      assert body["message"] == %{"task" => "ping"}
      assert is_binary(body["tag"])
      assert {:ok, ref} = Tag.decode(body["tag"])
      assert is_reference(ref)
    end

    test "uses default timeout when ?timeout= not given", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch") |> call()
      # empty queue, default timeout still triggers 204
      assert conn.status == 204
    end

    test "rejects timeout=abc with 400 Bad Timeout", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch?timeout=abc") |> call()
      assert conn.status == 400
      assert conn.resp_body == "Bad Timeout"
    end

    test "rejects timeout over the cap with 400", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch?timeout=99999999") |> call()
      assert conn.status == 400
      assert conn.resp_body == "Bad Timeout"
    end

    test "rejects timeout <= 0 with 400", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch?timeout=0") |> call()
      assert conn.status == 400
      assert conn.resp_body == "Bad Timeout"
    end

    test "rejects unknown query params with 400", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/fetch?chicken=100") |> call()
      assert conn.status == 400
      assert conn.resp_body =~ "Unknown Params: chicken"
    end
  end

  describe "POST /queues/:name/messages/:tag/ack" do
    test "acks a fetched message and returns 204", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "ack-me"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/ack") |> call()
      assert conn.status == 204

      # after ack, the message should be gone — fetch returns :empty
      assert :empty = MessageQueue.fetch(queue, timeout: 100)
    end

    test "rejects bad tag with 400", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/messages/not-a-real-tag/ack") |> call()
      assert conn.status == 400
      assert conn.resp_body == "Bad Tag"
    end
  end

  describe "POST /queues/:name/messages/:tag/nack" do
    test "?requeue=true puts the message back on the queue", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "retry"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/nack?requeue=true") |> call()
      assert conn.status == 204

      assert {:ok, %{"task" => "retry"}, _new_ref} = MessageQueue.fetch(queue, timeout: 100)
    end

    test "?requeue=false sends the message to the DLQ", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "dropme"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/nack?requeue=false") |> call()
      assert conn.status == 204

      assert :empty = MessageQueue.fetch(queue, timeout: 100)
      assert [%{payload: %{"task" => "dropme"}} | _] = MessageQueue.dlq_messages(queue)
    end

    test "rejects missing requeue with 400", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "x"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/nack") |> call()
      assert conn.status == 400
      assert conn.resp_body =~ "requeue must be"
    end

    test "rejects requeue=yes with 400", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "x"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/nack?requeue=yes") |> call()
      assert conn.status == 400
      assert conn.resp_body =~ "requeue must be"
    end

    test "rejects bad tag with 400 Bad Tag", %{queue: queue} do
      conn = conn(:post, "/queues/#{queue}/messages/garbage/nack?requeue=true") |> call()
      assert conn.status == 400
      assert conn.resp_body == "Bad Tag"
    end

    test "rejects unknown query params with 400", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "x"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn = conn(:post, "/queues/#{queue}/messages/#{tag}/nack?requeue=true&foo=1") |> call()
      assert conn.status == 400
      assert conn.resp_body =~ "Unknown Params: foo"
    end
  end

  describe "unmatched routes" do
    test "POST to unknown path returns 404", _ do
      conn = conn(:post, "/totally/unknown/path") |> call()
      assert conn.status == 404
      assert conn.resp_body == "not found"
    end

    test "GET / returns 404", _ do
      conn = conn(:get, "/") |> call()
      assert conn.status == 404
    end
  end

  describe "operations on a nonexistent queue return 404 (not 500)" do
    test "publish returns 404", _ do
      conn = post_json("/queues/no_such_queue_#{u()}/messages", %{}) |> call()
      assert conn.status == 404
      assert conn.resp_body == "Queue Not Found"
    end

    test "fetch returns 404", _ do
      conn = conn(:post, "/queues/no_such_queue_#{u()}/fetch?timeout=100") |> call()
      assert conn.status == 404
    end

    test "ack returns 404", _ do
      fake_tag = Tag.encode(make_ref())
      conn = conn(:post, "/queues/no_such_queue_#{u()}/messages/#{fake_tag}/ack") |> call()
      assert conn.status == 404
    end

    test "nack returns 404", _ do
      fake_tag = Tag.encode(make_ref())
      path = "/queues/no_such_queue_#{u()}/messages/#{fake_tag}/nack?requeue=true"
      conn = conn(:post, path) |> call()
      assert conn.status == 404
    end

    test "size returns 404", _ do
      conn = conn(:get, "/queues/no_such_queue_#{u()}/size") |> call()
      assert conn.status == 404
    end

    test "dlq returns 404", _ do
      conn = conn(:get, "/queues/no_such_queue_#{u()}/dlq") |> call()
      assert conn.status == 404
    end
  end

  describe "ack/nack with an unknown tag (queue exists, tag is stale)" do
    test "ack returns 404, not 204", %{queue: queue} do
      # Use a freshly minted ref that was never handed out by this queue.
      bogus_tag = Tag.encode(make_ref())
      conn = conn(:post, "/queues/#{queue}/messages/#{bogus_tag}/ack") |> call()
      assert conn.status == 404
      assert conn.resp_body == "Unknown Tag"
    end

    test "double-ack: second ack on the same tag returns 404", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "x"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      tag = Tag.encode(ref)

      conn1 = conn(:post, "/queues/#{queue}/messages/#{tag}/ack") |> call()
      assert conn1.status == 204

      conn2 = conn(:post, "/queues/#{queue}/messages/#{tag}/ack") |> call()
      assert conn2.status == 404
    end

    test "nack with unknown tag returns 404, not 204", %{queue: queue} do
      bogus_tag = Tag.encode(make_ref())
      path = "/queues/#{queue}/messages/#{bogus_tag}/nack?requeue=true"
      conn = conn(:post, path) |> call()
      assert conn.status == 404
    end
  end

  describe "PUT /queues/:name (create)" do
    test "creates an in-memory queue when body has no durable flag", _ do
      name = "put_q_#{u()}"
      conn = post_put_json("/queues/#{name}", %{}) |> call()
      assert conn.status == 204

      # The queue is now usable.
      pub = post_json("/queues/#{name}/messages", %{"k" => "v"}) |> call()
      assert pub.status == 201
    end

    test "is idempotent — second PUT on the same name still returns 204", _ do
      name = "put_q_#{u()}"
      conn1 = post_put_json("/queues/#{name}", %{}) |> call()
      assert conn1.status == 204
      conn2 = post_put_json("/queues/#{name}", %{}) |> call()
      assert conn2.status == 204
    end

    test "creates a durable queue when body has durable: true", _ do
      name = "put_dur_q_#{u()}"
      conn = post_put_json("/queues/#{name}", %{"durable" => true}) |> call()
      assert conn.status == 204

      {:ok, path} = MessageQueue.Log.path_for(name)
      assert File.exists?(path)

      on_exit(fn -> File.rm(path) end)
    end
  end

  describe "GET /queues/:name/size" do
    test "returns 0 for a fresh queue", %{queue: queue} do
      conn = conn(:get, "/queues/#{queue}/size") |> call()
      assert conn.status == 200
      assert Jason.decode!(conn.resp_body) == %{"size" => 0}
    end

    test "reflects pending count", %{queue: queue} do
      MessageQueue.publish(queue, %{"k" => 1})
      MessageQueue.publish(queue, %{"k" => 2})
      conn = conn(:get, "/queues/#{queue}/size") |> call()
      assert conn.status == 200
      assert Jason.decode!(conn.resp_body) == %{"size" => 2}
    end
  end

  describe "GET /queues/:name/dlq" do
    test "returns empty messages list when DLQ is empty", %{queue: queue} do
      conn = conn(:get, "/queues/#{queue}/dlq") |> call()
      assert conn.status == 200
      assert Jason.decode!(conn.resp_body) == %{"messages" => []}
    end

    test "returns DLQ contents with payload and attempt_count", %{queue: queue} do
      MessageQueue.publish(queue, %{"task" => "poison"})
      {:ok, _msg, ref} = MessageQueue.fetch(queue, timeout: 100)
      :ok = MessageQueue.nack(queue, ref, requeue: false)

      conn = conn(:get, "/queues/#{queue}/dlq") |> call()
      assert conn.status == 200

      body = Jason.decode!(conn.resp_body)
      assert body == %{"messages" => [%{"payload" => %{"task" => "poison"}, "attempt_count" => 0}]}
    end
  end

  describe "Tag.decode size bound" do
    test "rejects a base64 payload larger than the size cap" do
      # 10 KB of random bytes, base64-encoded. Decoded size > 64 byte cap.
      big = :crypto.strong_rand_bytes(10_000) |> Base.url_encode64(padding: false)
      assert MessageQueue.HTTP.Tag.decode(big) == :error
    end

    test "accepts a legitimate ref tag" do
      ref = make_ref()
      encoded = MessageQueue.HTTP.Tag.encode(ref)
      assert {:ok, ^ref} = MessageQueue.HTTP.Tag.decode(encoded)
    end
  end

  defp u, do: System.unique_integer([:positive])

  defp post_put_json(path, body) do
    conn(:put, path, Jason.encode!(body))
    |> put_req_header("content-type", "application/json")
  end
end
