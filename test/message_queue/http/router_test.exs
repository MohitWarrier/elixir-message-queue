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
end
