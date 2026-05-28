defmodule MessageQueue.HTTP.Router do
  use Plug.Router

  # Max client-requested long-poll wait. Anything over this -> 400.
  @max_timeout_ms 60_000

  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason)
  plug(:match)
  plug(:dispatch)

  # Create (or confirm) a queue. Idempotent.
  # Body (optional): JSON object with creation options (currently only
  # "durable" → boolean). Other options can be added here as needed.
  put "/queues/:name" do
    durable = conn.body_params["durable"] == true

    case MessageQueue.ensure_queue(name, durable: durable) do
      :ok -> send_resp(conn, 204, "")
      {:error, _reason} -> send_resp(conn, 500, "queue creation failed")
    end
  end

  # Publish a message to a queue.
  # Body: JSON message payload.
  post "/queues/:name/messages" do
    case with_queue(name, fn -> MessageQueue.publish(name, conn.body_params) end) do
      :ok -> send_resp(conn, 201, "")
      :no_queue -> send_resp(conn, 404, "Queue Not Found")
    end
  end

  # Inspect the pending size of a queue.
  get "/queues/:name/size" do
    case with_queue(name, fn -> MessageQueue.Queue.size(name) end) do
      :no_queue ->
        send_resp(conn, 404, "Queue Not Found")

      size when is_integer(size) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{"size" => size}))
    end
  end

  # Inspect the dead-letter queue.
  get "/queues/:name/dlq" do
    case with_queue(name, fn -> MessageQueue.dlq_messages(name) end) do
      :no_queue ->
        send_resp(conn, 404, "Queue Not Found")

      list when is_list(list) ->
        body =
          Jason.encode!(%{
            "messages" =>
              Enum.map(list, fn env ->
                %{"payload" => env.payload, "attempt_count" => env.attempt_count}
              end)
          })

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)
    end
  end

  # Fetch one message (long-poll).
  # Query: ?timeout=ms (optional, default 30000, max @max_timeout_ms)
  @fetch_params ["timeout"]
  post "/queues/:name/fetch" do
    with [] <- Map.keys(conn.query_params) -- @fetch_params,
         {n, ""} when n > 0 and n <= @max_timeout_ms <-
           Integer.parse(conn.query_params["timeout"] || "30000") do
      case with_queue(name, fn -> MessageQueue.fetch(name, timeout: n) end) do
        :no_queue ->
          send_resp(conn, 404, "Queue Not Found")

        {:ok, msg, tag} ->
          body = Jason.encode!(%{"message" => msg, "tag" => MessageQueue.HTTP.Tag.encode(tag)})

          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, body)

        :empty ->
          send_resp(conn, 204, "")
      end
    else
      # non empty list. [_] is a list with exactly one element
      [_ | _] = unknown ->
        send_resp(
          conn,
          400,
          "Unknown Params: #{Enum.join(unknown, ", ")}, Valid Params are: #{Enum.join(@fetch_params, ", ")}"
        )

      _ ->
        send_resp(conn, 400, "Bad Timeout")
    end
  end

  # Ack a delivery tag.
  post "/queues/:name/messages/:tag/ack" do
    with {:ok, ref} <- MessageQueue.HTTP.Tag.decode(tag),
         :ok <- with_queue(name, fn -> MessageQueue.ack(name, ref) end) do
      send_resp(conn, 204, "")
    else
      :error -> send_resp(conn, 400, "Bad Tag")
      :no_queue -> send_resp(conn, 404, "Queue Not Found")
      {:error, :unknown_tag} -> send_resp(conn, 404, "Unknown Tag")
    end
  end

  # Nack a delivery tag.
  # Query: ?requeue=true|false
  @nack_params ["requeue"]
  post "/queues/:name/messages/:tag/nack" do
    with [] <- Map.keys(conn.query_params) -- @nack_params,
         {:ok, ref} <- MessageQueue.HTTP.Tag.decode(tag),
         r when r in ["true", "false"] <- conn.query_params["requeue"],
         :ok <- with_queue(name, fn -> MessageQueue.nack(name, ref, requeue: r == "true") end) do
      send_resp(conn, 204, "")
    else
      [_ | _] = unknown ->
        send_resp(
          conn,
          400,
          "Unknown Params: #{Enum.join(unknown, ", ")}, Valid Params are: #{Enum.join(@nack_params, ", ")}"
        )

      :error ->
        send_resp(conn, 400, "Bad Tag")

      :no_queue ->
        send_resp(conn, 404, "Queue Not Found")

      {:error, :unknown_tag} ->
        send_resp(conn, 404, "Unknown Tag")

      _ ->
        send_resp(conn, 400, "requeue must be 'true' or 'false'")
    end
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # Runs `fun` only if a queue process is registered under `name`. If not,
  # returns `:no_queue` so the caller can send 404 cleanly instead of letting
  # `GenServer.call` exit with `:noproc` (which would crash the Bandit handler
  # and surface as 500 to the client).
  defp with_queue(name, fun) do
    case Registry.lookup(MessageQueue.Registry, name) do
      [] -> :no_queue
      [_] -> fun.()
    end
  end
end
