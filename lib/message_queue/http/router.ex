defmodule MessageQueue.HTTP.Router do
  use Plug.Router

  # Max client-requested long-poll wait. Anything over this -> 400.
  @max_timeout_ms 60_000

  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason)
  plug(:match)
  plug(:dispatch)

  # Publish a message to a queue.
  # Body: JSON message payload.
  post "/queues/:name/messages" do
    MessageQueue.publish(name, conn.body_params)
    send_resp(conn, 201, "")
  end

  # Fetch one message (long-poll).
  # Query: ?timeout=ms (optional, default 30000, max @max_timeout_ms)
  @fetch_params ["timeout"]
  post "/queues/:name/fetch" do
    with [] <- Map.keys(conn.query_params) -- @fetch_params,
         {n, ""} when n > 0 and n <= @max_timeout_ms <-
           Integer.parse(conn.query_params["timeout"] || "30000") do
      case MessageQueue.fetch(name, timeout: n) do
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
    case MessageQueue.HTTP.Tag.decode(tag) do
      :error ->
        send_resp(conn, 400, "Bad Tag")

      {:ok, ref} ->
        MessageQueue.ack(name, ref)
        send_resp(conn, 204, "")
    end
  end

  # Nack a delivery tag.
  # Query: ?requeue=true|false
  @nack_params ["requeue"]
  post "/queues/:name/messages/:tag/nack" do
    with [] <- Map.keys(conn.query_params) -- @nack_params,
         {:ok, ref} <- MessageQueue.HTTP.Tag.decode(tag),
         r when r in ["true", "false"] <- conn.query_params["requeue"] do
      MessageQueue.nack(name, ref, requeue: r == "true")
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

      _ ->
        send_resp(conn, 400, "requeue must be 'true' or 'false'")
    end
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end
