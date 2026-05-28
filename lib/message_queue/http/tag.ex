defmodule MessageQueue.HTTP.Tag do
  # Delivery tags are Erlang refs (from make_ref/0).
  # Refs can't go in a URL as-is — they look like #Reference<0.123.456.789>.
  # This module turns a ref into a URL-safe string and back.

  @spec encode(reference()) :: String.t()
  def encode(ref) do
    :erlang.term_to_binary(ref) |> Base.url_encode64(padding: false)
  end

  # term_to_binary of a make_ref() produces ~25 bytes. 64 is a comfortable
  # ceiling. Without this, a client could base64-encode a 10 MB term and force
  # BEAM to allocate it before is_reference/1 rejects it — memory exhaustion.
  @max_decoded_bytes 64

  @spec decode(String.t()) :: {:ok, reference()} | :error
  def decode(string) do
    case Base.url_decode64(string, padding: false) do
      :error ->
        :error

      {:ok, binary} when byte_size(binary) > @max_decoded_bytes ->
        :error

      {:ok, binary} ->
        # binary_to_term raises exception, doesnt return value so use try rescue block
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            ref when is_reference(ref) -> {:ok, ref}
            _ -> :error
          end
        rescue
          ArgumentError -> :error
        end
    end
  end
end
