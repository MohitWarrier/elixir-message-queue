defmodule MessageQueue.HTTP.Tag do
  # Delivery tags are Erlang refs (from make_ref/0).
  # Refs can't go in a URL as-is — they look like #Reference<0.123.456.789>.
  # This module turns a ref into a URL-safe string and back.

  @spec encode(reference()) :: String.t()
  def encode(ref) do
    :erlang.term_to_binary(ref) |> Base.url_encode64(padding: false)
  end

  @spec decode(String.t()) :: {:ok, reference()} | :error
  def decode(string) do
    case Base.url_decode64(string, padding: false) do
      :error ->
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
