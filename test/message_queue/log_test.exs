defmodule MessageQueue.LogTest do
  @moduledoc """
  Tests for the append-only WAL.

  Low-level — hits the file system directly rather than going through the
  Queue GenServer. Point is to verify framing, replay, and torn-write
  recovery in isolation.
  """

  use ExUnit.Case, async: false
  # async: false — tests share the priv/logs/ directory.

  alias MessageQueue.Log

  @test_queue "test_log_queue"

  setup do
    File.mkdir_p!("priv/logs")
    {:ok, path} = Log.path_for(@test_queue)
    File.rm(path)
    on_exit(fn -> File.rm(path) end)
    %{path: path}
  end

  describe "open/1 and close/1" do
    test "creates the log file if it does not exist", %{path: path} do
      refute File.exists?(path)

      {:ok, handle} = Log.open(@test_queue)
      assert File.exists?(path)

      :ok = Log.close(handle)
    end

    test "opens an existing log file without truncating it", %{path: path} do
      {:ok, h1} = Log.open(@test_queue)
      :ok = Log.append(h1, {:test, "data"})
      :ok = Log.sync(h1)
      :ok = Log.close(h1)

      size_before = File.stat!(path).size
      assert size_before > 0

      {:ok, h2} = Log.open(@test_queue)
      size_after = File.stat!(path).size
      assert size_after == size_before

      :ok = Log.close(h2)
    end

    test "rejects invalid queue names" do
      assert {:error, :invalid_name} = Log.open("../etc/passwd")
      assert {:error, :invalid_name} = Log.open("name with spaces")
      assert {:error, :invalid_name} = Log.open("name/with/slashes")
    end
  end

  describe "append/2 + replay/3" do
    test "single entry round-trips" do
      {:ok, handle} = Log.open(@test_queue)
      :ok = Log.append(handle, {:publish, %{payload: "hello"}})
      :ok = Log.sync(handle)
      :ok = Log.close(handle)

      {:ok, entries} = Log.replay(@test_queue, &collect/2, [])
      assert Enum.reverse(entries) == [{:publish, %{payload: "hello"}}]
    end

    test "many entries replay in insertion order" do
      {:ok, handle} = Log.open(@test_queue)

      for i <- 1..1000 do
        :ok = Log.append(handle, {:publish, i})
      end

      :ok = Log.sync(handle)
      :ok = Log.close(handle)

      {:ok, entries} = Log.replay(@test_queue, &collect/2, [])
      replayed = Enum.reverse(entries)
      expected = Enum.map(1..1000, &{:publish, &1})

      assert replayed == expected
    end

    test "entries can hold arbitrary Elixir terms" do
      ref = make_ref()

      entries = [
        {:tuple, "with", :stuff},
        %{nested: %{map: [1, 2, 3]}, with: "binaries"},
        <<1, 2, 3, 255>>,
        {:ref, ref},
        :just_an_atom,
        12345.6789,
        [1, "two", :three, {:four, %{five: 6}}]
      ]

      {:ok, handle} = Log.open(@test_queue)

      for e <- entries do
        :ok = Log.append(handle, e)
      end

      :ok = Log.sync(handle)
      :ok = Log.close(handle)

      {:ok, result} = Log.replay(@test_queue, &collect/2, [])
      assert Enum.reverse(result) == entries
    end

    test "references compare equal after round-trip through the log" do
      ref = make_ref()

      {:ok, handle} = Log.open(@test_queue)
      :ok = Log.append(handle, {:fetch, ref, %{payload: "x"}})
      :ok = Log.append(handle, {:ack, ref})
      :ok = Log.sync(handle)
      :ok = Log.close(handle)

      {:ok, entries} = Log.replay(@test_queue, &collect/2, [])
      [ack_entry, fetch_entry] = entries

      {:fetch, decoded_ref_1, _envelope} = fetch_entry
      {:ack, decoded_ref_2} = ack_entry

      # The two refs from separate entries should compare equal —
      # this is what makes ack/fetch matching work in replay.
      assert decoded_ref_1 == decoded_ref_2
      assert decoded_ref_1 == ref
    end
  end

  describe "sync/1" do
    test "returns :ok on a healthy handle" do
      {:ok, handle} = Log.open(@test_queue)
      :ok = Log.append(handle, :sentinel)
      assert :ok = Log.sync(handle)
      :ok = Log.close(handle)
    end
  end

  describe "torn-write recovery" do
    test "replay stops cleanly on a torn length prefix", %{path: path} do
      {:ok, handle} = Log.open(@test_queue)
      :ok = Log.append(handle, {:good, 1})
      :ok = Log.append(handle, {:good, 2})
      :ok = Log.sync(handle)
      :ok = Log.close(handle)

      # Append 2 garbage bytes — less than a full 4-byte length prefix.
      {:ok, raw} = :file.open(path, [:append, :binary, :raw])
      :ok = :file.write(raw, <<0, 0>>)
      :ok = :file.close(raw)

      {:ok, entries} = Log.replay(@test_queue, &collect/2, [])
      assert Enum.reverse(entries) == [{:good, 1}, {:good, 2}]
    end

    test "replay handles a completely empty file" do
      {:ok, handle} = Log.open(@test_queue)
      :ok = Log.close(handle)

      assert {:ok, :sentinel} =
               Log.replay(@test_queue, fn _, _ -> :should_not_be_called end, :sentinel)
    end

    test "replay handles a nonexistent file" do
      # setup deletes the file before each test.
      assert {:ok, :sentinel} =
               Log.replay(@test_queue, fn _, _ -> :should_not_be_called end, :sentinel)
    end
  end

  # --- Helpers ---

  defp collect(entry, acc), do: [entry | acc]
end
