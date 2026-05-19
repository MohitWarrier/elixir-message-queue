defmodule MessageQueue.LogTest do
  @moduledoc """
  Tests for the append-only WAL.

  These tests are intentionally low-level — they hit the file system directly
  rather than going through the Queue GenServer. The point is to verify the
  framing, the replay logic, and the torn-write recovery in isolation.
  """

  use ExUnit.Case, async: false
  # async: false because tests share the priv/logs/ directory. If you want
  # parallel tests, give each test a unique queue name and clean up after.

  alias MessageQueue.Log

  @test_queue "test_log_queue"

  setup do
    # TODO: ensure priv/logs exists and the test queue's log file is removed
    # before each test. Return :ok or {:ok, context}.
    :ok
  end

  describe "open/1 and close/1" do
    @tag :skip
    test "creates the log file if it does not exist" do
      # TODO
    end

    @tag :skip
    test "opens an existing log file without truncating it" do
      # TODO
    end
  end

  describe "append/2 + replay/3" do
    @tag :skip
    test "single entry round-trips" do
      # TODO: append one entry, close, replay, assert the fold sees that entry
    end

    @tag :skip
    test "many entries replay in insertion order" do
      # TODO: append 1000 entries, replay, assert order preserved
    end

    @tag :skip
    test "entries can hold arbitrary Elixir terms" do
      # TODO: tuples, maps, binaries, refs (note: refs do round-trip but are
      # not equal across node restarts — test for shape, not identity)
    end
  end

  describe "sync/1" do
    @tag :skip
    test "returns :ok on a healthy handle" do
      # TODO
    end
  end

  describe "torn-write recovery" do
    @tag :skip
    test "replay truncates a torn length prefix" do
      # TODO: write a few good entries + sync, then manually append 2 garbage
      # bytes (less than a full 4-byte length prefix), close, replay, assert
      # the fold saw exactly the good entries and the file size shrank to
      # exclude the 2 garbage bytes
    end

    @tag :skip
    test "replay truncates a torn payload" do
      # TODO: write a few good entries, then manually append <<999::32, "x">>
      # (claims 999 bytes but only 1 follows). Replay, assert good entries
      # survive and the file is truncated to before the bad length prefix.
    end

    @tag :skip
    test "replay handles a completely empty file" do
      # TODO: open + close without appending, replay, assert initial accumulator
      # is returned unchanged
    end

    @tag :skip
    test "replay handles a nonexistent file" do
      # TODO: replay a queue name with no log file on disk, assert :ok + initial
    end
  end
end
