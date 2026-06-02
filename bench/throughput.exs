defmodule Bench.Throughput do

   @payload :binary.copy("x", 100)

    def run(name, n, opts) do
      MessageQueue.ensure_queue(name, opts)

      # warmup loop
      {_, :ok} = :timer.tc(fn -> loop(name, 10_000) end)
      {microseconds, :ok} = :timer.tc(fn -> loop(name, n) end)
      ops_per_sec = n * 3 * 1_000_000 / microseconds
      IO.inspect(%{scenario: name, cycles: n, microseconds: microseconds, ops_per_sec: ops_per_sec})
    end

    defp loop(_name, 0), do: :ok

    defp loop(name, n) do
      :ok = MessageQueue.publish(name, @payload)
      # TODO: fetch — pattern match the returned {:ok, _msg, tag}
      {:ok, _msg, tag} = MessageQueue.fetch(name)
      :ok = MessageQueue.ack(name, tag)
      loop(name, n - 1)
    end
  end

Bench.Throughput.run("bench_in_memory", 100_000, durable: false)
Bench.Throughput.run("bench_on_disk", 100_000, durable: true)
