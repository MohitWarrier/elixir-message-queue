defmodule MessageQueue.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # The Registry that queue processes register themselves under.
      # `keys: :unique` because each queue name maps to exactly one process.
      {Registry, keys: :unique, name: MessageQueue.Registry},

      # DynamicSupervisor that owns queue processes. Starts them on demand
      # and restarts them automatically on crash.
      MessageQueue.QueueSupervisor,

      # HTTP server. Bandit runs the Plug router on port 4000.
      {Bandit, plug: MessageQueue.HTTP.Router, port: 4000}
    ]

    opts = [strategy: :one_for_one, name: MessageQueue.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
