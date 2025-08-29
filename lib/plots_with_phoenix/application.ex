defmodule PlotsWithPhoenix.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PlotsWithPhoenixWeb.Telemetry,
      PlotsWithPhoenix.Repo,
      {DNSCluster,
       query: Application.get_env(:plots_with_phoenix, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: PlotsWithPhoenix.PubSub},
      # Start a worker by calling: PlotsWithPhoenix.Worker.start_link(arg)
      # {PlotsWithPhoenix.Worker, arg},
      # Start to serve requests, typically the last entry
      PlotsWithPhoenixWeb.Endpoint,
      PlotsWithPhoenix.RSessionPool
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PlotsWithPhoenix.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PlotsWithPhoenixWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
