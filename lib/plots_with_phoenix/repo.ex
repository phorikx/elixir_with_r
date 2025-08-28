defmodule PlotsWithPhoenix.Repo do
  use Ecto.Repo,
    otp_app: :plots_with_phoenix,
    adapter: Ecto.Adapters.Postgres
end
