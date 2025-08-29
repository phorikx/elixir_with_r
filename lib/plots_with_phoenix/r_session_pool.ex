defmodule PlotsWithPhoenix.RSessionPool do
  use Supervisor

  @pool_name :r_session_pool
  @initial_size 5
  @max_overflow 10

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_opts) do
    pool_config = [
      name: {:local, @pool_name},
      worker_module: PlotsWithPhoenix.RSession,
      size: @initial_size,
      max_overflow: @max_overflow,
      strategy: :fifo
    ]

    children = [
      :poolboy.child_spec(@pool_name, pool_config, [])
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def eval(code, timeout \\ 30_000) do
    :poolboy.transaction(
      @pool_name,
      fn worker ->
        PlotsWithPhoenix.RSession.eval(worker, code, timeout)
      end,
      timeout + 1_000
    )
  catch
    :exit, {:timeout, _} -> {:error, :pool_timeout}
    :exit, reason -> {:error, {:pool_error, reason}}
  end
end
