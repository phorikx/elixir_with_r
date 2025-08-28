defmodule PlotsWithPhoenixWeb.PageController do
  use PlotsWithPhoenixWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
