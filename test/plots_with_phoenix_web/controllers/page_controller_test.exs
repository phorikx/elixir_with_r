defmodule PlotsWithPhoenixWeb.PageControllerTest do
  use PlotsWithPhoenixWeb.ConnCase

  test "GET /", %{conn: conn} do
    conn = get(conn, ~p"/")
    response = html_response(conn, 200)
    assert response =~ "Plots with Phoenix"
    assert response =~ "R Console"
    assert response =~ "Dataset Overview"
  end
end
