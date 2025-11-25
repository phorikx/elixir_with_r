defmodule PlotsWithPhoenixWeb.RConsoleLiveTest do
  use PlotsWithPhoenixWeb.ConnCase

  import Phoenix.LiveViewTest

  # Helper to wait for async updates in LiveView
  defp wait_for_result(view, timeout \\ 5000) do
    wait_until(
      fn ->
        html = render(view)
        html =~ "Output:" and not (html =~ "Executing...")
      end,
      timeout
    )
  end

  defp wait_until(fun, timeout) do
    end_time = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, end_time)
  end

  defp do_wait_until(fun, end_time) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < end_time do
        Process.sleep(50)
        do_wait_until(fun, end_time)
      else
        raise "Timeout waiting for condition"
      end
    end
  end

  describe "R Console" do
    test "renders the console interface", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/r_console")

      assert html =~ "R Console"
      assert has_element?(view, "form")
      assert has_element?(view, "button[type=\"submit\"]")
    end

    test "executes simple R code and displays result", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/r_console")

      # Submit a simple R expression
      view
      |> form("form", code: "1 + 1")
      |> render_submit()

      # Wait for async task to complete
      wait_for_result(view)

      # The result should appear on the page
      html = render(view)
      assert html =~ "[1] 2"
      assert html =~ "Input:"
      assert html =~ "Output:"
    end

    test "executes R code with output and displays it", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/r_console")

      # Submit code that produces output
      view
      |> form("form", code: "print('Hello from R')")
      |> render_submit()

      # Wait for async task
      wait_for_result(view)

      html = render(view)
      assert html =~ "Hello from R"
    end

    test "handles multiple executions in sequence", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/r_console")

      # First execution
      view
      |> form("form", code: "x <- 5")
      |> render_submit()

      wait_for_result(view)

      # Second execution using the variable from first
      view
      |> form("form", code: "x * 2")
      |> render_submit()

      wait_for_result(view)

      html = render(view)
      assert html =~ "[1] 10"
    end

    test "shows loading state during execution", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/r_console")

      # Submit code
      view
      |> form("form", code: "42")
      |> render_submit()

      # Check that loading indicator appears immediately
      html = render(view)
      assert html =~ "Executing..."
    end

    test "does not execute empty code", %{conn: conn} do
      {:ok, view, html_before} = live(conn, ~p"/r_console")

      # Count results before
      results_before = html_before |> String.split("Output:") |> length()

      # Submit empty code
      view
      |> form("form", code: "   ")
      |> render_submit()

      # Give it a moment
      Process.sleep(100)

      # Results should not have increased
      html_after = render(view)
      results_after = html_after |> String.split("Output:") |> length()

      assert results_before == results_after
    end

    test "displays timestamp for each result", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/r_console")

      view
      |> form("form", code: "1 + 1")
      |> render_submit()

      wait_for_result(view)

      html = render(view)
      # Timestamp should be in HH:MM:SS format
      assert html =~ ~r/\d{2}:\d{2}:\d{2}/
    end
  end
end
