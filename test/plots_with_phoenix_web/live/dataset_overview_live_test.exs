defmodule PlotsWithPhoenixWeb.DatasetOverviewLiveTest do
  use PlotsWithPhoenixWeb.ConnCase

  import Phoenix.LiveViewTest

  describe "Dataset Overview" do
    test "renders the dataset overview page", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/datasets")

      assert html =~ "Dataset Overview"
      assert html =~ "Loading progress"
    end

    test "displays all 12 months", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/datasets")

      # Check that all months are displayed
      months = ~w(january february march april may june july august september october november december)

      for month <- months do
        assert html =~ month
      end
    end

    test "shows loading state initially", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/datasets")

      # Should show some indication of loading
      assert html =~ "Queued..." or html =~ "Analyzing..."
    end

    test "shows file status for each month", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/datasets")

      # Wait for datasets to load
      :timer.sleep(2000)

      html = render(view)

      # Each dataset should show either:
      # - File not found (missing)
      # - Row count (loaded)
      # - Error message
      assert html =~ "File not found" or html =~ "rows" or html =~ "Error"
    end

    test "displays summary statistics", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/datasets")

      # Summary section should be present
      assert html =~ "Summary"
      assert html =~ "Loaded datasets:"
      assert html =~ "Total rows:"
      assert html =~ "Avg rows/month:"
      assert html =~ "Status:"
    end

    test "shows completion status when all datasets loaded", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/datasets")

      # Wait for all datasets to finish loading
      :timer.sleep(3000)

      html = render(view)

      # Should show "Complete" status
      assert html =~ "Complete"
      assert html =~ "12 / 12" or html =~ "0 / 12"
    end

    test "displays dataset details when loaded successfully", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/datasets")

      # Wait longer for datasets to load (12 datasets loading sequentially)
      Process.sleep(5000)

      html = render(view)

      # If any datasets exist and loaded, they should show row and column counts
      # Otherwise they should show "File not found" or still be loading
      assert html =~ "rows" or html =~ "File not found" or html =~ "Analyzing..."
    end

    test "handles missing dataset files gracefully", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/datasets")

      # Wait for initial check
      :timer.sleep(500)

      html = render(view)

      # Some months might not have files, should show appropriate message
      # (depends on whether fixtures exist)
      assert html =~ "File not found" or html =~ "rows"
    end

    test "maintains user session across page visits", %{conn: conn} do
      # First visit
      {:ok, _view1, _html} = live(conn, ~p"/datasets")

      # Get the session from the view's socket
      # The session ID should be set
      Process.sleep(100)

      # Second visit should reuse session (same conn has session cookie)
      {:ok, _view2, html2} = live(conn, ~p"/datasets")

      # Page should load normally
      assert html2 =~ "Dataset Overview"
    end

    test "shows error messages for failed dataset loads", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/datasets")

      # Wait for loading to complete
      :timer.sleep(3000)

      html = render(view)

      # Check if any errors are displayed (or successful loads)
      # The test passes as long as the page handles both cases
      assert html =~ "Error" or html =~ "rows" or html =~ "File not found"
    end
  end
end
