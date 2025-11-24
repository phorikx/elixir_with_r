# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Setup
```bash
mix setup              # Install dependencies, setup database, and build assets
```

### Running the Application
```bash
mix phx.server         # Start Phoenix server (localhost:4000)
iex -S mix phx.server  # Start with interactive shell
```

### Testing
```bash
mix test                    # Run all tests
mix test test/path/file.exs # Run specific test file
mix test --failed           # Run previously failed tests
```

### Code Quality
```bash
mix precommit  # Run full pre-commit checks (compile with warnings-as-errors, deps.unlock --unused, format, test)
mix format     # Format code
```

### Database
```bash
mix ecto.create  # Create database
mix ecto.migrate # Run migrations
mix ecto.reset   # Drop, create, and migrate database
```

### Dataset Generation
```bash
mix generate_dataset --template unemployment_benefits --rows 1000000 --reporting_period 20240101 --output fixtures/benefits_january.parquet
mix generate_dataset --iterate --template unemployment_benefits --input fixtures/benefits_january.parquet --output fixtures/benefits_february.parquet
```

## Architecture Overview

### R Session Management

This application bridges Phoenix LiveView with R for statistical computing and data analysis:

- **`PlotsWithPhoenix.RSession`** - GenServer that manages individual R process via Port communication
  - Spawns `R --no-restore --no-save --quiet` as external process
  - Uses regex patterns to detect R prompt states (`> ` for ready, `+ ` for continuation)
  - Handles async evaluation with 30s timeout

- **`PlotsWithPhoenix.UserRSessions`** - GenServer that manages per-user R session pools
  - Started in application supervision tree
  - Creates isolated R sessions per user_id (session management via cookies)
  - Implements 2-hour session timeout with 1-hour cleanup intervals
  - Auto-restarts crashed sessions

### LiveView Architecture

- **`DatasetOverviewLive`** - Demonstrates async data loading pattern
  - Loads Parquet datasets from `fixtures/` directory (12 monthly files)
  - Uses `Task.async/1` for parallel R-based dataset inspection
  - Tracks loading state per dataset with task refs
  - Implements proper Task cleanup with `Process.demonitor/2`

- **`RConsoleLive`** - Interactive R console interface (assumption based on file existence)

### Data Generation System

- **`DataGenerator`** - Core synthetic data engine
  - Supports multiple generator types: `:normal`, `:uniform`, `:categorical`, `:sequence`, `:dependent`, `:custom`, `:constant`
  - Implements topological sort (Kahn's algorithm) for dependent variable resolution
  - Streams data in 10k row chunks for memory efficiency

- **`DatasetTemplate`** - Declarative dataset schema definition
  - Define variables with generators and dependencies
  - Track dependency graph for proper generation order

- **`DatasetIterator`** - Evolves existing datasets across time periods
  - Implements record retention (70%), dropout (5%), and new records (10%)
  - Column-specific resampling with configurable rates
  - Column transformations (e.g., aging, date progression)

- **`IterationTemplate`** - Configuration for dataset evolution patterns

- **`ParquetExporter`** - Uses Explorer library to export DataFrames to Parquet format
  - Supports both streaming and batch export modes

### Dependencies of Note

- **Explorer** (~> 0.11.1) - DataFrame library for Parquet I/O
- **Poolboy** (~> 1.5) - Generic pooling library (likely for R sessions)
- **Req** (~> 0.5) - Modern HTTP client (preferred over HTTPoison/Tesla)

## Project-Specific Guidelines

### R Integration Patterns

When calling R from Elixir:
- Always wrap R code in `tryCatch` with error handlers
- Use `cat(paste(...))` to return structured output that can be parsed
- Escape file paths: `String.replace(file_path, "\\", "/")`
- Call via `PlotsWithPhoenix.UserRSessions.eval_for_user(user_session_id, r_code)` to ensure proper session isolation
- Default timeout is 35 seconds for eval operations
- **Important:** Use semicolons to write R code as single-line expressions to avoid continuation prompts (`+`)
- The arrow library is pre-loaded when R sessions start, so no need to load it in individual commands
- **Output parsing caveat:** The R session detects completion by matching `> ` at the end of output. Be cautious with R commands that might output strings containing `> ` as this could cause premature prompt detection

### Async Task Pattern in LiveViews

When loading data asynchronously (see `DatasetOverviewLive`):
1. Start tasks via `Task.async/1` and immediately send `{:dataset_task_started, id, task.ref}` to self
2. Store `task_ref` in assigns to correlate results
3. Handle `{ref, result}` for success and `{:DOWN, ref, ...}` for failures
4. Always call `Process.demonitor(ref, [:flush])` after handling task result
5. Track loading count separately from task refs

### Dataset Generation

- Templates define variables with generator types and dependencies
- Use `:dependent` generators for computed columns that rely on other columns in the same row
- The system automatically resolves generation order via topological sort
- For iterative datasets, configure retention/dropout/resampling rates in `IterationTemplate`

### Session Management

- User sessions identified via `user_session_id` in Phoenix session (cookie-based)
- Generate session IDs: `:crypto.strong_rand_bytes(16) |> Base.encode64()`
- Each user gets isolated R process to avoid cross-contamination
- Sessions auto-expire after 2 hours of inactivity
