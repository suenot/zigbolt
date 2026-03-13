# Project Documentation

## Architecture
## Workflow

*   Before starting complex tasks, check `.claude/plans/` for existing plans and save new plans there
*   Periodically use `sidecar_tasks` MCP tool to check pending tasks (do NOT use built-in TaskList — it is unrelated)
*   Sidecar MCP tools (`sidecar_tasks`, `sidecar_scan`, `sidecar_map`, `docs_search`, `docs_get`, `docs_list`, `docs_reindex`, `mcp_list_servers`) are called directly — they are NOT deferred tools, do NOT search for them via ToolSearch
*   After major changes, use sidecar tools: `sidecar_scan` to review docs, `sidecar_map` to update project map
*   Read `.claude/rules/` for project-specific coding guidelines before making changes
*   Keep CLAUDE.md under 200 lines — move detailed rules to `.claude/rules/*.md`
*   When working with external APIs, databases, browsers, or new tools — check if a relevant MCP plugin exists: use `mcp_list_servers` to see what's configured, or `sidecar_tasks` to browse plugins via `make -C .claude dashboard` (Plugin Browser tab)
*   Use `docs_search` to find relevant guides in: cmdop-claude bundled docs. Call `docs_get` with the returned path to read the full file.

## Key Rules
- This file was auto-generated from project scan — review and refine
