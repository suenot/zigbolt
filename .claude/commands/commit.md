# /commit — Smart Commit with Version Bump & Changelog

Create a git commit with automatic version bump, changelog generation, and version branch push.

## Steps

### 1. Analyze changes
- Run `git status` and `git diff --staged` (and `git diff` if nothing staged)
- Run `git log --oneline -5` to match commit message style
- Read `VERSION.toml` to get current version (create if doesn't exist)

### 2. Determine version bump type
Based on the nature of changes:
- **patch** (x.y.Z): bug fixes, small tweaks, dependency updates, test additions
- **minor** (x.Y.0): new features, new modules, enhancements, new config options
- **major** (X.0.0): breaking changes, architecture changes, API changes

If unclear, ask the user which bump type to use.

### 3. Stage files if needed
- If no files are staged, stage all modified/new files relevant to the changes
- Never stage `.env`, credentials, or secrets files
- Always include `VERSION.toml` and the new changelog file in the commit

### 4. Update VERSION.toml
- Increment the appropriate version field
- Reset lower fields (e.g., minor bump: patch → 0)
- Format:
  ```toml
  [version]
  major = 0
  minor = 2
  patch = 0
  ```
- Also update `version_major`, `version_minor`, `version_patch` in `src/root.zig` to match

### 5. Run tests before committing
- Run `zig build test` to ensure nothing is broken
- If tests fail, report the error and stop — do NOT commit broken code

### 6. Generate changelog
- Create `docs/changelog/vX.Y.Z.md` with:
  - Version and date header
  - Categorized changes (Bug Fixes, Features, Improvements, Breaking Changes, etc.)
  - Affected files/modules
  - Clear description of what changed and why
- Keep it concise but informative

### 7. Create the commit
- Stage `VERSION.toml`, `src/root.zig`, and `docs/changelog/vX.Y.Z.md` along with other changes
- Commit message format:
  ```
  v{version}: {short summary}

  {bullet list of key changes}

  Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
  ```
- Use HEREDOC for commit message formatting
- Do NOT amend existing commits

### 8. Create version branch and push
- Create a new branch named `v{major}.{minor}.{patch}` (e.g., `v0.2.0`)
- Push the version branch to ALL git remotes (get list via `git remote`)
- Push the main branch (master) to ALL git remotes
- Switch back to `master` branch after pushing

### 9. Show summary
- Print the new version number
- Print the commit hash
- Print the branch name that was pushed
- List all remotes it was pushed to

## Important
- Do NOT amend existing commits
- If pre-commit hook fails, fix the issue and create a NEW commit
- Always read VERSION.toml before modifying it
- Always run `zig build test` before committing
- Update version constants in both VERSION.toml and src/root.zig
