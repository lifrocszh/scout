deke## Agent skills

### Issue tracker

Issues are tracked as local Markdown files under `.scratch/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the five default canonical labels. See `docs/agents/triage-labels.md`.

### Domain docs

Domain docs use the single-context layout. See `docs/agents/domain.md`.

### Run reports

Store run reports under `./docs/runs/`, not under `.scratch/`.

### Generation builds

Before building, recreating, or activating a persisted Generation,
read [`docs/agents/generation-builds.md`](docs/agents/generation-builds.md).

### Code organization principles
1. Follow the Single Responsibility Principle at the module level.
2. Prefer deep modules with simple, stable interfaces.
3. Keep unrelated responsibilities in separate modules.
4. When a source file exceeds 500 lines, evaluate whether it should be decomposed. Do not split solely to satisfy a line limit.
5. Avoid unnecessary abstractions, wrappers, and single-function modules.
6. Minimize coupling between modules.
7. Before implementing substantial features, identify the appropriate module boundaries.
8. Refactor existing modules when adding functionality would introduce unrelated responsibilities.
9. Preserve existing behavior and tests during refactoring.
