# ADR-001: Python Runtime, Packaging, and Development Tooling

## Status

Accepted

## Date

2026-07-16

## Context

`boolean-maybe` needs a reproducible, reviewer-friendly Python project baseline before feature implementation begins. The baseline must support an installable CLI, isolate application source from repository tooling, provide deterministic dependency resolution, and establish common quality checks without selecting feature-level infrastructure.

The project is a small assessment application, not a general-purpose framework. Its tooling should therefore be conventional, cross-platform, and limited to tools with clear responsibilities.

Python 3.12 and 3.13 are both current runtime candidates. Python 3.12 is the more conservative initial target for reviewer environments and dependency compatibility while still providing a modern language baseline.

## Decision

### Runtime and supported platforms

* The application targets CPython 3.12.
* Project metadata must declare `requires-python = ">=3.12,<3.13"` until support for another Python minor version is explicitly verified and approved.
* `uv` may use a compatible system CPython 3.12 interpreter or acquire a managed CPython 3.12 interpreter when one is not available. Reviewers are not required to manage a separate project-specific Python installation tool.
* The project must remain runnable on current Windows, macOS, and Linux environments supported by CPython 3.12 and `uv`.
* Application behavior and paths must not rely on one operating system unless a later ADR explicitly approves that constraint.

### Project and dependency management

* `uv` is the project, Python environment, dependency, lockfile, and command runner.
* `pyproject.toml` is the canonical manifest for project metadata, build configuration, dependencies, entry points, and development-tool configuration.
* `uv.lock` must be committed and used for reproducible development and verification environments.
* The documented local verification workflow invokes tools through `uv run --locked` or an equivalent locked `uv` operation. It must fail when the lockfile is inconsistent with `pyproject.toml` and must not update dependencies implicitly. CI hosting and pre-commit automation remain outside this decision.
* Runtime dependencies and development dependencies must be declared separately. Development tools must not become runtime dependencies.

### Packaging and source layout

* The project is an installable Python package built with Hatchling.
* The build system is declared through `pyproject.toml` with `hatchling.build` as the build backend.
* Application source uses a `src/` layout under the import package `src/boolean_maybe/`.
* Tests live outside the installed package under `tests/`.
* The installed console entry point is declared in `[project.scripts]` as `boolean-maybe = "boolean_maybe.cli:main"`.
* The entry point is a thin CLI adapter. Exact commands, arguments, inputs, outputs, and exit-code behavior remain feature-specification concerns.

### Development tools

* Ruff is the formatter and linter. It replaces separate formatting and import-sorting tools unless an identified requirement cannot be met by Ruff.
* Pyright is the static type checker. The Python wrapper is declared as the `pyright[nodejs]` development dependency, invoked through `uv run --locked pyright`, configured in `pyproject.toml`, and targeted at Python 3.12.
* Pyright's implementation requires Node.js. The selected `nodejs` extra must place a compatible Node.js distribution in the `uv`-resolved development dependency graph. The standard verification workflow must not depend on a globally installed Node.js or permit the wrapper's default `nodeenv` fallback to download Node.js dynamically on first execution.
* pytest is the test runner. Its project configuration lives in `pyproject.toml`.
* Source code and tests are subject to formatting, linting, type checking, and automated tests. The exact rule selection and strictness may be introduced incrementally, but checks must be deterministic and repository-owned.

### Dependency policy

* This policy applies to runtime, build, and development dependencies, including binaries or language runtimes acquired transitively by development tools.
* Add a dependency only when the standard library, existing approved dependencies, or current tools cannot meet a concrete requirement clearly and maintainably.
* Every dependency must serve an approved feature, an accepted architecture decision, or a repository quality gate defined by this ADR.
* Prefer dependencies that support CPython 3.12 and all supported operating systems without requiring a separately operated service or a dynamically downloaded runtime during normal execution or verification.
* Declare direct dependency compatibility constraints in `pyproject.toml`; record exact resolved versions in `uv.lock`.
* Build and development dependencies must remain proportionate to the project scope and must not introduce runtime requirements for the installed application.
* Dependency upgrades are explicit maintenance changes and must update and verify the lockfile.
* Git, local-path, platform-specific, and pre-release dependencies require explicit justification and review.
* Transitive packages must not be imported or treated as direct contracts unless promoted to direct dependencies.

## Consequences

Benefits:

* Reviewers get one documented setup and command-running workflow across supported operating systems.
* A committed universal lockfile makes dependency resolution reproducible without turning exact versions into duplicated documentation.
* The `src/` layout tests the installed package boundary rather than accidentally importing from the repository root.
* A declared build backend makes the CLI package installable and keeps packaging compatible with standard Python build frontends.
* Ruff, Pyright, and pytest provide distinct formatting/linting, type-checking, and testing gates with configuration in one manifest.

Trade-offs:

* Restricting the supported runtime to Python 3.12 postpones adoption of Python 3.13 language features.
* `uv` becomes required contributor and reviewer tooling even though built distributions remain standard Python artifacts.
* Hatchling adds a build dependency to an application that could otherwise be executed as loose scripts.
* Pyright introduces a tooling ecosystem distinct from the Python runtime; it must remain a development-only concern.
* Pyright is implemented in TypeScript and requires a platform-specific Node.js distribution. Selecting `pyright[nodejs]` makes that distribution part of the resolved development environment instead of relying on the wrapper's runtime `nodeenv` download, but it increases lockfile size and cross-platform tooling complexity.
* Initial `uv` setup still requires access to the configured package sources to obtain Python and locked dependency artifacts. Fully air-gapped bootstrap is not guaranteed; after a locked environment is provisioned, routine checks must not require a separately operated service or an additional first-run runtime download.
* Supporting Windows, macOS, and Linux constrains later dependencies and filesystem assumptions.

## Impact on Core Entities

None.

This decision does not add or change core entities, fields, identities, invariants, lifecycle states, ownership, relationships, compatibility rules, or data security boundaries. No update to `docs/domain/core-entities.md` is required.

## Alternatives Considered

1. **CPython 3.13 as the initial runtime.**
   Not selected because the assessment benefits more from conservative reviewer compatibility than from the newest language features. Support can be broadened after explicit cross-platform verification.

2. **An open-ended `>=3.12` runtime range.**
   Not selected because declaring unverified future minor versions would overstate compatibility. The range can be widened deliberately.

3. **`venv` and `pip` without a project lockfile.**
   Not selected because this would require more setup conventions and would provide weaker dependency reproducibility.

4. **Poetry or PDM for project management.**
   Not selected because `uv` covers the required interpreter, environment, dependency, lock, build-front-end, and command-running workflow with less project-specific machinery.

5. **Setuptools or `uv_build` as the build backend.**
   Not selected for the initial baseline. Hatchling provides a focused, standards-compatible packaging backend and leaves room for layout configuration without coupling the build backend to the selected project manager.

6. **A flat source layout or uninstalled scripts.**
   Not selected because the product requires an installed console entry point and benefits from testing the same import boundary used by reviewers.

7. **mypy as the static type checker.**
   Not selected for the initial baseline. mypy avoids the additional Node.js runtime and offers a simpler pure-Python development dependency graph. Pyright was preferred for its performance, type inference, standards-based checking, and editor integration. The `pyright[nodejs]` requirement and locked invocation explicitly accept and constrain the resulting cross-platform tooling cost. This choice does not affect installed application behavior.

8. **Separate formatter, linter, and import sorter tools.**
   Not selected because Ruff can cover the initial formatting and linting responsibilities with fewer tools and configurations.

## Scope

This decision applies to:

* the supported Python runtime and operating-system portability target;
* project metadata, packaging, source layout, and installed entry point;
* dependency declaration, locking, and upgrade policy;
* baseline formatting, linting, static type checking, and test tooling.

This decision does not select or define:

* an HTTP client or simulator web framework;
* persistence technology, schema, migrations, or consistency mechanism;
* retry, timeout, rate-limit, or recovery semantics;
* concrete CLI commands or response schemas;
* CI hosting or deployment infrastructure;
* feature-specific dependencies.

## Follow-up on Acceptance

If this ADR is accepted:

* update the Technology Constraints table in `docs/architecture/architecture-overview.md` to record the selected Python version, `uv`, Hatchling, `src/` layout, console entry point, and development quality tools;
* replace `No ADRs have been accepted yet` in the overview's Related Architecture Decisions section with a reference to this ADR;
* verify the locked Pyright workflow on the supported operating-system targets and confirm that it does not use a global Node.js installation or perform a first-run `nodeenv` download.

## Related Documents

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`

External technical references:

* [uv project configuration](https://docs.astral.sh/uv/concepts/projects/config/)
* [uv locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/)
* [Hatch build configuration](https://hatch.pypa.io/latest/config/build/)
* [Ruff configuration](https://docs.astral.sh/ruff/configuration/)
* [Pyright configuration](https://github.com/microsoft/pyright/blob/main/docs/configuration.md)
* [pytest configuration](https://docs.pytest.org/en/stable/reference/customize.html)

Supersedes: none

Superseded by: none
