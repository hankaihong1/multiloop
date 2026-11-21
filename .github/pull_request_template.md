# Pull Request

## Description of changes

<!-- Describe what you changed and why. Include *what* and *why*, not just *how*. -->

## Type

- [ ] bugfix
- [ ] feature
- [ ] docs
- [ ] chore

> **Labels**: after opening the PR, add the matching label
> (`feature` / `fix` / `docs` / `chore` / `breaking`) — Release Drafter
> groups PRs by label and derives the next version number from them.

## Checklist

<!--
Before submitting, make sure `make all` (or the relevant docs check) passes.
See CONTRIBUTING.md -> "Pre-submission checklist" for details.
-->

- [ ] Tests pass (`make test` on free-threaded Python `>= 3.14t`)
- [ ] Lint & static checks pass (`make lint`: ruff check/format, mypy, pyright, clippy, rustfmt, rustdoc, version parity)
- [ ] CHANGELOG / CITATION updated (if cutting a release)
- [ ] EN/ZH docs synced (`README.md` ↔ `README_ZH.md`, `docs/*.md` ↔ `docs/*_ZH.md`, `examples/README.md` ↔ `examples/README_ZH.md`)

## Related issue

Closes #<issue>
