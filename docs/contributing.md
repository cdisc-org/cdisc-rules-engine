# Contributing

Thank you for your interest in contributing to CORE! There are two main ways to contribute: **rule contributions** (via `cdisc-open-rules`) and **engine contributions** (code, tests, documentation in this repository).

---

## Rule Contributions

Conformance rules are maintained separately in [`cdisc-open-rules`](https://github.com/cdisc-org/cdisc-open-rules). If you want to:

- Propose a new conformance rule
- Report an issue with an existing rule's logic
- Contribute a rule implementation

Please open an issue or pull request in that repository. Rule authoring can also be done through the hosted [CORE Rule Editor](https://cdisc-org.github.io/conformance-rules-editor).

For questions about rule contribution workflows, post in [GitHub Discussions](https://github.com/cdisc-org/cdisc-rules-engine/discussions).

---

## Engine Contributions

### Setting Up the Development Environment

Follow the [Development → Environment Setup](development.md#environment-setup) guide to clone the repository and install dependencies.

### Code Style

This project enforces consistent formatting and linting via pre-commit hooks.

**Tools used:**

- [`black`](https://black.readthedocs.io/) — Python code formatter
- [`flake8`](https://flake8.pycqa.org/) — Python linter
- [`prettier`](https://prettier.io/) — JSON, YAML, and Markdown formatter

Both `black` and `flake8` are included in `requirements-dev.txt`. After installing dependencies, install the pre-commit hooks:

```bash
pre-commit install
```

This installs the hooks into `.git/hooks/` so formatting and linting run automatically on each commit.

To run the checks manually:

```bash
pre-commit run --all-files
```

### Running Tests

```bash
python -m pytest tests
```

This runs both unit and regression tests. All tests must pass before submitting a pull request.

### Submitting a Pull Request

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, following the code style guidelines above.
3. Add or update tests for any changed behavior.
4. Ensure all tests pass locally.
5. Open a pull request with a clear description of the change and the motivation behind it.

For larger changes or new features, consider opening a GitHub Discussion or issue first to align on the approach.

---

## Reporting Bugs & Requesting Features

Use [GitHub Issues](https://github.com/cdisc-org/cdisc-rules-engine/issues) to report bugs or request features.

When reporting a bug, please include:

- A clear description of the problem
- Steps to reproduce
- Your operating system and Python version (or executable version)
- Relevant logs or error messages

---

## Questions & Discussion

For general questions, use the [Q&A discussion board](https://github.com/cdisc-org/cdisc-rules-engine/discussions/categories/q-a). Please search existing discussions before opening a new one.
