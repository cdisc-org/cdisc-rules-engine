# FAQ & Troubleshooting

> Still stuck? Post in the
> [Q&A discussion board](https://github.com/cdisc-org/cdisc-rules-engine/discussions/categories/q-a)
> or [open an issue](https://github.com/cdisc-org/cdisc-rules-engine/issues).

---

## Installation & Setup

### Which Python version does CORE require?

**Python 3.12 is required.** Other versions are not supported and may cause unexpected
errors or incorrect validation results.

```bash
python --version
```

Download Python 3.12 from [python.org](https://www.python.org/downloads/) if needed.

### The Mac executable won't open due to a security warning

Remove the quarantine attribute:

```bash
xattr -rd com.apple.quarantine .
```

### I get `[SSL: CERTIFICATE_VERIFY_FAILED]` when running `update-cache`

This is typically caused by a corporate firewall performing SSL inspection. CORE connects
to `api.library.cdisc.org` on port 443. Contact your IT department to either obtain the
corporate CA certificate bundle or request whitelisting for that hostname.

---

## Cache & API Key

### Do I need an API key?

An API key is required for controlled terminology and library metadata. **Rules are
accessible without a key.** Running `update-cache` without a key will still populate
conformance rules.

To obtain a key:
[wiki.cdisc.org — Getting Started](https://wiki.cdisc.org/display/LIBSUPRT/Getting+Started%3A+Access+to+CDISC+Library+API+using+API+Key+Authentication)

> Note: It can take up to an hour after sign-up for a key to be issued.

### Where do I put my API key?

Set it as the `CDISC_LIBRARY_API_KEY` environment variable, or add it (without quotes)
to a `.env` file in the project root directory:

```text
CDISC_LIBRARY_API_KEY=your_key_here
```

### My validation returned no results or unexpected rules

- **Console output / logs:** By default, engine logs are disabled. Use `-l` /
  `--log-level` to enable them. Available levels: `info`, `debug`, `warn`, `error`,
  `critical`.
- **The output report:** Open the results file and review the **Rule Report** tab (XLSX)
  or the top-level `Rules_Report` array (JSON). Rules with a status of `SKIPPED` will
  include a reason in the Issue Details — this is often the cause of unexpectedly absent
  results.
- **Scope flags:** Confirm that your `-s`, `-v`, and for TIG `-ss` arguments match the
  standard, version, and substandard you intended to validate against. A mismatch will
  cause rules to be silently out of scope.

If you're still not seeing expected results after checking the above, post in the
[Q&A discussion board](https://github.com/cdisc-org/cdisc-rules-engine/discussions/categories/q-a)
and include the relevant log output and the rule IDs you expected to run.

---

## Validation

### What does each rule run status mean?

| Status            | Meaning                                                                           |
| ----------------- | --------------------------------------------------------------------------------- |
| `SUCCESS`         | Rule ran; no issues found.                                                        |
| `SKIPPED`         | Rule did not run (column/domain not found, schema validation off, outside scope). |
| `ISSUE REPORTED`  | Rule ran; issues were found.                                                      |
| `EXECUTION ERROR` | Rule failed unexpectedly. Check the Issue Details tab for details.                |

### My dataset fails to load / wrong encoding

CORE defaults to `utf-8`. If your files use a different encoding, specify it:

```bash
python core.py validate -s sdtmig -v 3-4 -dp path/to/dataset.xpt -e cp1252
```

> NOTE: you may notice a `'utf-9' codec can't decode byte` error in the logs. This is
> usually due to Windows Smart Quotes, produced in Excel, which are CP1252 encoded, not
> utf-8. Unfortunately, Windows Smart Quotes produce a file that is mostly utf-8 with
> some CP1252 for the smart quotes so the `-e` command will not work to resolve this.
> You will need to locate these quotes and manually change them before being able to
> rerun this data.

### Will using -d pointed at my data directory cause CORE to include my Define-XML file in the validation?

No. Define-XML must be provided separately via `--define-xml-path` (`-dxp`).

### Validation is very slow on large files

Set `DATASET_SIZE_THRESHOLD=0` to force Dask processing for all datasets regardless
of size:

```bash
# Linux/Mac
DATASET_SIZE_THRESHOLD=0 ./core validate -s sdtmig -v 3-4 -d /path/to/datasets

# Windows PowerShell
$env:DATASET_SIZE_THRESHOLD=0; .\core.exe validate -s sdtmig -v 3-4 -d C:\path\to\datasets
```

Or add to a `.env` file in the root directory:

```text
DATASET_SIZE_THRESHOLD=0
```

By default the engine uses Dask automatically for datasets exceeding 1/4 of available
RAM.

### How do I validate against TIG?

TIG requires `--substandard` and, in the case of custom domains, `--use-case` to
identify what use case the custom domains are applicable to.

```bash
python core.py validate -s tig -v 1-0 -ss SDTM -uc INDH -d /path/to/datasets
```

Valid substandards: `SDTM`, `SEND`, `ADaM`, `CDASH`
Valid use cases: `INDH`, `PROD`, `NONCLIN`, `ANALYSIS`

### How do I run only specific rules?

Use `-r` to include specific CORE IDs, or `-er` to exclude them:

```bash
# Only these rules
python core.py validate -s sdtmig -v 3-4 -d /data -r CORE-000001 -r CORE-000002

# All rules except these
python core.py validate -s sdtmig -v 3-4 -d /data -er CORE-000001
```

You can view and clone the CDISC CORE rules at
[cdisc-open-rules](https://github.com/cdisc-org/cdisc-open-rules).

## Privacy & Data Protection

### Does CORE transmit my study data anywhere during validation?

**No. All input data remains local to the machine where CORE is executed.** Specifically:

- Study files are read directly from the local filesystem (or a specified input path)
  and are never uploaded or transmitted anywhere.
- Validation runs entirely in-process on the local machine (or whatever environment
  CORE is deployed to — on-premises server, cloud VM, container, etc.).
- The output report is written locally upon completion (or to a specified output
  directory).
- All metadata required for rule execution (controlled terminology packages, standard
  metadata, etc.) is pre-fetched via `update-cache` and bundled at release time. Rule
  Validation execution itself requires no outbound network calls carrying study data.

**No patient or personal data ever leaves the environment where CORE is installed**,
supporting compliance with data protection requirements such as HIPAA, GDPR, and sponsor
data governance policies.

---

## Custom Rules & Standards

### What's the difference between custom rules and custom standards?

- **Custom rules** are individual rule definitions stored in the cache by CORE ID.
- **Custom standards** map a standard identifier to a list of rule IDs, acting as a
  lookup for which rules apply.

Add your custom rules first, then create a standard that references them. See
[CLI Reference → update-cache](cli-reference.md#custom-rules) for details. Custom Rules
& Standards continue to be a work in progress, there are tickets within CORE's Issues
to fully implement further support for them in the future.

### Can a custom standard use CDISC library metadata?

Yes. If you name your custom standard after an existing CDISC standard
(e.g. `sdtmig/3-4`), CORE will fetch library metadata for that standard while applying
your custom rules.

---

## Still Need Help?

- **Search existing Q&A:**
  [GitHub Discussions](https://github.com/cdisc-org/cdisc-rules-engine/discussions/categories/q-a)
- **Open a new discussion:** For questions or usage help
- **Open an issue:**
  [GitHub Issues](https://github.com/cdisc-org/cdisc-rules-engine/issues)
  for bugs or feature requests
