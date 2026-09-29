# NHI D1 synchronization operations

## Safety model

- A stable SHA-256 fingerprint is calculated from the cleaned, sorted dataset.
- An unchanged fingerprint records a successful check without rewriting the drug table.
- A changed fingerprint triggers one paginated comparison with D1. Only inserted, changed, and removed drug codes are written.
- TFDA detailed prescription ingredients are downloaded and reduced on the GitHub runner. D1 receives one compact profile per matched NHI license, never the full 126,000-row source dataset.
- Same-group Level 1 verification compares the complete normalized ingredient-code, amount, and unit set. Profiles without structured amounts remain on the existing group-based fallback.
- Ingredient profiles use their own fingerprint. An unchanged profile set performs no profile-table scan and writes only the small synchronization-state check.
- The table and its indexes remain in place. Daily synchronization never drops or rebuilds them.
- The run is deferred when projected account usage exceeds the operating thresholds, current usage cannot be read and the change is large, or the source data changes abnormally.
- A deferred run fails only after the independent TFDA step has had a chance to run. GitHub can notify the maintainer, and the next daily schedule retries normally.

Default operating thresholds reserve capacity below the Cloudflare Free limits:

- stop optional work above 3,500,000 projected rows read;
- stop the NHI write above 70,000 projected rows written;
- when account analytics are unavailable, allow at most 10,000 estimated rows written;
- require review above 2,000 changed drug codes or a 5% deletion ratio.
- require at least 70% NHI-license coverage before ingredient profiles can be written;
- require review above 2,000 changed ingredient profiles after the initial empty-table bootstrap.

Override these values with `D1_READ_PAUSE_THRESHOLD`, `D1_WRITE_PAUSE_THRESHOLD`, `D1_UNKNOWN_USAGE_WRITE_CAP`, `D1_MAX_CHANGED_ROWS`, `D1_MAX_DELETE_RATIO`, `D1_INGREDIENT_MAX_CHANGED_ROWS`, and `D1_INGREDIENT_MIN_COVERAGE`.

## Manual modes

- Normal manual run: download, compare, and write only safe changes.
- `nhi_dry_run=true`: download and compare, but do not create synchronization state, write drug data, or run TFDA.
- `ingredient_only=true`: process NHI only as the active-license input, then update TFDA ingredient profiles without writing the NHI drug table or running the supply/recall sync. This explicit bootstrap path permits up to 25,000 estimated writes when account analytics are unavailable; scheduled runs retain the 10,000-row unknown-usage cap and every run retains the 70,000-row operating threshold.
- `d1_preflight_only=true`: validate D1 connectivity, required tables, indexes, latest sync log, and account usage without downloading NHI data or writing D1.
- `tfda_only=true`: skip NHI and run only the protected TFDA synchronization.

Every NHI run writes `upload_report.json` and `ingredient_profile_report.json` and adds both to the GitHub Actions step summary. `deferred_quota` and `deferred_data_anomaly` deliberately mark the workflow unsuccessful so the condition is visible and retried on the next schedule. The TFDA supply/recall job waits for the NHI/profile job to finish, preventing both jobs from independently reserving the same daily D1 capacity.

## Incident history

The former importer dropped and rebuilt `nhi_drugs` and three indexes on every run. A 13,808-drug import wrote about 69,000 rows. One manual run and one scheduled run in the same UTC billing day therefore wrote about 138,000 NHI rows before TFDA and other writes were counted.
