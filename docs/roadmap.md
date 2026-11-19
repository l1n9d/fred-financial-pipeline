# Roadmap

Work that is deliberately deferred, with the research already done so whoever
picks it up doesn't have to start from scratch. Each entry says what it needs,
what it would cost, and what has *not* been verified yet.

---

## A. SQL linting with sqlfluff

Considered for the Phase 1 CI workflow (Issue #3) and left out, because it
cannot be added without either reformatting existing models or hand-picking a
rule subset that would need validating first.

**It needs the dbt templater.** `templater = dbt` plus the
`sqlfluff-templater-dbt` package. The plain `jinja` templater does not resolve
third-party package macros, and `stg_fred_observations.sql:41` calls
`dbt_utils.generate_surrogate_key`. With the jinja templater that call would
need a hand-written Python shim loaded through sqlfluff's `library_path` —
sqlfluff's own docs use `dbt_utils.group_by` as the example of exactly this.

**Does the dbt templater need a warehouse connection?** Only conditionally.
sqlfluff's docs say a database is required "if your `dbt` model files access a
database at compile time", and note you can point it at a test database rather
than production. No model in this project queries at compile time (no
`run_query`, no introspective macros), so linting would very likely work
offline with the same placeholder `SNOWFLAKE_*` values the CI workflow already
uses for `dbt parse`. The docs do not state that case explicitly, and sqlfluff
has not been installed here to confirm it — **verifying this offline is step one**
whenever this is picked up. If it turns out a connection is required, this moves
behind entry B.

**It implies a one-time reformat.** Four of the seven models carry trailing
whitespace that sqlfluff's default layout rules flag:

- `models/staging/stg_fred_observations.sql`
- `models/staging/stg_fred_metadata.sql`
- `models/marts/mart_economic_dashboard.sql`
- `models/marts/mart_indicator_summary.sql`

That reformat should land as its own commit, separate from any logic change, so
the diff stays reviewable. The first real run will likely surface more than
whitespace, so budget for choosing a rule set rather than accepting all defaults.

---

## B. Phase 2 CI — `dbt build` against a dedicated Snowflake CI schema

Phase 1 CI validates structure only. It cannot catch SQL that is valid Jinja but
invalid Snowflake SQL, and it runs none of the warehouse-backed checks: `dbt run`,
the schema tests, the two singular tests in `dbt_fred/tests/`, or the three
Sahm-rule `unit_tests` in `models/intermediate/_int_models.yml`. Running those in
CI means giving GitHub Actions real Snowflake credentials.

**Blocker to fix first.** `macros/generate_schema_name.sql` returns
`custom_schema_name` verbatim, so a CI target would write straight into the
production `STAGING` / `INTERMEDIATE` / `MARTS` schemas. The macro has to become
target-aware (e.g. prefix `CI_` when `target.name == 'ci'`) before any of this is
safe. That change touches every layer and deserves its own issue and its own
verified `dbt build`.

**Snowflake side, least privilege.** A dedicated `FRED_CI` user and `CI_ROLE`:

- `USAGE` on `FRED_PIPELINE` and on an XS `CI_WH` (auto-suspend 60s)
- `SELECT` only on `RAW`, so CI reads real data but cannot change it
- `CREATE SCHEMA` on `FRED_PIPELINE`, so it owns — and can drop — its own CI schemas
- never `ACCOUNTADMIN`

Note that `profiles.yml` currently hardcodes `role: ACCOUNTADMIN`, `database` and
`warehouse`, so a `ci` output must parameterise all three. Prefer key-pair auth
over a password, since Snowflake is phasing out password-only service logins.

**Workflow side.** A `dbt-build` job gated on the Phase 1 jobs passing, reading
`SNOWFLAKE_*` and the private key from GitHub **secrets** scoped to a protected
Environment with a required reviewer, running
`dbt build --profiles-dir . --target ci`. A final `if: always()` step drops the
run's CI schema so failed runs leave no debris.

**Cost.** An XS warehouse is ~1 credit/hour, billed with a 60-second minimum per
resume. Seven small models is roughly 1–2 minutes of warehouse time per run —
a few cents. Even 100 runs a month stays in single-digit dollars. The real risk
is a re-run loop, which the workflow's `concurrency` group and the review gate
both bound.

**Security trade-offs.** Secrets are never exposed to pull requests from forks,
so this job must be conditional or fork PRs will fail confusingly. Anyone who can
open a branch PR in this repo can edit a workflow and print secrets, so the blast
radius has to be bounded by the role itself: read-only `RAW`, writes confined to
CI schemas, no production write path, and credentials rotatable independently of
the ones in `.env`.
