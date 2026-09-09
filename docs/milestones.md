# Milestones and gates

## M0 — source, contracts, and capacity

Accept when official URLs and dictionaries are reachable; Yellow and HVFHV
contracts are distinct; a complete annual window is inventoried from headers and
Parquet footers; schema evolution is observed; bounded samples are inspected;
and host capacity supports a documented target without downloading the corpus.

Cancel or redesign if source terms prevent reproducibility, the composition
cannot meet the scale objective, or projected storage leaves less than the
reserved free-space floor.

## M1 — vertical slice and standalone runtime

Process bounded real files through versioned landing, a minimal Delta
source-occurrence table per service, both service-contract paths and quality
checks. Fixture tests run without Spark; the Delta integration smoke and the
slice itself run on Docker Compose with a master plus worker processes under
explicit memory and core budgets.

Accept when replay is a no-op, counts and service-specific sums match a DuckDB
oracle, and the container runtime is reproducible. Measured results are in
[`m1-vertical-slice.md`](m1-vertical-slice.md). Contracted service tables and
analytical products move to M2 with the rest of the correctness work.

## M2 — contracted data correctness

Validate Delta publication, file-version replacement, compatible and incompatible
schema drift, quality incidents, and an incremental-versus-full-rebuild oracle.

Accept when readers never observe partial publication and every affected
partition matches the rebuild oracle. Concurrent writes may only be claimed for
what a probe actually measured on the storage in use.

Measured results are in [`m2-contracted-correctness.md`](m2-contracted-correctness.md).
Six real bounded versions were resolved against the two contracts, one was
rejected for incompatible drift, a warehouse maintained across two runs matched a
full rebuild on schema, keys, counts and content digests, and replaying changed
nothing. An injected failure between Delta tables remained invisible until replay
completed it, and a scoped rerun preserved omitted periods. A dataset publication
boundary decides which derivation context readers are on: landing a newer zone
lookup does not move it, a context change is refused unless one run rebuilds
every artifact the previous context published, and each contract fingerprint owns
its own tables so a revised output schema is materialised without `mergeSchema`.
A separate two-driver probe showed no loss or duplication for one throwaway Delta
table; orchestration remains single-coordinator. Analytical products, the
dimensional model and Power BI stay in M3.

## M3 — distributed execution and portfolio evidence

Run measured experiments near 1M, 20M, and the ratified annual scale. Compare
DuckDB, one Spark worker, and multiple workers fairly; observe real shuffle,
skew, spill, and executor loss. A faster single-node baseline is a valid result.

Accept when outputs remain equivalent, recovery does not double-publish, and
configuration-specific conclusions are documented. Add a secondary 2–3 page
Power BI report over the analytical products. Publish an explicit dimensional
model with a trip-occurrence fact, conformed date/zone/service/source-version
dimensions, and separate service-specific fare facts.

Two conditions M2 leaves open must be settled before the scale runs, not during
them.

- **Equivalence above the digest bound.** The exact sorted digest collects one
  hash per row on the driver and is skipped above `--max-digest-rows`, leaving
  only an additive checksum that is insensitive to permutations and to
  compensating errors. M3 works far above that bound, so equivalence at scale
  needs a mechanism that keeps the same strength — a per-partition digest, or a
  batched comparison — before any result is claimed equivalent.
- **Measurement cost separated from processing cost.** Measuring a table scans
  it several times: distinct keys, a count and a full digest per table, and
  three counts per written version. That is invisible at 5,000 rows and would
  dominate a run at 100 million, contaminating any comparison against DuckDB.
  The engine comparison must time processing, with measurement either excluded
  or reported separately.

## M4 — separately approved cloud proof

First select a provider from current demand, compatibility, security, and
official cost evidence. Then request approval for a bounded design and budget.
Only an applied-and-destroyed proof may support cloud, IAM, IaC, or measured-cost
claims.

## M5 — portfolio and defense

Consolidate the README, useful diagrams and ADRs, an operations runbook, measured
results and limitations, the final repository audit, independent review, and an
English interview briefing. M5 follows M3 whether M4 is executed or skipped.

Accept only when the Definition of Done is complete, remote CI is observed green,
public claims map to evidence, and no cloud capability is claimed without an
applied and destroyed M4 proof.
