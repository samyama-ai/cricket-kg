---
license: other
pretty_name: Cricket Knowledge Graph
tags:
  - knowledge-graph
  - samyama
  - property-graph
  - sports
language:
  - en
size_categories:
  - 10K<n<100K
---

# Dataset Card for `cricket-kg`

**36K nodes. 1.4M edges. Every ball from 21,324 matches -- Tests, ODIs, T20s, IPL, BBL, and more.**

> Part of the **Samyama** ecosystem. This card describes the dataset; the repository
> holds the loader and source-data specifics.

## Structure

**6 node labels** -- Player (12,933), Match (21,324), Tournament (1,053), Venue (877), Team (383), Season (49)

**12 edge types** -- BATTED_IN, BOWLED_IN, DISMISSED, FIELDED_DISMISSAL, PLAYED_FOR, COMPETED_IN, WON, WON_TOSS, HOSTED_AT, PART_OF, IN_SEASON, PLAYER_OF_MATCH

**Data source** -- [Cricsheet.org](https://cricsheet.org/) (CC-BY-4.0), 21,325 JSON files with ball-by-ball granularity

## Provenance and licence

Apache 2.0. Data from Cricsheet.org is CC-BY-4.0.

## Freshness

**Refresh cadence:** Cricsheet.org publishes new/updated match JSON continuously
(within days of each match being played), but this repository has no automated
refresh -- the loader (`etl/loader.py`) must be re-run manually against a fresh
`all_json.zip` download to pick up new matches. There is no scheduled cadence for
that manual re-run; treat the graph as a point-in-time historical extract rather
than a live-updating one.

**Data as of:** The loader was last modified and verified to run end-to-end against
Cricsheet's live `all_json.zip` on 2026-07-31 (from `git log` on `etl/loader.py`;
`DATASET_CARD.md` itself was last updated 2026-08-15). This dates the loader/pipeline,
not a vendored data snapshot -- this repository ships the loader and source-data
specifics, not a built graph; rebuilding from source pulls whatever Cricsheet
currently has.

## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.

## Citation

Please cite this repository if you use it. See [`CITATION.cff`](CITATION.cff) for
machine-readable metadata (CFF 1.2.0).

```bibtex
@misc{cricket_kg_2026,
  title        = {Cricket Knowledge Graph},
  author       = {Samyama},
  year         = {2026},
  howpublished = {\url{https://git.samyama.ai/Samyama.ai/cricket-kg}}
}
```

**No DOI.** This release has not been deposited to Zenodo, so there is no DOI to
cite. Getting one is open work -- it requires a human to make the Zenodo deposit
(KG-06).

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Cricsheet | [cricsheet.org](https://cricsheet.org/) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |
