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

## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.

## Links

| | |
|---|---|
| Samyama Graph | [github.com/samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph) |
| The Book | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Cricsheet | [cricsheet.org](https://cricsheet.org/) |
| Contact | [samyama.dev/contact](https://samyama.dev/contact) |
