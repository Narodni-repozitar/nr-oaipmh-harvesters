# NR OAI-PMH Harvesters

OAI-PMH metadata transformers for the [Czech National Repository](https://github.com/Narodni-repozitar) (Národní Repozitář). This package converts harvested MARC 21 records from external repositories into the NR metadata schema, enabling seamless ingestion into the Invenio-based national repository infrastructure.

## Overview

`nr-oaipmh-harvesters` is a plugin for [oarepo-oai-pmh-harvester](https://github.com/oarepo/oarepo-oai-pmh-harvester) that provides **transformer** implementations. Each transformer maps MARC 21 fields from a specific source repository to the NR documents metadata model (`nr-metadata`).

Currently supported sources:

| Source | Transformer key | Description |
|--------|----------------|-------------|
| **NUSL** (Národní úložiště šedé literatury) | `nusl` | National Repository of Grey Literature — theses, reports, conference papers, and more |

## Requirements

- Python ≥ 3.9
- A running [Invenio](https://inveniosoftware.org/) instance with the NR stack
- Dependencies (installed automatically): `oarepo-oai-pmh-harvester >= 4.0.0`, `dojson`, `Levenshtein`, `nr-metadata`

## Installation

```bash
pip install nr-oaipmh-harvesters
```

The package registers itself as an Invenio extension via entry points — no additional configuration is needed beyond the standard Invenio app setup.

## Usage

### Registering a harvester

Use the Invenio CLI to register a new OAI-PMH harvester. For NUSL:

```bash
invenio oarepo oai harvester add nusl \
    --name "NUSL harvester" \
    --url http://invenio.nusl.cz/oai2d/ \
    --set global \
    --prefix marcxml \
    --loader sickle \
    --transformer marcxml \
    --transformer nusl \
    --writer 'service{service=nr_documents}'
```

This sets up a harvester that:

1. Connects to the NUSL OAI-PMH endpoint.
2. Fetches records using the `sickle` loader.
3. Pipes them through the `marcxml` transformer (generic MARC XML → JSON), then the `nusl` transformer (NUSL-specific mapping to NR schema).
4. Writes the resulting records via the `nr_documents` service.

### Running the harvest

```bash
# Harvest new/updated records (incremental, from last timestamp)
invenio oarepo oai harvester run nusl

# Re-harvest everything
invenio oarepo oai harvester run nusl --all-records

# Run on background via Celery
invenio oarepo oai harvester run nusl --on-background

# Harvest specific record(s)
invenio oarepo oai harvester run nusl --identifier oai:invenio.nusl.cz:12345
```

## Architecture

### Transformer pipeline

```
OAI-PMH endpoint
  │
  ▼
Loader (sickle)         ── fetches raw XML
  │
  ▼
Transformer: marcxml    ── XML → flat JSON  {marc_field_code: value}
  │
  ▼
Transformer: nusl       ── MARC JSON → NR metadata schema
  │
  ▼
Writer (service)        ── creates/updates Invenio records
```

### NUSL transformer

The `NUSLTransformer` (extending `OAIRuleTransformer`) handles the following MARC 21 fields:

| MARC field | Target metadata |
|-----------|-----------------|
| 001 | System identifier (NUSL control number) |
| 020 / 022 | ISBN / ISSN |
| 035 | Original OAI record identifier |
| 041 | Language |
| 046 | Date issued / date modified |
| 245 / 246 | Title, translated title, alternate title, subtitle |
| 260 | Publisher |
| 336 | Certified methodology resource type |
| 490 | Series |
| 502 | Degree grantor, date defended |
| 520 | Abstract |
| 540 | Rights / license (Creative Commons parsing) |
| 586 | Defense status |
| 598 | Notes |
| 650 / 653 | Subjects and keywords (Czech / English) |
| 656 | Study field |
| 710 | Degree grantor (institutional) |
| 711 | Event (conference) |
| 720 | Creators and contributors (with ORCID, affiliation resolution) |
| 773 | Related item |
| 856 | Original record URL, external location, file attachments |
| 970 | Catalogue system number |
| 980 | Resource type |
| 996 | Accessibility |
| 998 | Collection |
| 999 | Funding references |

The transformer also performs post-processing such as deduplication of languages, contributors, subjects, and additional titles.

### Vocabulary resolution

The package includes a `VocabularyCache` that resolves free-text institution names (from MARC 720 affiliations) against the NR institutions vocabulary using Lucene queries and Levenshtein distance matching. Resolved institutions are cached via `invenio-cache` with a configurable TTL (default: 1 hour). A fallback temporary institutions lookup table (`temp_institutions.py`) is used for records that cannot be matched through the vocabulary service.

## Project structure

```
nr-oaipmh-harvesters/
├── nr_oaipmh_harvesters/
│   ├── config.py                 # Registers transformers in DATASTREAMS_TRANSFORMERS
│   ├── ext.py                    # Invenio extension (NRDocsOAIHarvesterExt)
│   └── nusl/
│       ├── __init__.py           # Exports NUSLTransformer
│       ├── transformer.py        # NUSL MARC 21 → NR metadata transformer
│       └── temp_institutions.py  # Fallback institution name mapping
├── tests/
│   ├── run_transform.py          # End-to-end harvest test
│   ├── run_transform_separately.py  # Per-record transformer test with validation
│   ├── test_institutions.py      # Institution resolution tests
│   ├── get_code.py
│   └── invenio.cfg
├── format.sh                     # Code formatting (black, autoflake, isort)
├── setup.cfg
├── setup.py
├── pyproject.toml
└── README.md
```

## Development

### Setup

```bash
git clone git@github.com:Narodni-repozitar/nr-oaipmh-harvesters.git
cd nr-oaipmh-harvesters
pip install -e ".[dev]"
```

### Code formatting

```bash
./format.sh
```

This runs `black` (target Python 3.10), `autoflake` (unused import removal), and `isort` (import sorting, black profile).

### Testing transformations locally

You can test the transformer against a local directory of OAI records:

```bash
# Requires a running Invenio app context and an oai-data directory
python tests/run_transform_separately.py
```

Errors are written to `/tmp/errors.yaml` for inspection.

## Adding a new source repository

To add support for harvesting from a new OAI-PMH source:

1. Create a new sub-package under `nr_oaipmh_harvesters/` (e.g., `nr_oaipmh_harvesters/my_source/`).
2. Implement a transformer class extending `OAIRuleTransformer` from `oarepo-oaipmh-harvester`.
3. Register the transformer in `config.py` by adding it to the `DATASTREAMS_TRANSFORMERS` dictionary.
4. Register the harvester via the Invenio CLI with `--transformer my_source`.

## Related packages

- [oarepo-oai-pmh-harvester](https://github.com/oarepo/oarepo-oai-pmh-harvester) — Core harvesting framework
- [nr-metadata](https://github.com/Narodni-repozitar/nr-model) — NR metadata model (documents & data)
- [nr-docs](https://github.com/Narodni-repozitar/nr-docs) — NR document repository application

## Authors

- Alžběta Pokorná (alzbeta.pokorna@cesnet.cz)
- Miroslav Šimek (miroslav.simek@cesnet.cz)
- Juraj Trappl (juraj.trappl@cesnet.cz)
