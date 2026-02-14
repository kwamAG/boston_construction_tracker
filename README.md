# Boston Large HVAC Construction Projects Tracker

An automated monitoring tool that tracks large-scale construction projects in Boston ($1M+) likely to involve significant HVAC, mechanical, plumbing, and pipefitting work. Data is pulled weekly from Boston's open data APIs — including Article 80 development projects and approved building permits — filtered by relevance, and published as an interactive dashboard.

**Live dashboard:** [https://kwamAG.github.io/boston_construction_tracker/](https://kwamAG.github.io/boston_construction_tracker/)

## Features

- **Automated weekly updates** via GitHub Actions (Mondays at 7 AM EST)
- **Two data sources**: Article 80 development projects and approved building permits from [data.boston.gov](https://data.boston.gov)
- **HVAC relevance scoring** (High / Medium / Low) based on keyword matching and project valuation
- **Interactive HTML dashboard** with search, filtering by source/relevance/neighborhood, and sorting
- **Zero external dependencies** — pure Python standard library backend and vanilla HTML/CSS/JS frontend

## How It Works

1. `tracker.py` queries the Boston Open Data APIs for construction projects valued at $1M+
2. Projects are scored for HVAC relevance using keyword matching (HVAC, mechanical, plumbing, MEP, etc.)
3. New projects are flagged and an updated HTML report is generated in `docs/index.html`
4. GitHub Actions commits and pushes the updated report, which is served via GitHub Pages

