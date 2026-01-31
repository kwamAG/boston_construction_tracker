# Boston Large HVAC Construction Projects Tracker

Queries Boston open data APIs weekly for large construction projects ($1M+) likely to involve significant HVAC/pipefitting work. Generates a mobile-friendly HTML report published via GitHub Pages.

## Data Sources

- **Article 80 Development Projects** — BPDA large development pipeline
- **Approved Building Permits** — All issued permits with dollar valuations

## Setup

1. Create a GitHub repo named `boston_construction_tracker`
2. Push this code to the repo
3. Enable GitHub Pages: Settings > Pages > Source: deploy from branch `main`, folder `/docs`
4. The workflow runs automatically every Monday at 7 AM EST
5. View the report at `https://<your-username>.github.io/boston_construction_tracker/`

## Run Locally

```bash
python3 tracker.py
open docs/index.html
```

No pip installs needed — uses only Python standard library.

## Configuration

Edit `config.json` to customize:
- `min_valuation` — minimum dollar value to include (default: $1,000,000)
- `auto_flag_valuation` — auto-flag threshold regardless of keywords (default: $10,000,000)
- `hvac_keywords` — full keyword list for filtering
- `direct_hvac_keywords` — keywords indicating direct HVAC/mechanical work
- `project_type_keywords` — project types likely to have large HVAC scopes
