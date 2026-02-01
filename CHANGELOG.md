# Changelog

## [1.1.0] - 2026-01-31

### Added
- **Search bar** — full-width text input filters cards by name, address, description, keywords, neighborhood, and applicant (200ms debounce)
- **Filter dropdowns** — Source (Article 80 / Permits), HVAC Relevance (High / Medium / Low), Neighborhood, Status, and Sort order (Newest / Oldest)
- **Unified card layout** — all projects displayed in a single filterable list replacing the previous 4-section layout
- **Source badges** — blue "Article 80" and orange "Permit" labels on each card
- **Links row on each card**:
  - View Project (links to BostonPlans.org for Article 80 projects)
  - Permit #XXXXX (links to Boston permits dataset page)
  - Search News (Google search for project name + address)
  - View on Map (Google Maps link)
- **Date sorting** — sort projects by newest or oldest using `primary_date`
- **"High Relevance" stat** — 5th summary box showing count of high-relevance projects
- **Additional API fields captured**:
  - Article 80: `website_url`, `last_filed_date`, `primary_date`
  - Permits: `permit_number`, `applicant`, `worktype`, `permit_type_descr`, `expiration_date`, `primary_date`
- **Permit status from API** — reads actual `STATUS` field instead of hardcoding "Issued"
- **Responsive mobile layout** — filters stack vertically on screens under 600px
- **"Showing X of Y projects"** counter and "No results" message

### Changed
- `generate_html()` fully rewritten with unified card renderer, inline JS, and updated CSS
- `process_article80()` and `process_permits()` extended with new fields (no changes to filtering/scoring logic)

### Unchanged
- `config.json`, `seen_projects.json` schema, `.github/workflows/weekly.yml`, `main()` flow
- Single self-contained HTML file with no external dependencies

## [1.0.0] - 2026-01-31

### Initial release
- Queries Boston Article 80 and Approved Building Permits APIs
- Filters for large construction projects ($1M+) with HVAC/pipefitting relevance
- Keyword matching and relevance scoring (high / medium / low)
- Tracks new vs. previously seen projects via `seen_projects.json`
- Generates mobile-friendly HTML report at `docs/index.html`
- GitHub Actions workflow for weekly automated updates
