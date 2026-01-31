#!/usr/bin/env python3
"""Boston Large HVAC Construction Projects Tracker.

Queries Boston open data APIs for large construction projects ($1M+) with
HVAC/pipefitting relevance and generates a mobile-friendly HTML report.
Uses only Python standard library (no pip installs).
"""

import json
import os
import ssl
import urllib.request
import urllib.parse
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
SEEN_PATH = os.path.join(SCRIPT_DIR, "seen_projects.json")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "docs", "index.html")

ARTICLE80_URL = "https://data.boston.gov/api/3/action/datastore_search"
ARTICLE80_RESOURCE = "32e3dc10-182d-4f51-bbd9-4c28b525f1ed"

PERMITS_URL = "https://data.boston.gov/api/3/action/datastore_search"
PERMITS_RESOURCE = "6ddcd912-32a0-43df-9908-63574f8c7e77"


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def api_fetch(base_url, resource_id, limit=1000, offset=0):
    """Fetch records from Boston CKAN datastore API."""
    params = urllib.parse.urlencode({
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset,
    })
    url = f"{base_url}?{params}"
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "BostonHVACTracker/1.0"})
    with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not data.get("success"):
        raise RuntimeError(f"API error for {resource_id}: {data}")
    return data["result"]


def fetch_all_records(base_url, resource_id, max_records=5000):
    """Page through API results to get all records up to max_records."""
    all_records = []
    offset = 0
    limit = 1000
    while offset < max_records:
        result = api_fetch(base_url, resource_id, limit=limit, offset=offset)
        records = result.get("records", [])
        all_records.extend(records)
        if len(records) < limit:
            break
        offset += limit
    return all_records


def parse_valuation(val):
    """Parse a dollar valuation string into a float."""
    if not val:
        return 0.0
    s = str(val).strip().replace("$", "").replace(",", "").replace(" ", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_sqft(val):
    """Parse square footage into an integer."""
    if not val:
        return 0
    s = str(val).strip().replace(",", "").replace(" ", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def match_keywords(text, keywords):
    """Return list of keywords found in text (case-insensitive)."""
    if not text:
        return []
    lower = text.lower()
    return [kw for kw in keywords if kw.lower() in lower]


def score_hvac_relevance(matched_direct, matched_type, valuation, auto_flag_val):
    """Score HVAC relevance: 'high', 'medium', or 'low'."""
    if valuation >= auto_flag_val:
        return "high"
    if matched_direct:
        return "high"
    if matched_type and valuation >= 5_000_000:
        return "high"
    if matched_type:
        return "medium"
    return "low"


def process_article80(records, config):
    """Filter and score Article 80 development projects."""
    direct_kw = config["direct_hvac_keywords"]
    type_kw = config["project_type_keywords"]
    auto_flag = config["auto_flag_valuation"]
    projects = []

    for r in records:
        # Build searchable text from all relevant fields
        # API field names: project__project_name, description, project_uses,
        # neighborhood, project_status, project__record_type, gross_square_footage
        search_fields = [
            r.get("project__project_name", ""),
            r.get("description", ""),
            r.get("project_uses", ""),
            r.get("neighborhood", ""),
            r.get("project_status", ""),
        ]
        search_text = " ".join(str(f) for f in search_fields if f)

        sqft = parse_sqft(r.get("gross_square_footage", ""))

        # Use total_development_cost if available, else estimate from sq footage
        dev_cost = parse_valuation(r.get("total_development_cost", ""))
        estimated_val = dev_cost if dev_cost > 0 else (sqft * 300 if sqft > 0 else 0)

        matched_direct = match_keywords(search_text, direct_kw)
        matched_type = match_keywords(search_text, type_kw)
        all_matched = matched_direct + matched_type

        # Include if: has HVAC keywords, or large project, or record type is "Large Project"
        record_type = str(r.get("project__record_type", "")).lower()
        is_large = "large" in record_type
        has_keywords = len(all_matched) > 0
        is_big_sqft = sqft >= 50000

        if not (has_keywords or is_large or is_big_sqft):
            continue

        relevance = score_hvac_relevance(matched_direct, matched_type, estimated_val, auto_flag)

        # Build address from parts
        street_num = r.get("project_street_number", "")
        street_name = r.get("project_street_name", "")
        street_suffix = r.get("project_street_suffix", "")
        address = " ".join(filter(None, [str(street_num), str(street_name), str(street_suffix)]))

        projects.append({
            "id": str(r.get("_id", r.get("project_id", ""))),
            "name": r.get("project__project_name") or "Unknown",
            "address": address or "N/A",
            "neighborhood": r.get("neighborhood") or "N/A",
            "status": r.get("project_status") or "N/A",
            "record_type": r.get("project__record_type") or "N/A",
            "sqft": sqft,
            "estimated_valuation": estimated_val,
            "description": r.get("description") or "N/A",
            "proposed_use": r.get("project_uses") or "N/A",
            "board_approval_date": r.get("last_board_approved_date") or "N/A",
            "keywords_matched": all_matched,
            "hvac_relevance": relevance,
            "source": "article80",
        })

    return projects


def process_permits(records, config):
    """Filter and score building permits."""
    min_val = config["min_valuation"]
    auto_flag = config["auto_flag_valuation"]
    direct_kw = config["direct_hvac_keywords"]
    type_kw = config["project_type_keywords"]
    projects = []

    for r in records:
        valuation = parse_valuation(r.get("DECLARED_VALUATION", r.get("declared_valuation", "")))
        if valuation < min_val:
            continue

        search_fields = [
            r.get("DESCRIPTION", r.get("description", "")),
            r.get("COMMENTS", r.get("comments", "")),
            r.get("PERMITTYPE", r.get("permittype", "")),
        ]
        search_text = " ".join(str(f) for f in search_fields if f)

        matched_direct = match_keywords(search_text, direct_kw)
        matched_type = match_keywords(search_text, type_kw)
        all_matched = matched_direct + matched_type

        # Include if has keywords or valuation >= auto-flag threshold
        if not all_matched and valuation < auto_flag:
            continue

        relevance = score_hvac_relevance(matched_direct, matched_type, valuation, auto_flag)

        sqft = parse_sqft(r.get("SQ_FEET", r.get("sq_feet", "")))

        projects.append({
            "id": str(r.get("_id", r.get("PERMITNUMBER", r.get("permitnumber", "")))),
            "name": r.get("DESCRIPTION", r.get("description", "Permit")) or "Permit",
            "address": r.get("ADDRESS", r.get("address", "N/A")),
            "neighborhood": r.get("CITY", r.get("city", "Boston")),
            "status": "Issued",
            "permit_type": r.get("PERMITTYPE", r.get("permittype", "N/A")),
            "sqft": sqft,
            "valuation": valuation,
            "description": r.get("COMMENTS", r.get("comments", "N/A")),
            "issued_date": r.get("ISSUED_DATE", r.get("issued_date", "N/A")),
            "keywords_matched": all_matched,
            "hvac_relevance": relevance,
            "source": "permits",
        })

    return projects


def identify_new_projects(projects, seen_ids):
    """Mark projects as new or previously seen."""
    for p in projects:
        p["is_new"] = p["id"] not in seen_ids
    return projects


def format_currency(val):
    """Format a number as currency."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:,.0f}"


def format_sqft(val):
    """Format square footage."""
    if val <= 0:
        return "N/A"
    return f"{val:,} sq ft"


def escape_html(text):
    """Escape HTML special characters."""
    if not text:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def generate_html(article80_projects, permit_projects, run_time):
    """Generate mobile-friendly HTML report."""
    new_a80 = [p for p in article80_projects if p.get("is_new")]
    seen_a80 = [p for p in article80_projects if not p.get("is_new")]
    new_permits = [p for p in permit_projects if p.get("is_new")]
    seen_permits = [p for p in permit_projects if not p.get("is_new")]

    # Sort by relevance (high first) then by valuation/sqft descending
    def sort_key(p):
        rel_order = {"high": 0, "medium": 1, "low": 2}
        return (rel_order.get(p.get("hvac_relevance", "low"), 2),
                -(p.get("valuation", 0) or p.get("estimated_valuation", 0)))

    for lst in [new_a80, seen_a80, new_permits, seen_permits]:
        lst.sort(key=sort_key)

    def relevance_badge(rel):
        colors = {"high": "#c0392b", "medium": "#e67e22", "low": "#7f8c8d"}
        color = colors.get(rel, "#7f8c8d")
        return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:10px;font-size:0.75em;font-weight:bold;">{rel.upper()}</span>'

    def new_badge():
        return '<span style="background:#27ae60;color:#fff;padding:2px 8px;border-radius:10px;font-size:0.75em;font-weight:bold;margin-left:4px;">NEW</span>'

    def render_a80_card(p):
        kw_html = ""
        if p["keywords_matched"]:
            tags = "".join(
                f'<span style="background:#eee;padding:2px 6px;border-radius:4px;font-size:0.75em;margin:2px;">{escape_html(k)}</span>'
                for k in set(p["keywords_matched"])
            )
            kw_html = f'<div style="margin-top:6px;">Keywords: {tags}</div>'

        badge = new_badge() if p.get("is_new") else ""
        return f"""
        <div style="border:1px solid #ddd;border-left:4px solid {'#27ae60' if p.get('is_new') else '#3498db'};border-radius:8px;padding:12px;margin-bottom:12px;background:#fff;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
                <strong style="font-size:1em;">{escape_html(p['name'])}</strong>
                <div>{relevance_badge(p['hvac_relevance'])}{badge}</div>
            </div>
            <div style="color:#666;font-size:0.85em;margin-top:4px;">
                {escape_html(p['address'])} &bull; {escape_html(p['neighborhood'])}
            </div>
            <div style="font-size:0.85em;margin-top:6px;">
                <strong>Status:</strong> {escape_html(p['status'])} &bull;
                <strong>Type:</strong> {escape_html(p['record_type'])} &bull;
                <strong>Size:</strong> {format_sqft(p['sqft'])}
                {f" &bull; <strong>Est. Value:</strong> {format_currency(p['estimated_valuation'])}" if p['estimated_valuation'] > 0 else ""}
            </div>
            <div style="font-size:0.85em;margin-top:6px;color:#444;">
                {escape_html(str(p.get('description') or 'N/A')[:300])}{'...' if len(str(p.get('description') or '')) > 300 else ''}
            </div>
            <div style="font-size:0.85em;margin-top:4px;color:#444;">
                <strong>Proposed Use:</strong> {escape_html(str(p.get('proposed_use') or 'N/A')[:200])}
            </div>
            {kw_html}
        </div>"""

    def render_permit_card(p):
        kw_html = ""
        if p["keywords_matched"]:
            tags = "".join(
                f'<span style="background:#eee;padding:2px 6px;border-radius:4px;font-size:0.75em;margin:2px;">{escape_html(k)}</span>'
                for k in set(p["keywords_matched"])
            )
            kw_html = f'<div style="margin-top:6px;">Keywords: {tags}</div>'

        badge = new_badge() if p.get("is_new") else ""
        issued = p.get("issued_date", "N/A")
        if issued and issued != "N/A" and "T" in str(issued):
            issued = str(issued).split("T")[0]

        return f"""
        <div style="border:1px solid #ddd;border-left:4px solid {'#27ae60' if p.get('is_new') else '#e67e22'};border-radius:8px;padding:12px;margin-bottom:12px;background:#fff;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
                <strong style="font-size:1em;">{escape_html(p['name'][:100])}</strong>
                <div>{relevance_badge(p['hvac_relevance'])}{badge}</div>
            </div>
            <div style="color:#666;font-size:0.85em;margin-top:4px;">
                {escape_html(p['address'])}
            </div>
            <div style="font-size:0.85em;margin-top:6px;">
                <strong>Valuation:</strong> {format_currency(p['valuation'])} &bull;
                <strong>Size:</strong> {format_sqft(p['sqft'])} &bull;
                <strong>Issued:</strong> {escape_html(str(issued))}
            </div>
            <div style="font-size:0.85em;margin-top:6px;color:#444;">
                {escape_html(str(p.get('description') or 'N/A')[:300])}
            </div>
            {kw_html}
        </div>"""

    def render_section(title, projects, renderer, empty_msg):
        cards = "".join(renderer(p) for p in projects) if projects else f'<p style="color:#888;">{empty_msg}</p>'
        count = len(projects)
        return f"""
        <div style="margin-bottom:24px;">
            <h2 style="border-bottom:2px solid #3498db;padding-bottom:6px;font-size:1.1em;">
                {title} <span style="font-size:0.8em;color:#888;">({count})</span>
            </h2>
            {cards}
        </div>"""

    summary_new = len(new_a80) + len(new_permits)
    summary_total = len(article80_projects) + len(permit_projects)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Boston HVAC Construction Tracker</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
            line-height: 1.5;
            padding: 12px;
            max-width: 800px;
            margin: 0 auto;
        }}
        h1 {{ font-size: 1.3em; margin-bottom: 4px; }}
        .summary {{
            background: #fff;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 16px;
            border: 1px solid #ddd;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 8px;
            margin-top: 8px;
        }}
        .stat {{
            text-align: center;
            padding: 8px;
            background: #f9f9f9;
            border-radius: 6px;
        }}
        .stat-num {{ font-size: 1.5em; font-weight: bold; color: #2c3e50; }}
        .stat-label {{ font-size: 0.75em; color: #888; }}
        details summary {{
            cursor: pointer;
            font-weight: bold;
            color: #3498db;
            padding: 4px 0;
        }}
    </style>
</head>
<body>
    <h1>Boston HVAC Construction Tracker</h1>
    <p style="color:#888;font-size:0.85em;margin-bottom:12px;">
        Large projects ($1M+) with HVAC/pipefitting relevance &bull;
        Updated {run_time.strftime('%B %d, %Y at %I:%M %p')} UTC
    </p>

    <div class="summary">
        <div class="summary-grid">
            <div class="stat">
                <div class="stat-num">{summary_new}</div>
                <div class="stat-label">New This Week</div>
            </div>
            <div class="stat">
                <div class="stat-num">{summary_total}</div>
                <div class="stat-label">Total Tracked</div>
            </div>
            <div class="stat">
                <div class="stat-num">{len(article80_projects)}</div>
                <div class="stat-label">Article 80</div>
            </div>
            <div class="stat">
                <div class="stat-num">{len(permit_projects)}</div>
                <div class="stat-label">Permits</div>
            </div>
        </div>
    </div>

    {render_section("New Large Article 80 Projects", new_a80, render_a80_card, "No new Article 80 projects this week.")}

    {render_section("New High-Value Permits ($1M+)", new_permits, render_permit_card, "No new high-value permits this week.")}

    <details>
        <summary>Previously Seen Article 80 Projects ({len(seen_a80)})</summary>
        <div style="margin-top:8px;">
            {render_section("", seen_a80, render_a80_card, "None yet.")}
        </div>
    </details>

    <details>
        <summary>Previously Seen Permits ({len(seen_permits)})</summary>
        <div style="margin-top:8px;">
            {render_section("", seen_permits, render_permit_card, "None yet.")}
        </div>
    </details>

    <footer style="margin-top:24px;padding-top:12px;border-top:1px solid #ddd;color:#aaa;font-size:0.75em;text-align:center;">
        Data from <a href="https://data.boston.gov" style="color:#aaa;">data.boston.gov</a> &bull;
        Article 80 Development Projects &bull; Approved Building Permits
    </footer>
</body>
</html>"""
    return html


def main():
    print("Boston HVAC Construction Tracker")
    print("=" * 40)

    # Load config
    config = load_json(CONFIG_PATH)
    print(f"Min valuation: {format_currency(config['min_valuation'])}")
    print(f"Auto-flag threshold: {format_currency(config['auto_flag_valuation'])}")

    # Load seen projects
    seen = load_json(SEEN_PATH)
    seen_a80_ids = set(seen.get("article80", []))
    seen_permit_ids = set(seen.get("permits", []))

    # Fetch Article 80 projects
    print("\nFetching Article 80 Development Projects...")
    try:
        a80_records = fetch_all_records(ARTICLE80_URL, ARTICLE80_RESOURCE, max_records=5000)
        print(f"  Fetched {len(a80_records)} records")
    except Exception as e:
        print(f"  Error fetching Article 80 data: {e}")
        a80_records = []

    # Fetch building permits
    print("Fetching Approved Building Permits...")
    try:
        permit_records = fetch_all_records(PERMITS_URL, PERMITS_RESOURCE, max_records=5000)
        print(f"  Fetched {len(permit_records)} records")
    except Exception as e:
        print(f"  Error fetching permits data: {e}")
        permit_records = []

    # Process and filter
    print("\nProcessing Article 80 projects...")
    a80_projects = process_article80(a80_records, config)
    print(f"  {len(a80_projects)} projects match HVAC criteria")

    print("Processing building permits...")
    permit_projects = process_permits(permit_records, config)
    print(f"  {len(permit_projects)} permits match HVAC criteria")

    # Mark new vs. previously seen
    a80_projects = identify_new_projects(a80_projects, seen_a80_ids)
    permit_projects = identify_new_projects(permit_projects, seen_permit_ids)

    new_a80 = sum(1 for p in a80_projects if p["is_new"])
    new_permits = sum(1 for p in permit_projects if p["is_new"])
    print(f"\n  New Article 80 projects: {new_a80}")
    print(f"  New permits: {new_permits}")

    # Generate HTML report
    run_time = datetime.now(timezone.utc)
    print("\nGenerating HTML report...")
    html = generate_html(a80_projects, permit_projects, run_time)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Report saved to {OUTPUT_PATH}")

    # Update seen projects
    all_a80_ids = list(seen_a80_ids | {p["id"] for p in a80_projects})
    all_permit_ids = list(seen_permit_ids | {p["id"] for p in permit_projects})
    seen["article80"] = all_a80_ids
    seen["permits"] = all_permit_ids
    seen["last_run"] = run_time.isoformat()
    save_json(SEEN_PATH, seen)
    print("  Updated seen_projects.json")

    print(f"\nDone! Open {OUTPUT_PATH} in a browser to view the report.")


if __name__ == "__main__":
    main()
