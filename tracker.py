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

        website_url = r.get("website_url") or ""
        last_filed = r.get("last_filed_date") or ""
        board_approved = r.get("last_board_approved_date") or ""
        primary_date = board_approved or last_filed

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
            "board_approval_date": board_approved or "N/A",
            "last_filed_date": last_filed or "N/A",
            "website_url": website_url,
            "primary_date": primary_date,
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

        permit_number = r.get("PERMITNUMBER", r.get("permitnumber", ""))
        issued_date = r.get("ISSUED_DATE", r.get("issued_date", ""))

        projects.append({
            "id": str(r.get("_id", permit_number or "")),
            "name": r.get("DESCRIPTION", r.get("description", "Permit")) or "Permit",
            "address": r.get("ADDRESS", r.get("address", "N/A")),
            "neighborhood": r.get("CITY", r.get("city", "Boston")),
            "status": r.get("STATUS", r.get("status", "")) or "Issued",
            "permit_type": r.get("PERMITTYPE", r.get("permittype", "N/A")),
            "permit_number": str(permit_number) if permit_number else "",
            "applicant": r.get("APPLICANT", r.get("applicant", "")) or "",
            "worktype": r.get("WORKTYPE", r.get("worktype", "")) or "",
            "permit_type_descr": r.get("PERMIT_TYPE_DESCR", r.get("permit_type_descr", "")) or "",
            "expiration_date": r.get("EXPIRATION_DATE", r.get("expiration_date", "")) or "",
            "sqft": sqft,
            "valuation": valuation,
            "description": r.get("COMMENTS", r.get("comments", "N/A")),
            "issued_date": issued_date or "N/A",
            "primary_date": issued_date or "",
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
    """Generate mobile-friendly HTML report with search, filters, and sorting."""
    all_projects = list(article80_projects) + list(permit_projects)

    # Sort by relevance (high first) then by valuation descending
    def sort_key(p):
        rel_order = {"high": 0, "medium": 1, "low": 2}
        return (rel_order.get(p.get("hvac_relevance", "low"), 2),
                -(p.get("valuation", 0) or p.get("estimated_valuation", 0)))

    all_projects.sort(key=sort_key)

    summary_new = sum(1 for p in all_projects if p.get("is_new"))
    summary_total = len(all_projects)
    summary_high = sum(1 for p in all_projects if p.get("hvac_relevance") == "high")

    # Collect unique neighborhoods and statuses for filter dropdowns
    neighborhoods = sorted(set(p.get("neighborhood", "N/A") for p in all_projects))
    statuses = sorted(set(p.get("status", "N/A") for p in all_projects))

    def format_date_display(date_str):
        """Format an ISO date string for display."""
        if not date_str:
            return ""
        s = str(date_str)
        if "T" in s:
            s = s.split("T")[0]
        return s

    def render_card(p):
        is_a80 = p.get("source") == "article80"
        source_label = "Article 80" if is_a80 else "Permit"
        source_color = "#3498db" if is_a80 else "#e67e22"
        border_color = "#27ae60" if p.get("is_new") else source_color

        rel = p.get("hvac_relevance", "low")
        rel_colors = {"high": "#c0392b", "medium": "#e67e22", "low": "#7f8c8d"}
        rel_color = rel_colors.get(rel, "#7f8c8d")

        new_badge = ""
        if p.get("is_new"):
            new_badge = '<span class="badge badge-new">NEW</span>'

        # Keywords
        kw_html = ""
        if p.get("keywords_matched"):
            tags = "".join(
                '<span class="kw-tag">{}</span>'.format(escape_html(k))
                for k in set(p["keywords_matched"])
            )
            kw_html = '<div class="card-keywords">Keywords: {}</div>'.format(tags)

        # Date display
        primary_date = p.get("primary_date", "")
        date_display = format_date_display(primary_date)
        date_sortable = date_display  # YYYY-MM-DD already sortable

        # Source-specific fields
        detail_html = ""
        if is_a80:
            val = p.get("estimated_valuation", 0)
            val_html = " &bull; <strong>Est. Value:</strong> {}".format(format_currency(val)) if val > 0 else ""
            detail_html = (
                '<div class="card-detail">'
                '<strong>Status:</strong> {status} &bull; '
                '<strong>Type:</strong> {rtype} &bull; '
                '<strong>Size:</strong> {sqft}{val}'
                '</div>'
                '<div class="card-detail">'
                '<strong>Proposed Use:</strong> {use}'
                '</div>'
            ).format(
                status=escape_html(p.get("status", "N/A")),
                rtype=escape_html(p.get("record_type", "N/A")),
                sqft=format_sqft(p.get("sqft", 0)),
                val=val_html,
                use=escape_html(str(p.get("proposed_use") or "N/A")[:200]),
            )
        else:
            issued = format_date_display(p.get("issued_date", ""))
            permit_num = p.get("permit_number", "")
            applicant = p.get("applicant", "")
            worktype = p.get("worktype", "")
            detail_html = (
                '<div class="card-detail">'
                '<strong>Valuation:</strong> {val} &bull; '
                '<strong>Size:</strong> {sqft} &bull; '
                '<strong>Issued:</strong> {issued}'
                '</div>'
            ).format(
                val=format_currency(p.get("valuation", 0)),
                sqft=format_sqft(p.get("sqft", 0)),
                issued=escape_html(issued) if issued else "N/A",
            )
            if permit_num or applicant or worktype:
                extras = []
                if permit_num:
                    extras.append("<strong>Permit:</strong> {}".format(escape_html(permit_num)))
                if applicant:
                    extras.append("<strong>Applicant:</strong> {}".format(escape_html(applicant)))
                if worktype:
                    extras.append("<strong>Work Type:</strong> {}".format(escape_html(worktype)))
                detail_html += '<div class="card-detail">{}</div>'.format(" &bull; ".join(extras))

        # Description
        desc = str(p.get("description") or "N/A")
        desc_short = desc[:300] + ("..." if len(desc) > 300 else "")

        # Links row
        links = []
        if is_a80 and p.get("website_url"):
            links.append('<a class="link-btn" href="{}" target="_blank" rel="noopener">View Project</a>'.format(
                escape_html(p["website_url"])))
        if not is_a80 and p.get("permit_number"):
            permit_url = "https://data.boston.gov/dataset/approved-building-permits/resource/6ddcd912-32a0-43df-9908-63574f8c7e77?filters=PERMITNUMBER%3A{}".format(
                urllib.parse.quote(str(p["permit_number"])))
            links.append('<a class="link-btn" href="{}" target="_blank" rel="noopener">Permit #{}</a>'.format(
                escape_html(permit_url), escape_html(str(p["permit_number"]))))

        address = p.get("address", "")
        name = p.get("name", "")
        search_q = urllib.parse.quote('"{}" "{}" Boston construction'.format(name[:60], address))
        links.append('<a class="link-btn" href="https://www.google.com/search?q={}" target="_blank" rel="noopener">Search News</a>'.format(search_q))

        if address and address != "N/A":
            map_q = urllib.parse.quote("{}, Boston, MA".format(address))
            links.append('<a class="link-btn" href="https://www.google.com/maps/search/?api=1&query={}" target="_blank" rel="noopener">View on Map</a>'.format(map_q))

        links_html = '<div class="card-links">{}</div>'.format("".join(links)) if links else ""

        # Build search text for data attribute
        search_parts = [
            name or "", address or "",
            str(p.get("description") or ""),
            " ".join(p.get("keywords_matched", [])),
            p.get("neighborhood") or "",
            p.get("applicant") or "",
        ]
        search_text = " ".join(search_parts).lower().replace('"', "&quot;")

        return (
            '<div class="project-card" '
            'data-source="{source}" '
            'data-relevance="{relevance}" '
            'data-neighborhood="{neighborhood}" '
            'data-status="{status}" '
            'data-is-new="{is_new}" '
            'data-date="{date}" '
            'data-search="{search}">'
            '<div class="card-header">'
            '<div class="card-title-row">'
            '<strong class="card-name">{name}</strong>'
            '<div class="card-badges">'
            '<span class="badge badge-source" style="background:{source_color};">{source_label}</span>'
            '<span class="badge" style="background:{rel_color};">{rel_upper}</span>'
            '{new_badge}'
            '</div></div>'
            '<div class="card-sub">{addr} &bull; {hood}'
            '{date_span}'
            '</div></div>'
            '{detail_html}'
            '<div class="card-desc">{desc}</div>'
            '{kw_html}'
            '{links_html}'
            '</div>'
        ).format(
            source=escape_html(p.get("source", "")),
            relevance=escape_html(rel),
            neighborhood=escape_html(p.get("neighborhood", "N/A")),
            status=escape_html(p.get("status", "N/A")),
            is_new="true" if p.get("is_new") else "false",
            date=escape_html(date_sortable),
            search=search_text[:500],
            name=escape_html(name[:120]),
            source_color=source_color,
            source_label=source_label,
            rel_color=rel_color,
            rel_upper=rel.upper(),
            new_badge=new_badge,
            addr=escape_html(address),
            hood=escape_html(p.get("neighborhood", "N/A")),
            date_span=' &bull; <strong>{}</strong>'.format(escape_html(date_display)) if date_display else "",
            detail_html=detail_html,
            desc=escape_html(desc_short),
            kw_html=kw_html,
            links_html=links_html,
        )

    cards_html = "".join(render_card(p) for p in all_projects)

    neighborhood_options = "".join(
        '<option value="{v}">{v}</option>'.format(v=escape_html(n)) for n in neighborhoods
    )
    status_options = "".join(
        '<option value="{v}">{v}</option>'.format(v=escape_html(s)) for s in statuses
    )

    html = """<!DOCTYPE html>
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
            max-width: 900px;
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
            grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
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
        .toolbar {{
            background: #fff;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 16px;
            border: 1px solid #ddd;
        }}
        .search-input {{
            width: 100%;
            padding: 10px 14px;
            font-size: 1em;
            border: 2px solid #ddd;
            border-radius: 6px;
            outline: none;
            margin-bottom: 10px;
        }}
        .search-input:focus {{
            border-color: #3498db;
            box-shadow: 0 0 0 3px rgba(52,152,219,0.15);
        }}
        .filter-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
        }}
        .filter-select {{
            padding: 7px 10px;
            font-size: 0.85em;
            border: 1px solid #ddd;
            border-radius: 6px;
            background: #fff;
            outline: none;
            flex: 1 1 140px;
            min-width: 120px;
        }}
        .filter-select:focus {{
            border-color: #3498db;
        }}
        .filter-count {{
            font-size: 0.85em;
            color: #888;
            margin-top: 8px;
        }}
        .project-card {{
            border: 1px solid #ddd;
            border-left: 4px solid #3498db;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 12px;
            background: #fff;
        }}
        .project-card[data-is-new="true"] {{
            border-left-color: #27ae60;
        }}
        .card-header {{
            margin-bottom: 6px;
        }}
        .card-title-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 6px;
        }}
        .card-name {{
            font-size: 1em;
            flex: 1 1 auto;
            min-width: 0;
            overflow-wrap: break-word;
        }}
        .card-badges {{
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
            flex-shrink: 0;
        }}
        .badge {{
            color: #fff;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.72em;
            font-weight: bold;
            white-space: nowrap;
        }}
        .badge-new {{
            background: #27ae60;
        }}
        .card-sub {{
            color: #666;
            font-size: 0.85em;
            margin-top: 4px;
        }}
        .card-detail {{
            font-size: 0.85em;
            margin-top: 4px;
        }}
        .card-desc {{
            font-size: 0.85em;
            margin-top: 6px;
            color: #444;
        }}
        .card-keywords {{
            margin-top: 6px;
            font-size: 0.85em;
        }}
        .kw-tag {{
            background: #eee;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.85em;
            margin: 2px;
            display: inline-block;
        }}
        .card-links {{
            margin-top: 8px;
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }}
        .link-btn {{
            display: inline-block;
            padding: 4px 10px;
            font-size: 0.78em;
            border: 1px solid #3498db;
            border-radius: 4px;
            color: #3498db;
            text-decoration: none;
            font-weight: 600;
        }}
        .link-btn:hover {{
            background: #3498db;
            color: #fff;
        }}
        .no-results {{
            text-align: center;
            color: #888;
            padding: 32px 12px;
            font-size: 1em;
            display: none;
        }}
        @media (max-width: 600px) {{
            .filter-row {{
                flex-direction: column;
            }}
            .filter-select {{
                flex: 1 1 100%;
            }}
        }}
    </style>
</head>
<body>
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
        <div>
            <h1>Boston HVAC Construction Tracker</h1>
            <p style="color:#888;font-size:0.85em;">
                Large projects ($1M+) with HVAC/pipefitting relevance &bull;
                Updated {run_time} UTC
            </p>
        </div>
        <button id="refreshBtn" onclick="triggerRefresh()" style="padding:8px 16px;font-size:0.85em;font-weight:600;background:#3498db;color:#fff;border:none;border-radius:6px;cursor:pointer;white-space:nowrap;height:fit-content;">Refresh Report</button>
    </div>
    <div id="refreshStatus" style="display:none;margin-top:6px;margin-bottom:8px;padding:8px 12px;border-radius:6px;font-size:0.85em;"></div>

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
                <div class="stat-num">{a80_count}</div>
                <div class="stat-label">Article 80</div>
            </div>
            <div class="stat">
                <div class="stat-num">{permit_count}</div>
                <div class="stat-label">Permits</div>
            </div>
            <div class="stat">
                <div class="stat-num">{high_count}</div>
                <div class="stat-label">High Relevance</div>
            </div>
        </div>
    </div>

    <div class="toolbar">
        <input type="text" id="searchInput" class="search-input" placeholder="Search by name, address, description, keyword, neighborhood, applicant...">
        <div class="filter-row">
            <select id="filterSource" class="filter-select">
                <option value="">All Sources</option>
                <option value="article80">Article 80</option>
                <option value="permits">Permits</option>
            </select>
            <select id="filterRelevance" class="filter-select">
                <option value="">All Relevance</option>
                <option value="high">High</option>
                <option value="medium">Medium</option>
                <option value="low">Low</option>
            </select>
            <select id="filterNeighborhood" class="filter-select">
                <option value="">All Neighborhoods</option>
                {neighborhood_options}
            </select>
            <select id="filterStatus" class="filter-select">
                <option value="">All Statuses</option>
                {status_options}
            </select>
            <select id="sortOrder" class="filter-select">
                <option value="default">Sort: Default</option>
                <option value="newest">Sort: Newest First</option>
                <option value="oldest">Sort: Oldest First</option>
            </select>
        </div>
        <div class="filter-count" id="filterCount"></div>
    </div>

    <div id="cardContainer">
        {cards_html}
    </div>
    <div class="no-results" id="noResults">No projects match your filters.</div>

    <footer style="margin-top:24px;padding-top:12px;border-top:1px solid #ddd;color:#aaa;font-size:0.75em;text-align:center;">
        Data from <a href="https://data.boston.gov" style="color:#aaa;">data.boston.gov</a> &bull;
        Article 80 Development Projects &bull; Approved Building Permits
    </footer>

    <script>
    (function() {{
        var cards = [];
        var container = document.getElementById('cardContainer');
        var noResults = document.getElementById('noResults');
        var countEl = document.getElementById('filterCount');
        var searchInput = document.getElementById('searchInput');
        var filterSource = document.getElementById('filterSource');
        var filterRelevance = document.getElementById('filterRelevance');
        var filterNeighborhood = document.getElementById('filterNeighborhood');
        var filterStatus = document.getElementById('filterStatus');
        var sortOrder = document.getElementById('sortOrder');
        var debounceTimer = null;

        // Collect all cards
        var els = container.getElementsByClassName('project-card');
        for (var i = 0; i < els.length; i++) {{
            cards.push(els[i]);
        }}
        var total = cards.length;

        function applyFilters() {{
            var q = searchInput.value.toLowerCase().trim();
            var src = filterSource.value;
            var rel = filterRelevance.value;
            var hood = filterNeighborhood.value;
            var stat = filterStatus.value;
            var shown = 0;

            for (var i = 0; i < cards.length; i++) {{
                var c = cards[i];
                var visible = true;
                if (src && c.getAttribute('data-source') !== src) visible = false;
                if (rel && c.getAttribute('data-relevance') !== rel) visible = false;
                if (hood && c.getAttribute('data-neighborhood') !== hood) visible = false;
                if (stat && c.getAttribute('data-status') !== stat) visible = false;
                if (q && c.getAttribute('data-search').indexOf(q) === -1) visible = false;
                c.style.display = visible ? '' : 'none';
                if (visible) shown++;
            }}

            countEl.textContent = 'Showing ' + shown + ' of ' + total + ' projects';
            noResults.style.display = (shown === 0) ? 'block' : 'none';
        }}

        function applySort() {{
            var order = sortOrder.value;
            if (order === 'default') return;

            var sorted = cards.slice().sort(function(a, b) {{
                var da = a.getAttribute('data-date') || '';
                var db = b.getAttribute('data-date') || '';
                if (order === 'newest') return da < db ? 1 : (da > db ? -1 : 0);
                return da > db ? 1 : (da < db ? -1 : 0);
            }});

            for (var i = 0; i < sorted.length; i++) {{
                container.appendChild(sorted[i]);
            }}
        }}

        function update() {{
            applySort();
            applyFilters();
        }}

        searchInput.addEventListener('input', function() {{
            if (debounceTimer) clearTimeout(debounceTimer);
            debounceTimer = setTimeout(update, 200);
        }});

        filterSource.addEventListener('change', update);
        filterRelevance.addEventListener('change', update);
        filterNeighborhood.addEventListener('change', update);
        filterStatus.addEventListener('change', update);
        sortOrder.addEventListener('change', update);

        // Initial count
        applyFilters();
    }})();

    function triggerRefresh() {{
        var btn = document.getElementById('refreshBtn');
        var status = document.getElementById('refreshStatus');
        var token = localStorage.getItem('gh_pat');
        if (!token) {{
            token = prompt('Enter your GitHub Personal Access Token (needs workflow scope).\\nThis will be saved in your browser for future use.');
            if (!token) return;
            localStorage.setItem('gh_pat', token);
        }}
        btn.disabled = true;
        btn.textContent = 'Triggering...';
        btn.style.background = '#7f8c8d';
        status.style.display = 'block';
        status.style.background = '#eaf4fd';
        status.style.color = '#3498db';
        status.textContent = 'Triggering workflow... The report will update in a few minutes.';

        fetch('https://api.github.com/repos/kwamAG/boston_construction_tracker/actions/workflows/weekly.yml/dispatches', {{
            method: 'POST',
            headers: {{
                'Authorization': 'Bearer ' + token,
                'Accept': 'application/vnd.github.v3+json',
                'Content-Type': 'application/json'
            }},
            body: JSON.stringify({{ ref: 'main' }})
        }}).then(function(r) {{
            if (r.status === 204) {{
                status.style.background = '#eafaf1';
                status.style.color = '#27ae60';
                status.textContent = 'Workflow triggered! The report will refresh automatically in a few minutes. Reload this page after that to see updated data.';
            }} else if (r.status === 401 || r.status === 403) {{
                localStorage.removeItem('gh_pat');
                status.style.background = '#fdecea';
                status.style.color = '#c0392b';
                status.textContent = 'Invalid or expired token. Click Refresh again to enter a new one.';
            }} else {{
                status.style.background = '#fdecea';
                status.style.color = '#c0392b';
                status.textContent = 'Error: HTTP ' + r.status + '. Check your token permissions.';
            }}
            btn.disabled = false;
            btn.textContent = 'Refresh Report';
            btn.style.background = '#3498db';
        }}).catch(function(e) {{
            status.style.background = '#fdecea';
            status.style.color = '#c0392b';
            status.textContent = 'Network error: ' + e.message;
            btn.disabled = false;
            btn.textContent = 'Refresh Report';
            btn.style.background = '#3498db';
        }});
    }}
    </script>
</body>
</html>""".format(
        run_time=run_time.strftime('%B %d, %Y at %I:%M %p'),
        summary_new=summary_new,
        summary_total=summary_total,
        a80_count=len(article80_projects),
        permit_count=len(permit_projects),
        high_count=summary_high,
        neighborhood_options=neighborhood_options,
        status_options=status_options,
        cards_html=cards_html,
    )
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
