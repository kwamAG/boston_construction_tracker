#!/usr/bin/env python3
"""Boston Large HVAC Construction Projects Tracker.

Queries Boston open data APIs for large construction projects ($1M+) with
HVAC/pipefitting relevance and generates a mobile-friendly HTML report.
Uses only Python standard library (no pip installs).
"""

import json
import os
import re
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

BRJP_SQL_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"


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


def api_fetch_sql(sql):
    """Fetch records from Boston CKAN datastore SQL API."""
    params = urllib.parse.urlencode({"sql": sql})
    url = f"{BRJP_SQL_URL}?{params}"
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "BostonHVACTracker/1.0"})
    with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not data.get("success"):
        raise RuntimeError(f"SQL API error: {data}")
    return data["result"].get("records", [])


def build_brjp_queries(resource_id, pipefitter_trades):
    """Build the 3 SQL queries for BRJP data.

    worker_hours_this_period is stored as text in the datastore,
    so all SUMs must CAST to NUMERIC first.
    """
    hrs = 'CAST("worker_hours_this_period" AS NUMERIC)'

    # Query 1: Per-project compliance summary with status/detail fields
    q1 = (
        'SELECT "agency", "compliance_project_name", "project_address", '
        'SUM({hrs}) AS total_hours, '
        'SUM(CASE WHEN "boston_resident" = \'t\' THEN {hrs} ELSE 0 END) AS resident_hours, '
        'SUM(CASE WHEN "person_of_color" = \'t\' THEN {hrs} ELSE 0 END) AS poc_hours, '
        'SUM(CASE WHEN "gender" = \'Woman\' THEN {hrs} ELSE 0 END) AS women_hours, '
        'MAX("period_ending") AS last_period, '
        'MAX("neighborhood") AS neighborhood, '
        'MAX("developer") AS developer, '
        'MAX("general_contractor_name") AS general_contractor '
        'FROM "{rid}" '
        'GROUP BY "agency", "compliance_project_name", "project_address"'
    ).format(hrs=hrs, rid=resource_id)

    # Query 2: Pipefitter trades per project
    trades_in = ", ".join("'{}'".format(t.replace("'", "''")) for t in pipefitter_trades)
    q2 = (
        'SELECT "compliance_project_name", "project_address", "trade", '
        'SUM({hrs}) AS trade_hours '
        'FROM "{rid}" '
        'WHERE "trade" IN ({trades}) '
        'GROUP BY "compliance_project_name", "project_address", "trade"'
    ).format(hrs=hrs, rid=resource_id, trades=trades_in)

    # Query 3: Global trade summary
    q3 = (
        'SELECT "trade", '
        'SUM({hrs}) AS total_hours, '
        'COUNT(DISTINCT "compliance_project_name") AS project_count, '
        'SUM(CASE WHEN "boston_resident" = \'t\' THEN {hrs} ELSE 0 END) AS resident_hours, '
        'SUM(CASE WHEN "person_of_color" = \'t\' THEN {hrs} ELSE 0 END) AS poc_hours, '
        'SUM(CASE WHEN "gender" = \'Woman\' THEN {hrs} ELSE 0 END) AS women_hours '
        'FROM "{rid}" '
        'GROUP BY "trade"'
    ).format(hrs=hrs, rid=resource_id)

    return q1, q2, q3


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


def normalize_address(addr):
    """Normalize an address string for fuzzy matching."""
    if not addr:
        return ""
    s = str(addr).upper().strip()
    # Remove unit/suite/apt suffixes
    s = re.sub(r'\s*(UNIT|SUITE|STE|APT|#)\s*\S*', '', s)
    # Standardize common abbreviations
    replacements = [
        (r'\bSTREET\b', 'ST'), (r'\bAVENUE\b', 'AVE'), (r'\bROAD\b', 'RD'),
        (r'\bDRIVE\b', 'DR'), (r'\bBOULEVARD\b', 'BLVD'), (r'\bLANE\b', 'LN'),
        (r'\bPLACE\b', 'PL'), (r'\bCOURT\b', 'CT'), (r'\bCIRCLE\b', 'CIR'),
        (r'\bHIGHWAY\b', 'HWY'), (r'\bPARKWAY\b', 'PKWY'), (r'\bTERRACE\b', 'TER'),
        (r'\bSOUTH\b', 'S'), (r'\bNORTH\b', 'N'), (r'\bEAST\b', 'E'), (r'\bWEST\b', 'W'),
    ]
    for pat, repl in replacements:
        s = re.sub(pat, repl, s)
    # Remove punctuation, collapse whitespace
    s = re.sub(r'[.,\-#/]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def process_brjp_projects(project_rows, targets):
    """Process raw BRJP per-project rows into compliance dicts."""
    projects = {}
    for r in project_rows:
        name = str(r.get("compliance_project_name") or "").strip()
        address = str(r.get("project_address") or "").strip()
        key = (name, address)

        total = float(r.get("total_hours") or 0)
        resident = float(r.get("resident_hours") or 0)
        poc = float(r.get("poc_hours") or 0)
        women = float(r.get("women_hours") or 0)
        agency = str(r.get("agency") or "").strip().upper()
        last_period = str(r.get("last_period") or "").strip()
        neighborhood = str(r.get("neighborhood") or "").strip()
        developer = str(r.get("developer") or "").strip()
        contractor = str(r.get("general_contractor") or "").strip()

        if key in projects:
            p = projects[key]
            p["total_hours"] += total
            p["resident_hours"] += resident
            p["poc_hours"] += poc
            p["women_hours"] += women
            if agency and agency not in p["agencies"]:
                p["agencies"].append(agency)
            # Keep the most recent period and non-empty detail fields
            if last_period > p.get("last_period", ""):
                p["last_period"] = last_period
            if neighborhood and not p.get("neighborhood"):
                p["neighborhood"] = neighborhood
            if developer and not p.get("developer"):
                p["developer"] = developer
            if contractor and not p.get("general_contractor"):
                p["general_contractor"] = contractor
        else:
            projects[key] = {
                "name": name,
                "address": address,
                "total_hours": total,
                "resident_hours": resident,
                "poc_hours": poc,
                "women_hours": women,
                "agencies": [agency] if agency else [],
                "norm_address": normalize_address(address),
                "last_period": last_period,
                "neighborhood": neighborhood,
                "developer": developer,
                "general_contractor": contractor,
            }

    # Determine cutoff for "Active" status: 6 months ago
    now = datetime.now(timezone.utc)
    cutoff = now.strftime("%Y-%m-%d")
    # ~6 months back
    cutoff_year = now.year if now.month > 6 else now.year - 1
    cutoff_month = now.month - 6 if now.month > 6 else now.month + 6
    cutoff = f"{cutoff_year}-{cutoff_month:02d}-01"

    # Compute percentages and compliance
    for p in projects.values():
        th = p["total_hours"]
        if th > 0:
            p["resident_pct"] = (p["resident_hours"] / th) * 100
            p["poc_pct"] = (p["poc_hours"] / th) * 100
            p["women_pct"] = (p["women_hours"] / th) * 100
        else:
            p["resident_pct"] = p["poc_pct"] = p["women_pct"] = 0.0

        meets_resident = p["resident_pct"] >= targets["resident_pct"]
        meets_poc = p["poc_pct"] >= targets["poc_pct"]
        meets_women = p["women_pct"] >= targets["women_pct"]

        if meets_resident and meets_poc and meets_women:
            p["compliance_status"] = "compliant"
        elif meets_resident or meets_poc or meets_women:
            p["compliance_status"] = "partial"
        else:
            p["compliance_status"] = "non-compliant"

        p["is_oed"] = "OED" in p["agencies"]
        p["is_bpda"] = "BPDA" in p["agencies"]

        # Active = reported hours within last 6 months
        lp = p.get("last_period", "")
        if lp and lp >= cutoff:
            p["project_status"] = "active"
        else:
            p["project_status"] = "completed"

    return projects


def process_pipefitter_by_project(pipe_rows):
    """Process pipefitter trade rows into per-project trade breakdown."""
    result = {}
    for r in pipe_rows:
        name = str(r.get("compliance_project_name") or "").strip()
        address = str(r.get("project_address") or "").strip()
        trade = str(r.get("trade") or "").strip()
        hours = float(r.get("trade_hours") or 0)
        key = (name, address)
        if key not in result:
            result[key] = {"name": name, "address": address, "trades": {}}
        result[key]["trades"][trade] = result[key]["trades"].get(trade, 0) + hours
    return result


def process_global_trades(trade_rows):
    """Process global trade summary rows."""
    trades = []
    for r in trade_rows:
        th = float(r.get("total_hours") or 0)
        rh = float(r.get("resident_hours") or 0)
        ph = float(r.get("poc_hours") or 0)
        wh = float(r.get("women_hours") or 0)
        trades.append({
            "trade": str(r.get("trade") or "").strip(),
            "total_hours": th,
            "project_count": int(r.get("project_count") or 0),
            "resident_pct": (rh / th * 100) if th > 0 else 0,
            "poc_pct": (ph / th * 100) if th > 0 else 0,
            "women_pct": (wh / th * 100) if th > 0 else 0,
        })
    trades.sort(key=lambda t: t["total_hours"], reverse=True)
    return trades


def match_brjp_to_projects(tracker_projects, brjp_projects):
    """Match BRJP data to existing Article 80/permit projects by address."""
    # Build lookup by normalized address
    brjp_by_addr = {}
    for key, bp in brjp_projects.items():
        norm = bp["norm_address"]
        if norm:
            brjp_by_addr[norm] = bp

    matched = 0
    for p in tracker_projects:
        addr = normalize_address(p.get("address", ""))
        if addr and addr in brjp_by_addr:
            bp = brjp_by_addr[addr]
            p["brjp"] = bp
            matched += 1
        else:
            p["brjp"] = None
    return matched


def compute_brjp_aggregates(brjp_projects):
    """Compute city-wide BRJP aggregate stats."""
    total_hours = sum(p["total_hours"] for p in brjp_projects.values())
    total_resident = sum(p["resident_hours"] for p in brjp_projects.values())
    total_poc = sum(p["poc_hours"] for p in brjp_projects.values())
    total_women = sum(p["women_hours"] for p in brjp_projects.values())

    compliant = sum(1 for p in brjp_projects.values() if p["compliance_status"] == "compliant")
    partial = sum(1 for p in brjp_projects.values() if p["compliance_status"] == "partial")
    non_compliant = sum(1 for p in brjp_projects.values() if p["compliance_status"] == "non-compliant")
    oed_count = sum(1 for p in brjp_projects.values() if p["is_oed"])

    return {
        "total_hours": total_hours,
        "resident_pct": (total_resident / total_hours * 100) if total_hours > 0 else 0,
        "poc_pct": (total_poc / total_hours * 100) if total_hours > 0 else 0,
        "women_pct": (total_women / total_hours * 100) if total_hours > 0 else 0,
        "total_projects": len(brjp_projects),
        "compliant": compliant,
        "partial": partial,
        "non_compliant": non_compliant,
        "oed_count": oed_count,
    }


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


def format_hours(val):
    """Format hours with comma separator."""
    if val <= 0:
        return "0"
    return "{:,.0f}".format(val)


def generate_html(article80_projects, permit_projects, run_time,
                  brjp_projects=None, brjp_aggregates=None,
                  pipefitter_by_project=None, global_trades=None,
                  pipefitter_trades=None, brjp_targets=None):
    """Generate mobile-friendly HTML report with tabs, BRJP compliance, and pipefitter stats."""
    brjp_projects = brjp_projects or {}
    brjp_aggregates = brjp_aggregates or {}
    pipefitter_by_project = pipefitter_by_project or {}
    global_trades = global_trades or []
    pipefitter_trades = pipefitter_trades or []
    brjp_targets = brjp_targets or {"resident_pct": 51.0, "poc_pct": 40.0, "women_pct": 12.0}

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
    brjp_tracked = brjp_aggregates.get("total_projects", 0)
    brjp_compliant = brjp_aggregates.get("compliant", 0)
    brjp_oed = brjp_aggregates.get("oed_count", 0)

    # Collect unique neighborhoods and statuses for filter dropdowns
    neighborhoods = sorted(set(p.get("neighborhood", "N/A") for p in all_projects))
    statuses = sorted(set(p.get("status", "N/A") for p in all_projects))

    def format_date_display(date_str):
        if not date_str:
            return ""
        s = str(date_str)
        if "T" in s:
            s = s.split("T")[0]
        return s

    def render_brjp_mini_bars(brjp_data):
        """Render mini BRJP compliance bars for a project card."""
        if not brjp_data:
            return ""
        bars = []
        metrics = [
            ("Residents", brjp_data["resident_pct"], brjp_targets["resident_pct"]),
            ("POC", brjp_data["poc_pct"], brjp_targets["poc_pct"]),
            ("Women", brjp_data["women_pct"], brjp_targets["women_pct"]),
        ]
        for label, pct, target in metrics:
            color = "#27ae60" if pct >= target else "#e74c3c"
            width = min(pct, 100)
            bars.append(
                '<div class="brjp-mini-row">'
                '<span class="brjp-mini-label">{label}</span>'
                '<div class="brjp-mini-track">'
                '<div class="brjp-mini-fill" style="width:{width:.1f}%;background:{color};"></div>'
                '<div class="brjp-mini-target" style="left:{target:.1f}%;"></div>'
                '</div>'
                '<span class="brjp-mini-val" style="color:{color};">{pct:.1f}%</span>'
                '</div>'.format(label=label, width=width, color=color, target=min(target, 100), pct=pct)
            )
        agency_list = brjp_data.get("agencies", [])
        agency_badges = ""
        for ag in agency_list:
            ag_color = "#8e44ad" if ag == "OED" else "#2980b9"
            agency_badges += '<span class="badge" style="background:{c};">{a}</span>'.format(c=ag_color, a=escape_html(ag))

        return (
            '<div class="brjp-mini">'
            '<div class="brjp-mini-header">BRJP Compliance {badges}</div>'
            '{bars}'
            '<div class="brjp-mini-hours">{hours} total hours</div>'
            '</div>'
        ).format(
            badges=agency_badges,
            bars="".join(bars),
            hours=format_hours(brjp_data["total_hours"]),
        )

    def render_card(p):
        is_a80 = p.get("source") == "article80"
        source_label = "Article 80" if is_a80 else "Permit"
        source_color = "#3498db" if is_a80 else "#e67e22"

        rel = p.get("hvac_relevance", "low")
        rel_colors = {"high": "#c0392b", "medium": "#e67e22", "low": "#7f8c8d"}
        rel_color = rel_colors.get(rel, "#7f8c8d")

        new_badge = ""
        if p.get("is_new"):
            new_badge = '<span class="badge badge-new">NEW</span>'

        # BRJP badges
        brjp_data = p.get("brjp")
        brjp_status = ""
        oed_val = ""
        if brjp_data:
            brjp_status = brjp_data.get("compliance_status", "")
            oed_val = "oed" if brjp_data.get("is_oed") else ("bpda" if brjp_data.get("is_bpda") else "")

        # Keywords
        kw_html = ""
        if p.get("keywords_matched"):
            tags = "".join(
                '<span class="kw-tag">{}</span>'.format(escape_html(k))
                for k in set(p["keywords_matched"])
            )
            kw_html = '<div class="card-keywords">Keywords: {}</div>'.format(tags)

        primary_date = p.get("primary_date", "")
        date_display = format_date_display(primary_date)
        date_sortable = date_display

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

        # BRJP mini bars
        brjp_html = render_brjp_mini_bars(brjp_data)

        desc = str(p.get("description") or "N/A")
        desc_short = desc[:300] + ("..." if len(desc) > 300 else "")

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
            'data-brjp-status="{brjp_status}" '
            'data-oed="{oed_val}" '
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
            '{brjp_html}'
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
            brjp_status=escape_html(brjp_status),
            oed_val=escape_html(oed_val),
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
            brjp_html=brjp_html,
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

    # --- Jobs Policy tab: per-project compliance table rows ---
    brjp_table_rows = ""
    for key in sorted(brjp_projects.keys(), key=lambda k: brjp_projects[k]["total_hours"], reverse=True):
        bp = brjp_projects[key]
        comp_colors = {"compliant": "#27ae60", "partial": "#f39c12", "non-compliant": "#e74c3c"}
        comp_label = bp["compliance_status"].replace("-", " ").title()
        comp_color = comp_colors.get(bp["compliance_status"], "#888")
        agencies_str = ", ".join(bp["agencies"]) if bp["agencies"] else "N/A"
        proj_status = bp.get("project_status", "completed")
        status_color = "#27ae60" if proj_status == "active" else "#95a5a6"
        status_label = "Active" if proj_status == "active" else "Completed"
        search_text = "{} {} {} {}".format(bp["name"], bp["address"], agencies_str,
                                           bp.get("developer", "")).lower().replace('"', "&quot;")

        # Build detail panel links
        bp_name_q = urllib.parse.quote('"{}" "{}" Boston construction'.format(
            bp["name"][:60], bp["address"]))
        bp_map_q = urllib.parse.quote("{}, Boston, MA".format(bp["address"])) if bp["address"] else ""
        detail_links = '<a class="link-btn" href="https://www.google.com/search?q={}" target="_blank" rel="noopener">Search News</a>'.format(bp_name_q)
        if bp_map_q:
            detail_links += '<a class="link-btn" href="https://www.google.com/maps/search/?api=1&query={}" target="_blank" rel="noopener">View on Map</a>'.format(bp_map_q)

        # Build detail panel progress bars
        detail_bars = ""
        for lbl, pct, tgt in [("Residents", bp["resident_pct"], brjp_targets["resident_pct"]),
                               ("People of Color", bp["poc_pct"], brjp_targets["poc_pct"]),
                               ("Women", bp["women_pct"], brjp_targets["women_pct"])]:
            bar_color = "#27ae60" if pct >= tgt else "#e74c3c"
            bar_w = min(pct, 100)
            detail_bars += (
                '<div class="brjp-mini-row">'
                '<span class="brjp-mini-label">{lbl}</span>'
                '<div class="brjp-mini-track">'
                '<div class="brjp-mini-fill" style="width:{w:.1f}%;background:{c};"></div>'
                '<div class="brjp-mini-target" style="left:{t:.1f}%;"></div>'
                '</div>'
                '<span class="brjp-mini-val" style="color:{c};">{p:.1f}%</span>'
                '</div>'
            ).format(lbl=lbl, w=bar_w, c=bar_color, t=min(tgt, 100), p=pct)

        # Detail info items
        detail_info = ""
        if bp.get("developer"):
            detail_info += '<div class="detail-info-item"><strong>Developer:</strong> {}</div>'.format(
                escape_html(bp["developer"][:100]))
        if bp.get("general_contractor"):
            detail_info += '<div class="detail-info-item"><strong>General Contractor:</strong> {}</div>'.format(
                escape_html(bp["general_contractor"][:100]))
        if bp.get("neighborhood"):
            detail_info += '<div class="detail-info-item"><strong>Neighborhood:</strong> {}</div>'.format(
                escape_html(bp["neighborhood"][:60]))
        if bp.get("last_period"):
            detail_info += '<div class="detail-info-item"><strong>Last Reported:</strong> {}</div>'.format(
                escape_html(bp["last_period"][:10]))
        detail_info += '<div class="detail-info-item"><strong>Hours:</strong> Resident {rh} &bull; POC {ph} &bull; Women {wh} &bull; Total {th}</div>'.format(
            rh=format_hours(bp["resident_hours"]),
            ph=format_hours(bp["poc_hours"]),
            wh=format_hours(bp["women_hours"]),
            th=format_hours(bp["total_hours"]),
        )

        brjp_table_rows += (
            '<tr class="brjp-row" data-compliance="{comp}" data-agency="{agency}" '
            'data-project-status="{proj_status}" data-search="{search}">'
            '<td class="expandable-cell">{name}<br><small style="color:#888;">{addr}</small></td>'
            '<td style="color:{rc};">{rpct:.1f}%</td>'
            '<td style="color:{pc};">{ppct:.1f}%</td>'
            '<td style="color:{wc};">{wpct:.1f}%</td>'
            '<td>{hours}</td>'
            '<td><span style="color:{comp_color};font-weight:600;">{comp_label}</span></td>'
            '<td>{agencies}</td>'
            '<td><span style="color:{status_color};font-weight:600;">{status_label}</span></td>'
            '</tr>'
            '<tr class="brjp-detail-row">'
            '<td colspan="8"><div class="detail-panel">'
            '{detail_bars}'
            '<div class="detail-info">{detail_info}</div>'
            '<div class="detail-links">{detail_links}</div>'
            '</div></td></tr>'
        ).format(
            comp=escape_html(bp["compliance_status"]),
            agency=escape_html("oed" if bp["is_oed"] else ("bpda" if bp["is_bpda"] else "")),
            proj_status=escape_html(proj_status),
            search=escape_html(search_text[:500]),
            name=escape_html(bp["name"][:80]),
            addr=escape_html(bp["address"][:60]),
            rc="#27ae60" if bp["resident_pct"] >= brjp_targets["resident_pct"] else "#e74c3c",
            rpct=bp["resident_pct"],
            pc="#27ae60" if bp["poc_pct"] >= brjp_targets["poc_pct"] else "#e74c3c",
            ppct=bp["poc_pct"],
            wc="#27ae60" if bp["women_pct"] >= brjp_targets["women_pct"] else "#e74c3c",
            wpct=bp["women_pct"],
            hours=format_hours(bp["total_hours"]),
            comp_color=comp_color,
            comp_label=comp_label,
            agencies=escape_html(agencies_str),
            status_color=status_color,
            status_label=status_label,
            detail_bars=detail_bars,
            detail_info=detail_info,
            detail_links=detail_links,
        )

    # --- Pipefitter Stats tab: trade summary cards ---
    pipe_trade_cards = ""
    pipe_trades_in_data = [t for t in global_trades if t["trade"] in pipefitter_trades]
    for t in pipe_trades_in_data:
        pipe_trade_cards += (
            '<div class="trade-card">'
            '<div class="trade-card-name">{trade}</div>'
            '<div class="trade-card-hours">{hours} hrs</div>'
            '<div class="trade-card-projects">{pc} projects</div>'
            '<div class="trade-card-stats">'
            '<span style="color:{rc};">Res {rpct:.0f}%</span> &bull; '
            '<span style="color:{poc};">POC {ppct:.0f}%</span> &bull; '
            '<span style="color:{wc};">Women {wpct:.0f}%</span>'
            '</div></div>'
        ).format(
            trade=escape_html(t["trade"]),
            hours=format_hours(t["total_hours"]),
            pc=t["project_count"],
            rc="#27ae60" if t["resident_pct"] >= brjp_targets["resident_pct"] else "#e74c3c",
            rpct=t["resident_pct"],
            poc="#27ae60" if t["poc_pct"] >= brjp_targets["poc_pct"] else "#e74c3c",
            ppct=t["poc_pct"],
            wc="#27ae60" if t["women_pct"] >= brjp_targets["women_pct"] else "#e74c3c",
            wpct=t["women_pct"],
        )

    # --- Pipefitter per-project table rows ---
    pipe_table_rows = ""
    all_pipe_trades = sorted(set(
        trade for pdata in pipefitter_by_project.values() for trade in pdata["trades"]
    ))
    pipe_trade_options = "".join(
        '<option value="{v}">{v}</option>'.format(v=escape_html(t)) for t in all_pipe_trades
    )

    # Lookup BRJP project status for pipefitter rows
    brjp_by_name_addr = {}
    for bk, bp in brjp_projects.items():
        brjp_by_name_addr[(bp["name"], bp["address"])] = bp

    for key in sorted(pipefitter_by_project.keys(),
                      key=lambda k: sum(pipefitter_by_project[k]["trades"].values()), reverse=True):
        pd = pipefitter_by_project[key]
        total_pipe_hours = sum(pd["trades"].values())
        bp_match = brjp_by_name_addr.get((pd["name"], pd["address"]))
        proj_status = bp_match.get("project_status", "completed") if bp_match else "completed"

        # Build detail panel for this project
        pd_name_q = urllib.parse.quote('"{}" "{}" Boston construction'.format(
            pd["name"][:60], pd["address"]))
        pd_map_q = urllib.parse.quote("{}, Boston, MA".format(pd["address"])) if pd["address"] else ""
        pipe_detail_links = '<a class="link-btn" href="https://www.google.com/search?q={}" target="_blank" rel="noopener">Search News</a>'.format(pd_name_q)
        if pd_map_q:
            pipe_detail_links += '<a class="link-btn" href="https://www.google.com/maps/search/?api=1&query={}" target="_blank" rel="noopener">View on Map</a>'.format(pd_map_q)

        # All trades breakdown for detail
        trades_breakdown = ""
        for t_name, t_hrs in sorted(pd["trades"].items(), key=lambda x: x[1], reverse=True):
            pct_of_total = (t_hrs / total_pipe_hours * 100) if total_pipe_hours > 0 else 0
            trades_breakdown += (
                '<div class="trade-breakdown-row">'
                '<span class="trade-breakdown-name">{name}</span>'
                '<div class="brjp-mini-track">'
                '<div class="brjp-mini-fill" style="width:{pct:.1f}%;background:#3498db;"></div>'
                '</div>'
                '<span class="trade-breakdown-hrs">{hrs} hrs</span>'
                '</div>'
            ).format(name=escape_html(t_name), pct=min(pct_of_total, 100), hrs=format_hours(t_hrs))

        # BRJP compliance info if available
        pipe_detail_compliance = ""
        if bp_match:
            pipe_detail_compliance += '<div class="detail-info-item"><strong>BRJP Status:</strong> {}</div>'.format(
                escape_html(bp_match["compliance_status"].replace("-", " ").title()))
            if bp_match.get("developer"):
                pipe_detail_compliance += '<div class="detail-info-item"><strong>Developer:</strong> {}</div>'.format(
                    escape_html(bp_match["developer"][:100]))
            if bp_match.get("general_contractor"):
                pipe_detail_compliance += '<div class="detail-info-item"><strong>General Contractor:</strong> {}</div>'.format(
                    escape_html(bp_match["general_contractor"][:100]))

        for trade, hours in sorted(pd["trades"].items(), key=lambda x: x[1], reverse=True):
            search_text = "{} {} {}".format(pd["name"], pd["address"], trade).lower().replace('"', "&quot;")
            pipe_table_rows += (
                '<tr class="pipe-row" data-trade="{trade_val}" data-project-status="{proj_status}" data-search="{search}">'
                '<td class="expandable-cell">{name}<br><small style="color:#888;">{addr}</small></td>'
                '<td>{trade}</td>'
                '<td>{hours}</td>'
                '<td><span style="color:{status_color};font-weight:600;">{status_label}</span></td>'
                '</tr>'
                '<tr class="pipe-detail-row">'
                '<td colspan="4"><div class="detail-panel">'
                '<div class="detail-subtitle">All Pipefitter Trades for this Project</div>'
                '{trades_breakdown}'
                '<div class="detail-info">{pipe_compliance}</div>'
                '<div class="detail-links">{pipe_links}</div>'
                '</div></td></tr>'
            ).format(
                trade_val=escape_html(trade),
                proj_status=escape_html(proj_status),
                search=escape_html(search_text[:500]),
                name=escape_html(pd["name"][:80]),
                addr=escape_html(pd["address"][:60]),
                trade=escape_html(trade),
                hours=format_hours(hours),
                status_color="#27ae60" if proj_status == "active" else "#95a5a6",
                status_label="Active" if proj_status == "active" else "Completed",
                trades_breakdown=trades_breakdown,
                pipe_compliance=pipe_detail_compliance,
                pipe_links=pipe_detail_links,
            )

    # --- Gauge helper ---
    def gauge_html(label, pct, target):
        color = "#27ae60" if pct >= target else "#e74c3c"
        deg = min(pct / 100 * 360, 360)
        return (
            '<div class="gauge-wrap">'
            '<div class="gauge" style="background:conic-gradient({color} 0deg {deg:.1f}deg, #eee {deg:.1f}deg 360deg);">'
            '<div class="gauge-inner">'
            '<div class="gauge-pct" style="color:{color};">{pct:.1f}%</div>'
            '<div class="gauge-target">Target: {target:.0f}%</div>'
            '</div></div>'
            '<div class="gauge-label">{label}</div>'
            '</div>'
        ).format(color=color, deg=deg, pct=pct, target=target, label=label)

    gauges_html = ""
    if brjp_aggregates:
        gauges_html = (
            gauge_html("Boston Residents", brjp_aggregates.get("resident_pct", 0), brjp_targets["resident_pct"])
            + gauge_html("People of Color", brjp_aggregates.get("poc_pct", 0), brjp_targets["poc_pct"])
            + gauge_html("Women", brjp_aggregates.get("women_pct", 0), brjp_targets["women_pct"])
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
            grid-template-columns: repeat(auto-fit, minmax(90px, 1fr));
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
        .stat-label {{ font-size: 0.7em; color: #888; }}

        /* Tabs */
        .tab-bar {{
            display: flex;
            gap: 0;
            background: #fff;
            border-radius: 8px 8px 0 0;
            border: 1px solid #ddd;
            border-bottom: none;
            margin-bottom: 0;
            overflow-x: auto;
        }}
        .tab-btn {{
            flex: 1;
            padding: 10px 16px;
            border: none;
            background: #f5f5f5;
            cursor: pointer;
            font-size: 0.9em;
            font-weight: 600;
            color: #666;
            border-bottom: 3px solid transparent;
            white-space: nowrap;
        }}
        .tab-btn.active {{
            background: #fff;
            color: #2c3e50;
            border-bottom-color: #3498db;
        }}
        .tab-btn:hover {{ background: #eef; }}
        .tab-content {{
            display: none;
            background: #fff;
            border: 1px solid #ddd;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 16px;
            margin-bottom: 16px;
        }}
        .tab-content.active {{ display: block; }}

        .toolbar {{
            background: #f9f9f9;
            border-radius: 8px;
            padding: 12px;
            margin-bottom: 16px;
            border: 1px solid #eee;
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
            flex: 1 1 130px;
            min-width: 110px;
        }}
        .filter-select:focus {{ border-color: #3498db; }}
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
        .project-card[data-is-new="true"] {{ border-left-color: #27ae60; }}
        .card-header {{ margin-bottom: 6px; }}
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
        .card-badges {{ display: flex; gap: 4px; flex-wrap: wrap; flex-shrink: 0; }}
        .badge {{
            color: #fff;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.72em;
            font-weight: bold;
            white-space: nowrap;
        }}
        .badge-new {{ background: #27ae60; }}
        .card-sub {{ color: #666; font-size: 0.85em; margin-top: 4px; }}
        .card-detail {{ font-size: 0.85em; margin-top: 4px; }}
        .card-desc {{ font-size: 0.85em; margin-top: 6px; color: #444; }}
        .card-keywords {{ margin-top: 6px; font-size: 0.85em; }}
        .kw-tag {{
            background: #eee; padding: 2px 6px; border-radius: 4px;
            font-size: 0.85em; margin: 2px; display: inline-block;
        }}
        .card-links {{ margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }}
        .link-btn {{
            display: inline-block; padding: 4px 10px; font-size: 0.78em;
            border: 1px solid #3498db; border-radius: 4px; color: #3498db;
            text-decoration: none; font-weight: 600;
        }}
        .link-btn:hover {{ background: #3498db; color: #fff; }}
        .no-results {{
            text-align: center; color: #888; padding: 32px 12px;
            font-size: 1em; display: none;
        }}

        /* BRJP mini bars on project cards */
        .brjp-mini {{
            margin-top: 8px; padding: 8px; background: #f8f9fa;
            border-radius: 6px; border: 1px solid #eee;
        }}
        .brjp-mini-header {{
            font-size: 0.78em; font-weight: 700; margin-bottom: 4px;
            display: flex; align-items: center; gap: 6px;
        }}
        .brjp-mini-row {{ display: flex; align-items: center; gap: 6px; margin: 2px 0; }}
        .brjp-mini-label {{ font-size: 0.72em; width: 60px; color: #666; }}
        .brjp-mini-track {{
            flex: 1; height: 8px; background: #eee; border-radius: 4px;
            position: relative; overflow: visible;
        }}
        .brjp-mini-fill {{ height: 100%; border-radius: 4px; }}
        .brjp-mini-target {{
            position: absolute; top: -2px; width: 2px; height: 12px;
            background: #333; border-radius: 1px;
        }}
        .brjp-mini-val {{ font-size: 0.72em; width: 45px; text-align: right; font-weight: 600; }}
        .brjp-mini-hours {{ font-size: 0.7em; color: #999; margin-top: 2px; }}

        /* Gauges */
        .gauge-row {{ display: flex; justify-content: center; gap: 24px; flex-wrap: wrap; margin: 16px 0; }}
        .gauge-wrap {{ text-align: center; }}
        .gauge {{
            width: 120px; height: 120px; border-radius: 50%;
            display: flex; align-items: center; justify-content: center;
            margin: 0 auto;
        }}
        .gauge-inner {{
            width: 88px; height: 88px; border-radius: 50%; background: #fff;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
        }}
        .gauge-pct {{ font-size: 1.3em; font-weight: 700; }}
        .gauge-target {{ font-size: 0.65em; color: #888; }}
        .gauge-label {{ font-size: 0.85em; font-weight: 600; margin-top: 6px; }}

        /* Info box */
        .info-box {{
            background: #eaf4fc; border: 1px solid #b8d4e8; border-radius: 6px;
            padding: 10px 14px; font-size: 0.85em; margin-bottom: 16px; color: #2c5777;
        }}

        /* Tables */
        .data-table {{
            width: 100%; border-collapse: collapse; font-size: 0.82em; margin-top: 12px;
        }}
        .data-table th {{
            background: #f5f5f5; padding: 8px 6px; text-align: left;
            border-bottom: 2px solid #ddd; font-weight: 600; white-space: nowrap;
        }}
        .data-table td {{ padding: 8px 6px; border-bottom: 1px solid #eee; }}
        .data-table tr:hover {{ background: #f9f9f9; }}
        .table-wrap {{ overflow-x: auto; }}

        /* Trade cards */
        .trade-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0; }}
        .trade-card {{
            background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 12px; text-align: center;
        }}
        .trade-card-name {{ font-weight: 700; font-size: 0.95em; }}
        .trade-card-hours {{ font-size: 1.2em; font-weight: 700; color: #2c3e50; margin: 4px 0; }}
        .trade-card-projects {{ font-size: 0.75em; color: #888; }}
        .trade-card-stats {{ font-size: 0.72em; margin-top: 6px; }}

        /* Expandable detail rows */
        .brjp-row, .pipe-row {{ cursor: pointer; }}
        .brjp-row:hover, .pipe-row:hover {{ background: #eef5ff; }}
        .expandable-cell {{ position: relative; padding-left: 20px !important; }}
        .expandable-cell::before {{
            content: "\\25B6";
            position: absolute; left: 4px; top: 8px;
            font-size: 0.65em; color: #999;
            transition: transform 0.2s;
        }}
        .brjp-row.open .expandable-cell::before,
        .pipe-row.open .expandable-cell::before {{
            transform: rotate(90deg);
        }}
        .brjp-detail-row, .pipe-detail-row {{ display: none; }}
        .brjp-detail-row.open, .pipe-detail-row.open {{ display: table-row; }}
        .detail-panel {{
            padding: 12px; background: #f8f9fa; border-radius: 6px;
            border: 1px solid #eee;
        }}
        .detail-info {{ margin: 8px 0; }}
        .detail-info-item {{ font-size: 0.82em; margin: 3px 0; }}
        .detail-links {{ margin-top: 10px; display: flex; flex-wrap: wrap; gap: 6px; }}
        .detail-subtitle {{
            font-size: 0.82em; font-weight: 700; margin-bottom: 6px; color: #2c3e50;
        }}
        .trade-breakdown-row {{
            display: flex; align-items: center; gap: 6px; margin: 3px 0;
        }}
        .trade-breakdown-name {{ font-size: 0.78em; width: 110px; color: #555; }}
        .trade-breakdown-hrs {{ font-size: 0.78em; width: 80px; text-align: right; font-weight: 600; color: #2c3e50; }}

        @media (max-width: 600px) {{
            .filter-row {{ flex-direction: column; }}
            .filter-select {{ flex: 1 1 100%; }}
            .gauge {{ width: 100px; height: 100px; }}
            .gauge-inner {{ width: 72px; height: 72px; }}
            .gauge-pct {{ font-size: 1.1em; }}
        }}
    </style>
</head>
<body>
    <h1>Boston HVAC Construction Tracker</h1>
    <p style="color:#888;font-size:0.85em;margin-bottom:12px;">
        Large projects ($1M+) with HVAC/pipefitting relevance &bull;
        Updated {run_time} UTC
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
            <div class="stat">
                <div class="stat-num">{brjp_tracked}</div>
                <div class="stat-label">BRJP Tracked</div>
            </div>
            <div class="stat">
                <div class="stat-num">{brjp_compliant}</div>
                <div class="stat-label">Fully Compliant</div>
            </div>
            <div class="stat">
                <div class="stat-num">{brjp_oed}</div>
                <div class="stat-label">OED Projects</div>
            </div>
        </div>
    </div>

    <!-- Tab bar -->
    <div class="tab-bar">
        <button class="tab-btn active" data-tab="projects">Projects</button>
        <button class="tab-btn" data-tab="jobspolicy">Jobs Policy</button>
        <button class="tab-btn" data-tab="pipefitter">Pipefitter Stats</button>
    </div>

    <!-- === Projects Tab === -->
    <div class="tab-content active" id="tab-projects">
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
                <select id="filterBrjp" class="filter-select">
                    <option value="">All BRJP Status</option>
                    <option value="compliant">Fully Compliant</option>
                    <option value="partial">Partial</option>
                    <option value="non-compliant">Non-Compliant</option>
                </select>
                <select id="filterOed" class="filter-select">
                    <option value="">All Agencies</option>
                    <option value="oed">OED</option>
                    <option value="bpda">BPDA</option>
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
    </div>

    <!-- === Jobs Policy Tab === -->
    <div class="tab-content" id="tab-jobspolicy">
        <h2 style="font-size:1.1em;margin-bottom:12px;">Boston Residents Jobs Policy (BRJP) Compliance</h2>
        <div class="info-box">
            <strong>BRJP Requirements:</strong>
            Boston Residents &ge; {target_resident:.0f}% &bull;
            People of Color &ge; {target_poc:.0f}% &bull;
            Women &ge; {target_women:.0f}%
            (measured by worker hours)
        </div>
        <div class="gauge-row">
            {gauges_html}
        </div>
        <div class="toolbar">
            <input type="text" id="brjpSearch" class="search-input" placeholder="Search BRJP projects by name or address...">
            <div class="filter-row">
                <select id="brjpFilterCompliance" class="filter-select">
                    <option value="">All Compliance</option>
                    <option value="compliant">Fully Compliant</option>
                    <option value="partial">Partial</option>
                    <option value="non-compliant">Non-Compliant</option>
                </select>
                <select id="brjpFilterAgency" class="filter-select">
                    <option value="">All Agencies</option>
                    <option value="oed">OED</option>
                    <option value="bpda">BPDA</option>
                </select>
                <select id="brjpFilterStatus" class="filter-select">
                    <option value="active" selected>Active Projects</option>
                    <option value="">All Projects</option>
                    <option value="completed">Completed</option>
                </select>
            </div>
            <div class="filter-count" id="brjpFilterCount"></div>
        </div>
        <div class="table-wrap">
            <table class="data-table" id="brjpTable">
                <thead>
                    <tr>
                        <th>Project</th>
                        <th>Residents %</th>
                        <th>POC %</th>
                        <th>Women %</th>
                        <th>Total Hours</th>
                        <th>Compliance</th>
                        <th>Agency</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {brjp_table_rows}
                </tbody>
            </table>
        </div>
    </div>

    <!-- === Pipefitter Stats Tab === -->
    <div class="tab-content" id="tab-pipefitter">
        <h2 style="font-size:1.1em;margin-bottom:12px;">Pipefitter &amp; Related Trade Statistics</h2>
        <div class="trade-cards">
            {pipe_trade_cards}
        </div>
        <div class="toolbar">
            <input type="text" id="pipeSearch" class="search-input" placeholder="Search pipefitter projects by name or address...">
            <div class="filter-row">
                <select id="pipeFilterTrade" class="filter-select">
                    <option value="">All Trades</option>
                    {pipe_trade_options}
                </select>
                <select id="pipeFilterStatus" class="filter-select">
                    <option value="active" selected>Active Projects</option>
                    <option value="">All Projects</option>
                    <option value="completed">Completed</option>
                </select>
            </div>
            <div class="filter-count" id="pipeFilterCount"></div>
        </div>
        <div class="table-wrap">
            <table class="data-table" id="pipeTable">
                <thead>
                    <tr>
                        <th>Project</th>
                        <th>Trade</th>
                        <th>Hours</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {pipe_table_rows}
                </tbody>
            </table>
        </div>
    </div>

    <footer style="margin-top:24px;padding-top:12px;border-top:1px solid #ddd;color:#aaa;font-size:0.75em;text-align:center;">
        Data from <a href="https://data.boston.gov" style="color:#aaa;">data.boston.gov</a> &bull;
        Article 80 Development Projects &bull; Approved Building Permits &bull; Jobs Policy Compliance Reports
    </footer>

    <script>
    (function() {{
        /* === Tab switching === */
        var tabBtns = document.querySelectorAll('.tab-btn');
        var tabContents = document.querySelectorAll('.tab-content');
        for (var i = 0; i < tabBtns.length; i++) {{
            tabBtns[i].addEventListener('click', function() {{
                for (var j = 0; j < tabBtns.length; j++) {{
                    tabBtns[j].classList.remove('active');
                    tabContents[j].classList.remove('active');
                }}
                this.classList.add('active');
                document.getElementById('tab-' + this.getAttribute('data-tab')).classList.add('active');
            }});
        }}

        /* === Projects Tab filters === */
        var cards = [];
        var container = document.getElementById('cardContainer');
        var noResults = document.getElementById('noResults');
        var countEl = document.getElementById('filterCount');
        var searchInput = document.getElementById('searchInput');
        var filterSource = document.getElementById('filterSource');
        var filterRelevance = document.getElementById('filterRelevance');
        var filterNeighborhood = document.getElementById('filterNeighborhood');
        var filterStatus = document.getElementById('filterStatus');
        var filterBrjp = document.getElementById('filterBrjp');
        var filterOed = document.getElementById('filterOed');
        var sortOrder = document.getElementById('sortOrder');
        var debounceTimer = null;

        var els = container.getElementsByClassName('project-card');
        for (var i = 0; i < els.length; i++) {{ cards.push(els[i]); }}
        var total = cards.length;

        function applyProjectFilters() {{
            var q = searchInput.value.toLowerCase().trim();
            var src = filterSource.value;
            var rel = filterRelevance.value;
            var hood = filterNeighborhood.value;
            var stat = filterStatus.value;
            var brjp = filterBrjp.value;
            var oed = filterOed.value;
            var shown = 0;
            for (var i = 0; i < cards.length; i++) {{
                var c = cards[i];
                var visible = true;
                if (src && c.getAttribute('data-source') !== src) visible = false;
                if (rel && c.getAttribute('data-relevance') !== rel) visible = false;
                if (hood && c.getAttribute('data-neighborhood') !== hood) visible = false;
                if (stat && c.getAttribute('data-status') !== stat) visible = false;
                if (brjp && c.getAttribute('data-brjp-status') !== brjp) visible = false;
                if (oed && c.getAttribute('data-oed') !== oed) visible = false;
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
            for (var i = 0; i < sorted.length; i++) {{ container.appendChild(sorted[i]); }}
        }}

        function updateProjects() {{ applySort(); applyProjectFilters(); }}

        searchInput.addEventListener('input', function() {{
            if (debounceTimer) clearTimeout(debounceTimer);
            debounceTimer = setTimeout(updateProjects, 200);
        }});
        filterSource.addEventListener('change', updateProjects);
        filterRelevance.addEventListener('change', updateProjects);
        filterNeighborhood.addEventListener('change', updateProjects);
        filterStatus.addEventListener('change', updateProjects);
        filterBrjp.addEventListener('change', updateProjects);
        filterOed.addEventListener('change', updateProjects);
        sortOrder.addEventListener('change', updateProjects);
        applyProjectFilters();

        /* === Jobs Policy Tab: click-to-expand + filters === */
        var brjpRows = document.querySelectorAll('.brjp-row');
        var brjpDetailRows = document.querySelectorAll('.brjp-detail-row');
        var brjpSearch = document.getElementById('brjpSearch');
        var brjpComp = document.getElementById('brjpFilterCompliance');
        var brjpAgency = document.getElementById('brjpFilterAgency');
        var brjpStatus = document.getElementById('brjpFilterStatus');
        var brjpCount = document.getElementById('brjpFilterCount');
        var brjpTotal = brjpRows.length;

        for (var i = 0; i < brjpRows.length; i++) {{
            brjpRows[i].addEventListener('click', (function(idx) {{
                return function() {{
                    var row = brjpRows[idx];
                    var detail = brjpDetailRows[idx];
                    var isOpen = row.classList.contains('open');
                    row.classList.toggle('open');
                    detail.classList.toggle('open');
                }};
            }})(i));
        }}

        function applyBrjpFilters() {{
            var q = brjpSearch.value.toLowerCase().trim();
            var comp = brjpComp.value;
            var agency = brjpAgency.value;
            var status = brjpStatus.value;
            var shown = 0;
            for (var i = 0; i < brjpRows.length; i++) {{
                var r = brjpRows[i];
                var d = brjpDetailRows[i];
                var visible = true;
                if (comp && r.getAttribute('data-compliance') !== comp) visible = false;
                if (agency && r.getAttribute('data-agency') !== agency) visible = false;
                if (status && r.getAttribute('data-project-status') !== status) visible = false;
                if (q && r.getAttribute('data-search').indexOf(q) === -1) visible = false;
                r.style.display = visible ? '' : 'none';
                if (!visible) {{
                    r.classList.remove('open');
                    d.classList.remove('open');
                }}
                if (visible) shown++;
            }}
            brjpCount.textContent = 'Showing ' + shown + ' of ' + brjpTotal + ' projects';
        }}

        brjpSearch.addEventListener('input', function() {{
            if (debounceTimer) clearTimeout(debounceTimer);
            debounceTimer = setTimeout(applyBrjpFilters, 200);
        }});
        brjpComp.addEventListener('change', applyBrjpFilters);
        brjpAgency.addEventListener('change', applyBrjpFilters);
        brjpStatus.addEventListener('change', applyBrjpFilters);
        applyBrjpFilters();

        /* === Pipefitter Tab: click-to-expand + filters === */
        var pipeRows = document.querySelectorAll('.pipe-row');
        var pipeDetailRows = document.querySelectorAll('.pipe-detail-row');
        var pipeSearch = document.getElementById('pipeSearch');
        var pipeTrade = document.getElementById('pipeFilterTrade');
        var pipeStatus = document.getElementById('pipeFilterStatus');
        var pipeCount = document.getElementById('pipeFilterCount');
        var pipeTotal = pipeRows.length;

        for (var i = 0; i < pipeRows.length; i++) {{
            pipeRows[i].addEventListener('click', (function(idx) {{
                return function() {{
                    pipeRows[idx].classList.toggle('open');
                    pipeDetailRows[idx].classList.toggle('open');
                }};
            }})(i));
        }}

        function applyPipeFilters() {{
            var q = pipeSearch.value.toLowerCase().trim();
            var trade = pipeTrade.value;
            var status = pipeStatus.value;
            var shown = 0;
            for (var i = 0; i < pipeRows.length; i++) {{
                var r = pipeRows[i];
                var d = pipeDetailRows[i];
                var visible = true;
                if (trade && r.getAttribute('data-trade') !== trade) visible = false;
                if (status && r.getAttribute('data-project-status') !== status) visible = false;
                if (q && r.getAttribute('data-search').indexOf(q) === -1) visible = false;
                r.style.display = visible ? '' : 'none';
                if (!visible) {{
                    r.classList.remove('open');
                    d.classList.remove('open');
                }}
                if (visible) shown++;
            }}
            pipeCount.textContent = 'Showing ' + shown + ' of ' + pipeTotal + ' rows';
        }}

        pipeSearch.addEventListener('input', function() {{
            if (debounceTimer) clearTimeout(debounceTimer);
            debounceTimer = setTimeout(applyPipeFilters, 200);
        }});
        pipeTrade.addEventListener('change', applyPipeFilters);
        pipeStatus.addEventListener('change', applyPipeFilters);
        applyPipeFilters();
    }})();
    </script>
</body>
</html>""".format(
        run_time=run_time.strftime('%B %d, %Y at %I:%M %p'),
        summary_new=summary_new,
        summary_total=summary_total,
        a80_count=len(article80_projects),
        permit_count=len(permit_projects),
        high_count=summary_high,
        brjp_tracked=brjp_tracked,
        brjp_compliant=brjp_compliant,
        brjp_oed=brjp_oed,
        neighborhood_options=neighborhood_options,
        status_options=status_options,
        cards_html=cards_html,
        target_resident=brjp_targets["resident_pct"],
        target_poc=brjp_targets["poc_pct"],
        target_women=brjp_targets["women_pct"],
        gauges_html=gauges_html,
        brjp_table_rows=brjp_table_rows,
        pipe_trade_cards=pipe_trade_cards,
        pipe_trade_options=pipe_trade_options,
        pipe_table_rows=pipe_table_rows,
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

    # Fetch and process BRJP data
    brjp_projects = {}
    brjp_aggregates = {}
    pipefitter_by_project = {}
    global_trades = []
    brjp_targets = config.get("brjp_targets", {"resident_pct": 51.0, "poc_pct": 40.0, "women_pct": 12.0})
    pipefitter_trades = config.get("pipefitter_trades", [])

    if config.get("brjp_enabled", False):
        brjp_resource = config.get("brjp_resource_id", "")
        if brjp_resource:
            q1, q2, q3 = build_brjp_queries(brjp_resource, pipefitter_trades)

            print("\nFetching BRJP compliance data...")
            try:
                project_rows = api_fetch_sql(q1)
                print(f"  Fetched {len(project_rows)} project compliance rows")
            except Exception as e:
                print(f"  Error fetching BRJP project data: {e}")
                project_rows = []

            print("Fetching BRJP pipefitter trade data...")
            try:
                pipe_rows = api_fetch_sql(q2)
                print(f"  Fetched {len(pipe_rows)} pipefitter rows")
            except Exception as e:
                print(f"  Error fetching pipefitter data: {e}")
                pipe_rows = []

            print("Fetching BRJP global trade summary...")
            try:
                trade_rows = api_fetch_sql(q3)
                print(f"  Fetched {len(trade_rows)} trade summary rows")
            except Exception as e:
                print(f"  Error fetching trade summary: {e}")
                trade_rows = []

            if project_rows:
                print("\nProcessing BRJP data...")
                brjp_projects = process_brjp_projects(project_rows, brjp_targets)
                print(f"  {len(brjp_projects)} unique BRJP projects")
                brjp_aggregates = compute_brjp_aggregates(brjp_projects)
                print(f"  Compliant: {brjp_aggregates['compliant']}, "
                      f"Partial: {brjp_aggregates['partial']}, "
                      f"Non-compliant: {brjp_aggregates['non_compliant']}")

                # Match BRJP to tracker projects
                all_tracker = a80_projects + permit_projects
                matched = match_brjp_to_projects(all_tracker, brjp_projects)
                print(f"  Matched {matched} tracker projects to BRJP data")

            if pipe_rows:
                pipefitter_by_project = process_pipefitter_by_project(pipe_rows)
                print(f"  {len(pipefitter_by_project)} projects with pipefitter trades")

            if trade_rows:
                global_trades = process_global_trades(trade_rows)

    # Generate HTML report
    run_time = datetime.now(timezone.utc)
    print("\nGenerating HTML report...")
    html = generate_html(
        a80_projects, permit_projects, run_time,
        brjp_projects=brjp_projects,
        brjp_aggregates=brjp_aggregates,
        pipefitter_by_project=pipefitter_by_project,
        global_trades=global_trades,
        pipefitter_trades=pipefitter_trades,
        brjp_targets=brjp_targets,
    )

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
