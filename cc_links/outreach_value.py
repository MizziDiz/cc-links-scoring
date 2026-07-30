"""Provider-neutral outreach value, cost and outcome scoring."""

from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VALUE_SCHEMA_VERSION = 1

VALUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS value_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS domain_metrics (
    registered_domain TEXT NOT NULL,
    provider TEXT NOT NULL,
    as_of TEXT,
    domain_rating REAL,
    domain_authority REAL,
    authority_score REAL,
    trust_flow REAL,
    citation_flow REAL,
    organic_traffic REAL,
    referring_domains REAL,
    spam_score REAL,
    topical_relevance REAL,
    geo_relevance REAL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY (registered_domain,provider)
);

CREATE TABLE IF NOT EXISTS outreach_outcomes (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    status TEXT NOT NULL,
    quoted_cost REAL,
    actual_cost REAL,
    currency TEXT,
    published_url TEXT,
    contacted_at TEXT,
    published_at TEXT,
    live_30d INTEGER,
    live_90d INTEGER,
    notes TEXT,
    imported_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS page_values (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    score_band TEXT NOT NULL,
    page_quality_score REAL NOT NULL,
    promise_level TEXT NOT NULL,
    publication_probability REAL NOT NULL,
    commercial_model TEXT NOT NULL,
    price_status TEXT NOT NULL,
    advertised_price REAL,
    advertised_currency TEXT,
    normalized_price REAL,
    base_currency TEXT NOT NULL,
    metrics_coverage INTEGER NOT NULL,
    authority_component REAL,
    flow_component REAL,
    traffic_component REAL,
    relevance_component REAL,
    domain_strength_score REAL,
    placement_quality_score REAL NOT NULL,
    expected_effectiveness_points REAL,
    expected_contact_cost REAL NOT NULL,
    expected_content_cost REAL NOT NULL,
    expected_total_cost REAL,
    expected_cost_per_publication REAL,
    effectiveness_per_100_cost REAL,
    evaluation_status TEXT NOT NULL,
    value_reasons TEXT NOT NULL,
    evaluated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_page_values_domain
    ON page_values(registered_domain);
CREATE INDEX IF NOT EXISTS idx_page_values_effectiveness
    ON page_values(expected_effectiveness_points);
CREATE INDEX IF NOT EXISTS idx_page_values_cost
    ON page_values(expected_total_cost);
CREATE INDEX IF NOT EXISTS idx_page_values_status
    ON page_values(evaluation_status);
"""

METRIC_FIELDS = (
    "domain_rating",
    "domain_authority",
    "authority_score",
    "trust_flow",
    "citation_flow",
    "organic_traffic",
    "referring_domains",
    "spam_score",
    "topical_relevance",
    "geo_relevance",
)

HEADER_ALIASES = {
    "domain": "registered_domain",
    "registereddomain": "registered_domain",
    "target": "registered_domain",
    "provider": "provider",
    "source": "provider",
    "asof": "as_of",
    "date": "as_of",
    "domainrating": "domain_rating",
    "dr": "domain_rating",
    "domainauthority": "domain_authority",
    "da": "domain_authority",
    "authorityscore": "authority_score",
    "as": "authority_score",
    "trustflow": "trust_flow",
    "tf": "trust_flow",
    "citationflow": "citation_flow",
    "cf": "citation_flow",
    "organictraffic": "organic_traffic",
    "traffic": "organic_traffic",
    "referringdomains": "referring_domains",
    "refdomains": "referring_domains",
    "rd": "referring_domains",
    "spamscore": "spam_score",
    "topicalrelevance": "topical_relevance",
    "georelevance": "geo_relevance",
}

OUTCOME_STATUSES = {
    "planned",
    "contacted",
    "replied",
    "quoted",
    "accepted",
    "published",
    "rejected",
    "unreachable",
}

PAGE_VALUE_COLUMNS = (
    "url",
    "registered_domain",
    "score_band",
    "page_quality_score",
    "promise_level",
    "publication_probability",
    "commercial_model",
    "price_status",
    "advertised_price",
    "advertised_currency",
    "normalized_price",
    "base_currency",
    "metrics_coverage",
    "authority_component",
    "flow_component",
    "traffic_component",
    "relevance_component",
    "domain_strength_score",
    "placement_quality_score",
    "expected_effectiveness_points",
    "expected_contact_cost",
    "expected_content_cost",
    "expected_total_cost",
    "expected_cost_per_publication",
    "effectiveness_per_100_cost",
    "evaluation_status",
    "value_reasons",
    "evaluated_at",
)


@dataclass(frozen=True)
class CostConfig:
    """Internal costs and base currency used for comparable economics."""

    contact_cost: float = 0.0
    content_cost: float = 0.0
    base_currency: str = "USD"


@dataclass(frozen=True)
class ValueComponents:
    """Auditable prediction for one candidate page."""

    metrics_coverage: int
    authority: float | None
    flow: float | None
    traffic: float | None
    relevance: float | None
    domain_strength: float | None
    placement_quality: float
    expected_effectiveness: float | None
    expected_contact_cost: float
    expected_content_cost: float
    expected_total_cost: float | None
    expected_cost_per_publication: float | None
    effectiveness_per_100_cost: float | None
    status: str
    reasons: tuple[str, ...]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clamp(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(" ", "").replace("%", "")
    if not text:
        return None
    multiplier = 1.0
    if text[-1:].lower() in {"k", "m", "b"}:
        multiplier = {"k": 1_000.0, "m": 1_000_000.0, "b": 1_000_000_000.0}[
            text[-1].lower()
        ]
        text = text[:-1]
    if "," in text and "." not in text:
        tail = text.rsplit(",", 1)[1]
        text = text.replace(",", ".") if len(tail) <= 2 else text.replace(",", "")
    else:
        text = text.replace(",", "")
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _header_key(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _domain(value: str) -> str:
    text = value.strip().lower()
    for prefix in ("https://", "http://"):
        text = text.removeprefix(prefix)
    return text.split("/", 1)[0].split(":", 1)[0].removeprefix("www.")


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not value:
        return []
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


def initialize_value_db(path: str | Path) -> sqlite3.Connection:
    """Open or create the value database without touching source databases."""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(output), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(VALUE_SCHEMA)
    with connection:
        connection.execute(
            "INSERT OR REPLACE INTO value_meta(key,value) VALUES (?,?)",
            ("schema_version", str(VALUE_SCHEMA_VERSION)),
        )
    return connection


def import_domain_metrics(csv_path: str | Path, out_db: str | Path) -> dict[str, int]:
    """Import a provider export mapped to canonical DR/DA/TF/CF fields."""
    connection = initialize_value_db(out_db)
    imported = 0
    skipped = 0
    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        mapped_headers = {
            header: HEADER_ALIASES.get(_header_key(header), "")
            for header in (reader.fieldnames or [])
        }
        if "registered_domain" not in mapped_headers.values():
            connection.close()
            raise ValueError("metrics CSV needs a domain/registered_domain column")
        for source_row in reader:
            row = {
                target: source_row.get(source, "")
                for source, target in mapped_headers.items()
                if target
            }
            domain = _domain(str(row.get("registered_domain") or ""))
            if not domain:
                skipped += 1
                continue
            values = {field: _number(row.get(field)) for field in METRIC_FIELDS}
            if not any(value is not None for value in values.values()):
                skipped += 1
                continue
            with connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO domain_metrics (
                        registered_domain,provider,as_of,domain_rating,
                        domain_authority,authority_score,trust_flow,citation_flow,
                        organic_traffic,referring_domains,spam_score,
                        topical_relevance,geo_relevance,imported_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        domain,
                        str(row.get("provider") or "import").strip() or "import",
                        str(row.get("as_of") or "").strip() or None,
                        *(values[field] for field in METRIC_FIELDS),
                        _utc_now(),
                    ),
                )
            imported += 1
    connection.close()
    return {"imported": imported, "skipped": skipped}


def write_metrics_template(scores_db: str | Path, csv_path: str | Path) -> int:
    """Write one provider-neutral blank row per scored registered domain."""
    rows = _read_rows(
        scores_db,
        """
        SELECT registered_domain,
               MAX(CASE WHEN score_band='high' THEN url END) AS high_url,
               MAX(url) AS fallback_url,
               MAX(combined_score) AS best_score
        FROM page_scores
        GROUP BY registered_domain
        ORDER BY registered_domain
        """,
    )
    fieldnames = [
        "registered_domain",
        "example_url",
        "best_page_score",
        "provider",
        "as_of",
        *METRIC_FIELDS,
    ]
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "registered_domain": row["registered_domain"],
                    "example_url": row["high_url"] or row["fallback_url"],
                    "best_page_score": row["best_score"],
                }
            )
    return len(rows)


def import_outcomes(csv_path: str | Path, out_db: str | Path) -> dict[str, int]:
    """Import the observed funnel used to recalibrate predicted probabilities."""
    connection = initialize_value_db(out_db)
    imported = 0
    skipped = 0
    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            url = str(row.get("url") or "").strip()
            domain = _domain(
                str(row.get("registered_domain") or row.get("domain") or url)
            )
            status = str(row.get("status") or "").strip().lower()
            if not url or not domain or status not in OUTCOME_STATUSES:
                skipped += 1
                continue
            with connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO outreach_outcomes (
                        url,registered_domain,status,quoted_cost,actual_cost,
                        currency,published_url,contacted_at,published_at,
                        live_30d,live_90d,notes,imported_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        url,
                        domain,
                        status,
                        _number(row.get("quoted_cost")),
                        _number(row.get("actual_cost")),
                        str(row.get("currency") or "").strip().upper() or None,
                        str(row.get("published_url") or "").strip() or None,
                        str(row.get("contacted_at") or "").strip() or None,
                        str(row.get("published_at") or "").strip() or None,
                        int(
                            str(row.get("live_30d") or "").lower()
                            in {"1", "true", "yes"}
                        ),
                        int(
                            str(row.get("live_90d") or "").lower()
                            in {"1", "true", "yes"}
                        ),
                        str(row.get("notes") or "")[:1_000] or None,
                        _utc_now(),
                    ),
                )
            imported += 1
    connection.close()
    return {"imported": imported, "skipped": skipped}


def aggregate_domain_metrics(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, float | None]:
    """Average repeated observations of the same canonical provider field."""

    def average(field: str) -> float | None:
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        return round(sum(values) / len(values), 2) if values else None

    return {field: average(field) for field in METRIC_FIELDS}


def domain_strength_components(
    metrics: Mapping[str, float | None],
) -> tuple[
    float | None, float | None, float | None, float | None, float | None, int, list[str]
]:
    """Normalize authority, flow, traffic and relevance with missing-data awareness."""
    reasons: list[str] = []
    authority = None
    for authority_field in (
        "domain_rating",
        "authority_score",
        "domain_authority",
    ):
        authority_value = metrics.get(authority_field)
        if authority_value is not None:
            authority = _clamp(float(authority_value))
            reasons.append(f"authority:{authority_field}")
            break

    trust_flow = metrics.get("trust_flow")
    citation_flow = metrics.get("citation_flow")
    flow = None
    if trust_flow is not None:
        flow = float(trust_flow)
        if citation_flow is not None and float(citation_flow) > 0:
            balance = min(100.0, float(trust_flow) / float(citation_flow) * 100.0)
            flow = 0.7 * float(trust_flow) + 0.3 * balance
            reasons.append("flow_balance_included")
        flow = _clamp(flow)

    traffic_value = metrics.get("organic_traffic")
    traffic = (
        _clamp(math.log10(max(0.0, float(traffic_value)) + 1.0) / 6.0 * 100.0)
        if traffic_value is not None
        else None
    )
    referring = metrics.get("referring_domains")
    referring_score = (
        _clamp(math.log10(max(0.0, float(referring)) + 1.0) / 5.0 * 100.0)
        if referring is not None
        else None
    )
    relevance_values = [
        float(value)
        for value in (
            metrics.get("topical_relevance"),
            metrics.get("geo_relevance"),
        )
        if value is not None
    ]
    relevance = (
        _clamp(sum(relevance_values) / len(relevance_values))
        if relevance_values
        else None
    )

    weighted = [
        (authority, 0.35, "authority"),
        (flow, 0.30, "flow"),
        (traffic, 0.20, "traffic"),
        (referring_score, 0.05, "referring_domains"),
        (relevance, 0.10, "relevance"),
    ]
    available = [
        (value, weight, name) for value, weight, name in weighted if value is not None
    ]
    coverage = len(available)
    if not available:
        return authority, flow, traffic, relevance, None, 0, ["metrics_missing"]
    total_weight = sum(weight for _, weight, _ in available)
    strength = (
        sum(float(value) * weight for value, weight, _ in available) / total_weight
    )
    reasons.extend(f"metric:{name}" for _, _, name in available)
    spam = metrics.get("spam_score")
    if spam is not None:
        penalty = 1.0 - min(max(float(spam), 0.0), 100.0) / 200.0
        strength *= penalty
        reasons.append("spam_penalty")
    return authority, flow, traffic, relevance, _clamp(strength), coverage, reasons


def placement_quality_score(terms: Mapping[str, Any]) -> tuple[float, list[str]]:
    """Score only what and where the publisher promises to place."""
    reasons: list[str] = []
    links = set(_json_list(terms.get("link_attributes")))
    locations = set(_json_list(terms.get("placement_locations")))
    if "dofollow" in links and "nofollow" in links:
        link_score = 50.0
        reasons.append("link_policy_conflicting")
    elif "dofollow" in links:
        link_score = 100.0
        reasons.append("link:dofollow")
    elif links & {"nofollow", "sponsored", "ugc"}:
        link_score = 20.0
        reasons.append("link:limited_equity")
    else:
        link_score = 55.0
        reasons.append("link:unspecified")

    location_scores = {
        "editorial_body": 100.0,
        "site_blog": 80.0,
        "homepage": 75.0,
        "author_bio": 60.0,
        "social_promotion": 30.0,
    }
    location_score = max(
        (location_scores[item] for item in locations if item in location_scores),
        default=50.0,
    )
    reasons.append(
        "location:"
        + max(
            (item for item in locations if item in location_scores),
            key=lambda item: location_scores[item],
            default="unspecified",
        )
    )
    permanence = str(terms.get("permanence") or "unspecified")
    permanence_score = {
        "permanent": 100.0,
        "temporary": 35.0,
        "unspecified": 65.0,
    }.get(permanence, 65.0)
    reasons.append(f"permanence:{permanence}")
    return _clamp(
        0.5 * link_score + 0.3 * location_score + 0.2 * permanence_score
    ), reasons


def evaluate_page_value(
    *,
    page_quality_score: float,
    terms: Mapping[str, Any],
    metrics: Mapping[str, float | None],
    normalized_price: float | None,
    cost_config: CostConfig,
) -> ValueComponents:
    """Calculate expected placement value and comparable cost without hidden imputation."""
    (
        authority,
        flow,
        traffic,
        relevance,
        domain_strength,
        coverage,
        metric_reasons,
    ) = domain_strength_components(metrics)
    placement_quality, placement_reasons = placement_quality_score(terms)
    probability = min(0.95, max(0.01, float(terms.get("promise_probability") or 0.15)))
    effectiveness = None
    if domain_strength is not None:
        qualification_factor = 0.5 + 0.5 * _clamp(page_quality_score) / 100.0
        effectiveness = round(
            domain_strength
            * probability
            * placement_quality
            / 100.0
            * qualification_factor,
            2,
        )
    contact_cost = round(cost_config.contact_cost / probability, 2)
    responsibility = str(terms.get("content_responsibility") or "unspecified")
    content_cost = 0.0 if responsibility == "site_writes" else cost_config.content_cost
    total_cost = (
        round(contact_cost + content_cost + normalized_price, 2)
        if normalized_price is not None
        else None
    )
    cost_per_publication = total_cost
    efficiency = (
        round(effectiveness / total_cost * 100.0, 2)
        if effectiveness is not None and total_cost is not None and total_cost > 0
        else None
    )
    missing: list[str] = []
    if domain_strength is None:
        missing.append("metrics")
    if normalized_price is None:
        missing.append("price")
    status = "complete" if not missing else "missing_" + "_and_".join(missing)
    reasons = [
        *metric_reasons,
        *placement_reasons,
        f"promise:{terms.get('promise_level') or 'unclear'}",
        f"content:{responsibility}",
    ]
    return ValueComponents(
        metrics_coverage=coverage,
        authority=authority,
        flow=flow,
        traffic=traffic,
        relevance=relevance,
        domain_strength=domain_strength,
        placement_quality=placement_quality,
        expected_effectiveness=effectiveness,
        expected_contact_cost=contact_cost,
        expected_content_cost=round(content_cost, 2),
        expected_total_cost=total_cost,
        expected_cost_per_publication=cost_per_publication,
        effectiveness_per_100_cost=efficiency,
        status=status,
        reasons=tuple(sorted(set(reasons))),
    )


def _fx_rates(path: str | Path | None, base_currency: str) -> dict[str, float]:
    rates = {base_currency.upper(): 1.0}
    if not path:
        return rates
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            currency = str(row.get("currency") or "").strip().upper()
            rate = _number(row.get("rate_to_base"))
            if currency and rate is not None and rate > 0:
                rates[currency] = rate
    return rates


def _read_rows(path: str | Path, query: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(query)]
    finally:
        connection.close()


def build_value_scores(
    *,
    scores_db: str | Path,
    terms_db: str | Path,
    out_db: str | Path,
    cost_config: CostConfig | None = None,
    fx_csv: str | Path | None = None,
) -> dict[str, Any]:
    """Join page scores, extracted promises and imported metrics into value rows."""
    cost_config = cost_config or CostConfig()
    if cost_config.contact_cost < 0 or cost_config.content_cost < 0:
        raise ValueError("contact and content costs cannot be negative")
    if len(cost_config.base_currency.strip()) != 3:
        raise ValueError("base_currency must be a three-letter currency code")
    connection = initialize_value_db(out_db)
    score_rows = _read_rows(
        scores_db,
        """
        SELECT url,registered_domain,score_band,combined_score
        FROM page_scores ORDER BY registered_domain,url
        """,
    )
    term_rows = {
        str(row["url"]): row
        for row in _read_rows(terms_db, "SELECT * FROM placement_terms")
    }
    connection.row_factory = sqlite3.Row
    grouped_metrics: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in connection.execute("SELECT * FROM domain_metrics"):
        grouped_metrics[str(row["registered_domain"])].append(dict(row))
    rates = _fx_rates(fx_csv, cost_config.base_currency)
    evaluated_at = _utc_now()
    missing_terms = 0
    with connection:
        connection.execute("DELETE FROM page_values")
        for score in score_rows:
            terms = term_rows.get(str(score["url"]))
            if terms is None:
                missing_terms += 1
                continue
            metrics = aggregate_domain_metrics(
                grouped_metrics.get(str(score["registered_domain"]), [])
            )
            advertised = None
            if terms.get("price_status") == "free":
                advertised = 0.0
            elif terms.get("price_min") is not None:
                advertised = (
                    float(terms["price_min"])
                    + float(terms.get("price_max") or terms["price_min"])
                ) / 2.0
            currency = str(terms.get("currency") or "").upper() or None
            normalized_price = (
                round(advertised * rates[currency], 2)
                if advertised is not None and currency in rates
                else 0.0
                if terms.get("price_status") == "free"
                else None
            )
            components = evaluate_page_value(
                page_quality_score=float(score["combined_score"]),
                terms=terms,
                metrics=metrics,
                normalized_price=normalized_price,
                cost_config=cost_config,
            )
            placeholders = ",".join("?" for _ in PAGE_VALUE_COLUMNS)
            connection.execute(
                f"INSERT INTO page_values ({','.join(PAGE_VALUE_COLUMNS)}) "
                f"VALUES ({placeholders})",
                (
                    score["url"],
                    score["registered_domain"],
                    score["score_band"],
                    score["combined_score"],
                    terms["promise_level"],
                    terms["promise_probability"],
                    terms["commercial_model"],
                    terms["price_status"],
                    advertised,
                    currency,
                    normalized_price,
                    cost_config.base_currency.upper(),
                    components.metrics_coverage,
                    components.authority,
                    components.flow,
                    components.traffic,
                    components.relevance,
                    components.domain_strength,
                    components.placement_quality,
                    components.expected_effectiveness,
                    components.expected_contact_cost,
                    components.expected_content_cost,
                    components.expected_total_cost,
                    components.expected_cost_per_publication,
                    components.effectiveness_per_100_cost,
                    components.status,
                    json.dumps(components.reasons, ensure_ascii=False),
                    evaluated_at,
                ),
            )
    report = {
        "schema_version": VALUE_SCHEMA_VERSION,
        "evaluated_at": evaluated_at,
        "pages": len(score_rows) - missing_terms,
        "missing_terms": missing_terms,
        "base_currency": cost_config.base_currency.upper(),
        "cost_config": asdict(cost_config),
        "evaluation_status": {
            str(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT evaluation_status,COUNT(*) FROM page_values GROUP BY 1"
            )
        },
        "promise_levels": {
            str(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT promise_level,COUNT(*) FROM page_values GROUP BY 1"
            )
        },
        "price_status": {
            str(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT price_status,COUNT(*) FROM page_values GROUP BY 1"
            )
        },
        "observed_outcomes": {
            str(row[0]): int(row[1])
            for row in connection.execute(
                "SELECT status,COUNT(*) FROM outreach_outcomes GROUP BY 1"
            )
        },
        "observed_by_promise": [
            {
                "promise_level": str(row[0]),
                "attempted": int(row[1]),
                "accepted_or_published": int(row[2] or 0),
                "published": int(row[3] or 0),
                "live_90d": int(row[4] or 0),
            }
            for row in connection.execute(
                """
                SELECT p.promise_level,COUNT(*),
                       SUM(o.status IN ('accepted','published')),
                       SUM(o.status='published'),SUM(o.live_90d=1)
                FROM outreach_outcomes o
                JOIN page_values p USING(url)
                GROUP BY p.promise_level ORDER BY p.promise_level
                """
            )
        ],
        "quick_check": str(connection.execute("PRAGMA quick_check").fetchone()[0]),
    }
    connection.close()
    return report


def write_value_outputs(
    out_db: str | Path,
    *,
    report_path: str | Path,
    csv_path: str | Path,
    report: Mapping[str, Any],
) -> None:
    """Write deterministic report and value-ranked CSV."""
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = list(
            connection.execute(
                """
                SELECT * FROM page_values
                ORDER BY expected_effectiveness_points IS NULL,
                         expected_effectiveness_points DESC,
                         page_quality_score DESC,url
                """
            )
        )
    finally:
        connection.close()
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    with Path(csv_path).open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(dict(row) for row in rows)
