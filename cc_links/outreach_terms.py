"""Extract auditable publication promises and commercial terms from outreach HTML."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import time
import unicodedata
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from botocore.exceptions import (  # type: ignore[import-untyped]
    BotoCoreError,
    ClientError,
)
from lxml import etree  # type: ignore[import-untyped]
from lxml import html as lxml_html  # type: ignore[import-untyped]
from requests.exceptions import RequestException
from warcio.exceptions import ArchiveLoadFailed  # type: ignore[import-untyped]

from cc_links.fetch import enable_s3, fetch_warc_record
from cc_links.outreach_enrich import parse_warc_html

TERMS_SCHEMA_VERSION = 2
TERMS_EXTRACTOR_VERSION = 7

TERMS_SCHEMA = """
CREATE TABLE IF NOT EXISTS terms_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS placement_terms (
    url TEXT PRIMARY KEY,
    registered_domain TEXT NOT NULL,
    source TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    fetch_status TEXT NOT NULL,
    placement_types TEXT NOT NULL DEFAULT '[]',
    promise_level TEXT NOT NULL DEFAULT 'unclear',
    promise_probability REAL NOT NULL DEFAULT 0.15,
    commercial_model TEXT NOT NULL DEFAULT 'unknown',
    price_status TEXT NOT NULL DEFAULT 'unknown',
    price_kind TEXT NOT NULL DEFAULT 'unknown',
    price_min REAL,
    price_max REAL,
    currency TEXT,
    link_attributes TEXT NOT NULL DEFAULT '[]',
    placement_locations TEXT NOT NULL DEFAULT '[]',
    permanence TEXT NOT NULL DEFAULT 'unspecified',
    content_responsibility TEXT NOT NULL DEFAULT 'unspecified',
    turnaround_days_max INTEGER,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    error_kind TEXT,
    error_detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_placement_terms_domain
    ON placement_terms(registered_domain);
CREATE INDEX IF NOT EXISTS idx_placement_terms_promise
    ON placement_terms(promise_level);
CREATE INDEX IF NOT EXISTS idx_placement_terms_price
    ON placement_terms(price_status,currency,price_min);
"""

GUARANTEE_RE = re.compile(
    r"\b(?:publication\s+is\s+guaranteed|guaranteed\s+publication|"
    r"we\s+will\s+publish|your\s+(?:post|article)\s+will\s+be\s+published|"
    r"publicacion\s+garantizada|sera\s+publicad[oa]|"
    r"publicacao\s+garantida|sera\s+publicad[oa])\b",
    re.IGNORECASE,
)
CONDITIONAL_RE = re.compile(
    r"\b(?:subject\s+to\s+(?:editorial\s+)?review|if\s+accepted|"
    r"once\s+(?:it\s+is\s+|your\s+(?:article|submission)\s+is\s+)?approved|"
    r"upon\s+(?:editorial\s+)?acceptance|after\s+(?:editorial\s+)?approval|"
    r"selected\s+for\s+publication|article\s+(?:is|was)\s+chosen|"
    r"if\s+(?:your|the)\s+(?:article|submission|content)\s+"
    r"(?:is\s+)?(?:accepted|approved|passes|meets)|"
    r"if\s+everything\s+(?:works\s+out|meets\s+our\s+requirements)|"
    r"submissions?\s+(?:are|will\s+be)\s+reviewed|editorial\s+approval|"
    r"we\s+reserve\s+the\s+right|(?:does\s+not|not)\s+guarantee\s+publication|"
    r"sujeto\s+a\s+(?:revision|aprobacion)|si\s+(?:es\s+)?aceptad[oa]|"
    r"una\s+vez\s+aprobad[oa]|tras\s+(?:la\s+)?aprobacion|"
    r"si\s+(?:el\s+)?(?:articulo|contenido)\s+cumple|"
    r"nos\s+reservamos\s+el\s+derecho|"
    r"sujeit[oa]\s+a\s+(?:revisao|avaliacao)|se\s+(?:for\s+)?aceit[oa]|"
    r"apos\s+(?:a\s+)?aprovacao|uma\s+vez\s+aprovad[oa]|"
    r"nao\s+garantimos?\s+a\s+publicacao)\b",
    re.IGNORECASE,
)
OPEN_SUBMISSION_RE = re.compile(
    r"\b(?:write\s+for\s+us|submit\s+(?:your|an?)\s+article|"
    r"become\s+(?:an?\s+)?contributor|guest\s+post\s+guidelines|"
    r"escribe\s+para\s+nosotros|envia\s+tu\s+articulo|"
    r"publica\s+con\s+nosotros|guia\s+para\s+autores|"
    r"escreva\s+para\s+nos|envie\s+(?:seu|um)\s+artigo|"
    r"publique\s+conosco|diretrizes\s+para\s+autores|"
    r"ecrivez\s+pour\s+nous|soumettre\s+un\s+article|"
    r"schreib(?:e|en)?\s+fur\s+uns|gastbeitrag\s+einreichen)\b",
    re.IGNORECASE,
)
CLOSED_SUBMISSION_RE = re.compile(
    r"\b(?:"
    r"(?:no\s+longer|not|currently\s+not)\s+accepting\s+"
    r"(?:guest\s+posts?|submissions?|articles?)|"
    r"(?:guest\s+post|article|submission)\s+(?:submissions?\s+)?"
    r"(?:are\s+)?closed|submissions?\s+(?:are\s+)?closed|"
    r"we\s+do\s+not\s+accept\s+(?:guest\s+posts?|submissions?|articles?)|"
    r"ya\s+no\s+aceptamos\s+(?:articulos|publicaciones|colaboraciones)|"
    r"no\s+aceptamos\s+(?:articulos|publicaciones)\s+invitad[oa]s?|"
    r"nao\s+aceitamos\s+(?:mais\s+)?(?:artigos|posts|submissoes)"
    r")\b",
    re.IGNORECASE,
)

PLACEMENT_PATTERNS: dict[str, re.Pattern[str]] = {
    "guest_post": re.compile(
        r"\b(?:guest\s+(?:post|article|contribution)|"
        r"articulo\s+invitado|post\s+invitado|"
        r"artigo\s+convidado|gastbeitrag)\b",
        re.IGNORECASE,
    ),
    "sponsored_post": re.compile(
        r"\b(?:sponsored\s+(?:guest\s+)?(?:post|article|content)|paid\s+placement|"
        r"advertorial|articulo\s+patrocinado|contenido\s+patrocinado|"
        r"artigo\s+patrocinado|publireportage|publireportaje)\b",
        re.IGNORECASE,
    ),
    "contributor": re.compile(
        r"\b(?:become\s+(?:an?\s+)?contributor|contributor\s+program|"
        r"colaborador(?:a)?|contribuidor(?:a)?|contributeur|autor\s+werden)\b",
        re.IGNORECASE,
    ),
    "editorial_submission": re.compile(
        r"\b(?:submit\s+(?:your|an?)\s+article|article\s+submission|"
        r"envia\s+tu\s+articulo|enviar\s+articulo|"
        r"envie\s+(?:seu|um)\s+artigo|soumettre\s+un\s+article)\b",
        re.IGNORECASE,
    ),
    "link_insertion": re.compile(
        r"\b(?:link\s+insertion|niche\s+edit|existing\s+article|"
        r"insercion\s+de\s+enlace|insercao\s+de\s+link)\b",
        re.IGNORECASE,
    ),
}

LINK_PATTERNS: dict[str, re.Pattern[str]] = {
    "dofollow": re.compile(r"\b(?:do[\s-]?follow|followed\s+link)\b", re.IGNORECASE),
    "nofollow": re.compile(r"\bno[\s-]?follow\b", re.IGNORECASE),
    "sponsored": re.compile(
        r"\brel\s*=\s*[\"']?sponsored|sponsored\s+link\b", re.IGNORECASE
    ),
    "ugc": re.compile(r"\brel\s*=\s*[\"']?ugc\b", re.IGNORECASE),
}
LOCATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "editorial_body": re.compile(
        r"\b(?:in[-\s]?content|contextual\s+link|within\s+the\s+(?:article|content)|"
        r"enlace\s+contextual|dentro\s+del\s+articulo|link\s+contextual)\b",
        re.IGNORECASE,
    ),
    "author_bio": re.compile(
        r"\b(?:author\s+bio|author\s+biography|bio\s+link|"
        r"biografia\s+del\s+autor|biografia\s+do\s+autor)\b",
        re.IGNORECASE,
    ),
    "site_blog": re.compile(
        r"\b(?:published\s+on\s+(?:our|the)\s+(?:site|website|blog)|"
        r"publication\s+on\s+(?:our|the)\s+(?:site|blog)|"
        r"publicado\s+en\s+(?:nuestro|el)\s+(?:sitio|blog)|"
        r"publicado\s+no\s+(?:nosso|blog|site))\b",
        re.IGNORECASE,
    ),
    "homepage": re.compile(
        r"\b(?:(?:featured|published|placed)\s+on\s+(?:the\s+)?home\s?page|"
        r"home\s?page\s+placement|"
        r"(?:publicad[oa]|destacad[oa])\s+en\s+(?:la\s+)?pagina\s+principal|"
        r"(?:publicad[oa]|destacad[oa])\s+na\s+pagina\s+inicial)\b",
        re.IGNORECASE,
    ),
    "social_promotion": re.compile(
        r"\b(?:shared|promoted)\s+on\s+(?:our\s+)?social|"
        r"(?:compartid[oa]|promocionad[oa])\s+en\s+(?:nuestras\s+)?redes\s+sociales|"
        r"(?:compartilhad[oa]|promovid[oa])\s+nas\s+(?:nossas\s+)?redes\s+sociais\b",
        re.IGNORECASE,
    ),
}

PERMANENT_RE = re.compile(
    r"\b(?:permanent(?:ly)?|lifetime|never\s+removed|"
    r"permanente|de\s+por\s+vida|nao\s+sera\s+removido)\b",
    re.IGNORECASE,
)
TEMPORARY_RE = re.compile(
    r"\b(?:temporary|for\s+(?:only\s+)?\d+\s+(?:months?|years?)|"
    r"durante\s+\d+\s+(?:meses|anos)|temporal)\b",
    re.IGNORECASE,
)
AUTHOR_PROVIDES_RE = re.compile(
    r"\b(?:you\s+(?:must|will|should)\s+(?:write|provide|submit)|"
    r"send\s+us\s+your\s+(?:article|draft)|"
    r"debes\s+(?:escribir|enviar)|envianos\s+tu\s+articulo|"
    r"voce\s+deve\s+(?:escrever|enviar)|envie\s+seu\s+artigo)\b",
    re.IGNORECASE,
)
SITE_WRITES_RE = re.compile(
    r"\b(?:we\s+(?:will|can)\s+write\s+(?:the\s+)?(?:article|content)|"
    r"content\s+creation\s+included|"
    r"redactamos\s+(?:el\s+)?articulo|criacao\s+de\s+conteudo\s+incluida)\b",
    re.IGNORECASE,
)
FREE_RE = re.compile(
    r"\b(?:free\s+(?:submission|guest\s+post|publication)|"
    r"no\s+(?:submission|publication)\s+fee|without\s+charge|"
    r"publicacion\s+gratuita|sin\s+costo|sin\s+tarifa|"
    r"publicacao\s+gratuita|sem\s+custo|sem\s+taxa)\b",
    re.IGNORECASE,
)
PAID_RE = re.compile(
    r"\b(?:publication\s+fee|submission\s+fee|guest\s+post\s+fee|"
    r"paid\s+(?:guest\s+)?(?:post|placement|publication)|"
    r"sponsored\s+(?:post|article)|"
    r"tarifa\s+de\s+publicacion|publicacion\s+pagada|articulo\s+patrocinado|"
    r"taxa\s+de\s+publicacao|publicacao\s+paga|artigo\s+patrocinado)\b",
    re.IGNORECASE,
)
QUOTE_RE = re.compile(
    r"\b(?:contact\s+(?:us\s+)?for\s+(?:pricing|a\s+quote)|request\s+a\s+quote|"
    r"pricing\s+on\s+request|consulte\s+(?:el\s+)?precio|solicite\s+or[cs]amento|"
    r"preco\s+sob\s+consulta)\b",
    re.IGNORECASE,
)
TURNAROUND_RE = re.compile(
    r"\b(?:within|in|takes?|turnaround(?:\s+time)?(?:\s+is)?|"
    r"en|dentro\s+de|em|prazo\s+de)\s*"
    r"(?P<low>\d{1,3})(?:\s*[-–]\s*(?P<high>\d{1,3}))?\s*"
    r"(?P<unit>business\s+days?|days?|weeks?|dias?|semanas?)\b",
    re.IGNORECASE,
)
PRICE_RE = re.compile(
    r"(?:(?P<prefix>US\$|USD|EUR|GBP|BRL|INR|R\$|\$|€|£|₹)\s*"
    r"(?P<amount1>(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?))|"
    r"(?P<amount2>(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?))\s*"
    r"(?P<suffix>USD|EUR|GBP|BRL|INR|\$))",
    re.IGNORECASE,
)
PLACEMENT_PRICE_CONTEXT_RE = re.compile(
    r"\b(?:publication\s+(?:fees?|charges?|prices?|pricing)|"
    r"publishing\s+(?:fees?|charges?|prices?)|"
    r"submission\s+(?:fees?|charges?|prices?)|"
    r"article\s+(?:processing|publishing)\s+(?:fees?|charges?)|"
    r"guest\s+post|sponsored\s+post|link\s+insertion|"
    r"placement|advertorial|single\s+post|multiple\s+posts?|"
    r"per\s+(?:article|post)|"
    r"pricing\s+details|payment\s+details|editorial\s+(?:review\s+)?fee|"
    r"(?:package|plan)\s+(?:price|fee|cost)|"
    r"tarifa\s+de\s+publicacion|por\s+articulo|post\s+invitado|"
    r"taxa\s+de\s+publicacao|por\s+artigo|artigo\s+patrocinado)\b",
    re.IGNORECASE,
)
PRICE_DIRECT_CONTEXT_RE = re.compile(
    r"\b(?:"
    r"(?:publication|publishing|submission|editorial|processing)\s+"
    r"(?:fees?|charges?|prices?|pricing|costs?)|"
    r"(?:guest|sponsored)\s+(?:post|article)\s+"
    r"(?:fees?|prices?|pricing|costs?|packages?)|"
    r"link\s+insertion(?:\s+(?:fees?|prices?|pricing|costs?))?|"
    r"(?:packages?|plans?)\s+(?:start(?:ing)?\s+at|from|at|for|price|fee|cost)|"
    r"(?:prices?|pricing|fees?|rates?)\s*(?:start(?:ing)?\s+at|from|:)|"
    r"(?:single|multiple)\s+posts?|"
    r"(?:per|each)\s+(?:article|post|placement)|"
    r"one[-\s]?time\s+(?:placement|publication)|"
    r"tarifa\s+de\s+publicacion|precio\s+por\s+articulo|"
    r"taxa\s+de\s+publicacao|preco\s+por\s+artigo"
    r")\b",
    re.IGNORECASE,
)
PRICE_EXCLUSION_RE = re.compile(
    r"\b(?:earn(?:s|ed|ing)?|payout|we\s+pay|paid\s+contributor|"
    r"performance\s+bonus|compensat(?:e|es|ed|ion)|"
    r"donation|donacion|donacao|order\s+now|challenge|"
    r"bitcoin|ethereum|crypto\s+price|market\s+cap|"
    r"free\s+shipping|seed\s+funding|series\s+[a-z]\s+funding|"
    r"professional\s+(?:editing|correction)|"
    r"(?:edicion|correccion)\s+profesional|"
    r"criacao\s+de\s+conteudo)\b",
    re.IGNORECASE,
)
PRICE_ADDON_PREFIX_RE = re.compile(
    r"\b(?:additional|extra|optional)\s+(?:links?|backlinks?)"
    r"(?:\s+(?:costs?|fee))?\s*[:\-]?\s*$",
    re.IGNORECASE,
)
PRICE_NONPLACEMENT_PREFIX_RE = re.compile(
    r"\b(?:save|bonus|earn|payout|funding|donation|donacion|donacao|"
    r"books?\s+under|free\s+shipping|salary|prize|worth|valued\s+at)"
    r"(?:\s+\w+){0,5}\s*[:\-]?\s*$",
    re.IGNORECASE,
)

PROMISE_PROBABILITY = {
    "closed": 0.01,
    "guaranteed": 0.85,
    "conditional_review": 0.50,
    "open_submission": 0.30,
    "unclear": 0.15,
}
CURRENCY_MAP = {
    "$": "USD",
    "US$": "USD",
    "USD": "USD",
    "€": "EUR",
    "EUR": "EUR",
    "£": "GBP",
    "GBP": "GBP",
    "R$": "BRL",
    "BRL": "BRL",
    "₹": "INR",
    "INR": "INR",
}


@dataclass(frozen=True)
class TermsConfig:
    """Settings that affect archived HTML commercial-term extraction."""

    max_html_bytes: int = 5_000_000
    retries: int = 2
    retry_backoff: float = 1.0


@dataclass(frozen=True)
class TermsSummary:
    """Counters for a resumable terms extraction run."""

    input_pages: int
    scheduled_pages: int
    completed_pages: int
    successful_pages: int
    priced_pages: int
    explicit_promise_pages: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normalized(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    without_marks = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return re.sub(r"\s+", " ", without_marks).strip()


def _price_number(value: str) -> float | None:
    compact = re.sub(r"\s+", "", value)
    if not compact:
        return None
    if "," in compact and "." in compact:
        if compact.rfind(",") > compact.rfind("."):
            compact = compact.replace(".", "").replace(",", ".")
        else:
            compact = compact.replace(",", "")
    elif "," in compact:
        tail = compact.rsplit(",", 1)[1]
        compact = (
            compact.replace(",", ".") if len(tail) <= 2 else compact.replace(",", "")
        )
    try:
        amount = float(compact)
    except ValueError:
        return None
    return amount if 0 < amount <= 10_000_000 else None


def _evidence(
    text: str, matches: Sequence[tuple[str, re.Match[str]]]
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for signal, match in matches:
        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 160)
        snippet = text[start:end].strip()
        key = (signal, snippet)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"signal": signal, "snippet": snippet[:500]})
        if len(rows) >= 40:
            break
    return rows


def extract_placement_terms(html: str) -> dict[str, Any]:
    """Return structured promises, price and link-placement evidence from HTML."""
    parser = lxml_html.HTMLParser(recover=True, remove_comments=True)
    try:
        document = lxml_html.fromstring(html, parser=parser)
    except (etree.ParserError, ValueError):
        document = lxml_html.fromstring("<html></html>", parser=parser)
    for node in document.xpath("//script|//style|//noscript|//svg"):
        parent = node.getparent()
        if parent is not None:
            parent.remove(node)
    text = _normalized(" ".join(document.itertext()))[:3_000_000]
    segment_xpath = (
        "//p|//li|//tr|//label|//option|//figcaption|"
        "//h1|//h2|//h3|//h4|//h5|//h6|"
        "//div[not(.//div)]|//section[not(.//section)]"
    )
    price_segments = sorted(
        {
            segment
            for node in document.xpath(segment_xpath)
            if 5 <= len(segment := _normalized(" ".join(node.itertext()))) <= 2_000
        }
    )
    if not price_segments:
        price_segments = [text[:2_000]]
    matches: list[tuple[str, re.Match[str]]] = []

    def found(signal: str, pattern: re.Pattern[str]) -> bool:
        match = pattern.search(text)
        if match:
            matches.append((signal, match))
            return True
        return False

    closed = found("promise:closed", CLOSED_SUBMISSION_RE)
    conditional = found("promise:conditional_review", CONDITIONAL_RE)
    guaranteed = found("promise:guaranteed", GUARANTEE_RE) and not conditional
    open_submission = found("promise:open_submission", OPEN_SUBMISSION_RE)
    if closed:
        promise_level = "closed"
    elif guaranteed:
        promise_level = "guaranteed"
    elif conditional:
        promise_level = "conditional_review"
    elif open_submission:
        promise_level = "open_submission"
    else:
        promise_level = "unclear"

    placement_types = [
        name
        for name, pattern in PLACEMENT_PATTERNS.items()
        if found(f"placement:{name}", pattern)
    ]
    link_attributes = [
        name
        for name, pattern in LINK_PATTERNS.items()
        if found(f"link:{name}", pattern)
    ]
    placement_locations = [
        name
        for name, pattern in LOCATION_PATTERNS.items()
        if found(f"location:{name}", pattern)
    ]

    if found("permanence:permanent", PERMANENT_RE):
        permanence = "permanent"
    elif found("permanence:temporary", TEMPORARY_RE):
        permanence = "temporary"
    else:
        permanence = "unspecified"

    author_provides = found("content:author_provides", AUTHOR_PROVIDES_RE)
    site_writes = found("content:site_writes", SITE_WRITES_RE)
    content_responsibility = (
        "both_options"
        if author_provides and site_writes
        else "author_provides"
        if author_provides
        else "site_writes"
        if site_writes
        else "unspecified"
    )

    free_signal = found("commercial:free", FREE_RE)
    paid_signal = False
    for paid_match in PAID_RE.finditer(text):
        prefix = text[max(0, paid_match.start() - 25) : paid_match.start()]
        if re.search(r"\b(?:no|without|sin|sem)\s*$", prefix, re.IGNORECASE):
            continue
        matches.append(("commercial:paid", paid_match))
        paid_signal = True
        break
    quote_signal = found("commercial:quote_required", QUOTE_RE)
    prices: list[tuple[float, str]] = []
    review_prices: list[tuple[float, str]] = []
    price_evidence: list[dict[str, str]] = []
    seen_price_options: set[tuple[float, str, str]] = set()
    for segment in price_segments:
        price_context = bool(PLACEMENT_PRICE_CONTEXT_RE.search(segment))
        excluded_context = bool(PRICE_EXCLUSION_RE.search(segment))
        if not price_context or excluded_context:
            continue
        for match in PRICE_RE.finditer(segment):
            token = match.group("prefix") or match.group("suffix") or ""
            amount_text = match.group("amount1") or match.group("amount2") or ""
            amount = _price_number(amount_text)
            currency = CURRENCY_MAP.get(token.upper(), CURRENCY_MAP.get(token))
            prefix = segment[max(0, match.start() - 100) : match.start()]
            local_context = segment[
                max(0, match.start() - 180) : min(len(segment), match.end() + 180)
            ]
            addon_price = bool(PRICE_ADDON_PREFIX_RE.search(prefix))
            nonplacement_price = bool(PRICE_NONPLACEMENT_PREFIX_RE.search(prefix))
            if amount is None or not currency or addon_price or nonplacement_price:
                continue
            option = (amount, currency, segment)
            if option in seen_price_options:
                continue
            seen_price_options.add(option)
            direct_context = bool(PRICE_DIRECT_CONTEXT_RE.search(local_context))
            target = prices if direct_context else review_prices
            target.append((amount, currency))
            price_evidence.append(
                {
                    "signal": (
                        "commercial:advertised_price"
                        if direct_context
                        else "commercial:price_review"
                    ),
                    "snippet": local_context[:500],
                }
            )
    currencies = sorted({currency for _, currency in prices})
    review_currencies = sorted({currency for _, currency in review_prices})
    if prices:
        price_status = (
            "advertised_mixed_currency" if len(currencies) > 1 else "advertised"
        )
        commercial_model = "paid"
        price_kind = (
            "placement_fee_mixed_currency" if len(currencies) > 1 else "placement_fee"
        )
    elif review_prices:
        price_status = (
            "advertised_review_mixed_currency"
            if len(review_currencies) > 1
            else "advertised_review"
        )
        commercial_model = "paid"
        price_kind = "placement_fee_unverified"
    elif free_signal and paid_signal:
        price_status = "conflicting"
        commercial_model = "mixed"
        price_kind = "conflicting"
    elif free_signal:
        price_status = "free"
        commercial_model = "free"
        price_kind = "free"
    elif paid_signal:
        price_status = "paid_unquoted"
        commercial_model = "paid"
        price_kind = "placement_fee_unquoted"
    elif quote_signal:
        price_status = "quote_required"
        commercial_model = "quote_required"
        price_kind = "placement_fee_unquoted"
    else:
        price_status = "unknown"
        commercial_model = "unknown"
        price_kind = "unknown"
    selected_currencies = currencies or review_currencies
    currency = (
        selected_currencies[0]
        if len(selected_currencies) == 1
        else "MIXED"
        if selected_currencies
        else None
    )
    selected_prices = prices or review_prices
    price_values = [amount for amount, _ in selected_prices]

    turnaround_days: list[int] = []
    for match in TURNAROUND_RE.finditer(text):
        high = int(match.group("high") or match.group("low"))
        unit = match.group("unit").lower()
        turnaround_days.append(high * 7 if "week" in unit or "semana" in unit else high)
        matches.append(("turnaround", match))

    return {
        "placement_types": sorted(placement_types),
        "promise_level": promise_level,
        "promise_probability": PROMISE_PROBABILITY[promise_level],
        "commercial_model": commercial_model,
        "price_status": price_status,
        "price_kind": price_kind,
        "price_min": min(price_values)
        if price_values and currency != "MIXED"
        else None,
        "price_max": max(price_values)
        if price_values and currency != "MIXED"
        else None,
        "currency": currency,
        "link_attributes": sorted(link_attributes),
        "placement_locations": sorted(placement_locations),
        "permanence": permanence,
        "content_responsibility": content_responsibility,
        "turnaround_days_max": max(turnaround_days) if turnaround_days else None,
        "evidence_json": (_evidence(text, matches) + price_evidence)[:40],
    }


def _input_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _init_terms_db(
    path: Path, *, input_db: Path, config: TermsConfig
) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(TERMS_SCHEMA)
    identity = {
        "schema_version": str(TERMS_SCHEMA_VERSION),
        "extractor_version": str(TERMS_EXTRACTOR_VERSION),
        "input_sha256": _input_digest(input_db),
        "config": _json(asdict(config)),
    }
    existing = dict(connection.execute("SELECT key,value FROM terms_meta"))
    if existing:
        mismatches = {
            key: (existing.get(key), value)
            for key, value in identity.items()
            if existing.get(key) != value
        }
        if mismatches:
            connection.close()
            raise ValueError(f"terms resume identity mismatch: {mismatches}")
    else:
        with connection:
            connection.executemany(
                "INSERT INTO terms_meta(key,value) VALUES (?,?)",
                sorted(identity.items()),
            )
    return connection


def _load_pages(input_db: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{input_db}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in connection.execute(
                """
                SELECT url,registered_domain,fetch_time,warc_filename,
                       warc_record_offset,warc_record_length
                FROM outreach_pages
                ORDER BY registered_domain,url
                """
            )
        ]
    finally:
        connection.close()


def _extract_warc_terms(row: Mapping[str, Any], config: TermsConfig) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(config.retries + 1):
        try:
            raw = fetch_warc_record(
                str(row["warc_filename"]),
                int(row["warc_record_offset"]),
                int(row["warc_record_length"]),
            )
            page_html, _, _ = parse_warc_html(raw, config.max_html_bytes)
            if page_html is None:
                raise ValueError("WARC record is not HTML")
            return {
                "url": str(row["url"]),
                "registered_domain": str(row["registered_domain"]),
                "source": "warc",
                "analyzed_at": _utc_now(),
                "fetch_status": "ok",
                **extract_placement_terms(page_html),
            }
        except (
            ArchiveLoadFailed,
            BotoCoreError,
            ClientError,
            RequestException,
            EOFError,
            TimeoutError,
            OSError,
            ValueError,
        ) as exc:
            last_error = exc
            if attempt < config.retries:
                time.sleep(config.retry_backoff * (2**attempt))
    assert last_error is not None
    return {
        "url": str(row["url"]),
        "registered_domain": str(row["registered_domain"]),
        "source": "warc",
        "analyzed_at": _utc_now(),
        "fetch_status": "error",
        "placement_types": [],
        "promise_level": "unclear",
        "promise_probability": PROMISE_PROBABILITY["unclear"],
        "commercial_model": "unknown",
        "price_status": "unknown",
        "price_kind": "unknown",
        "link_attributes": [],
        "placement_locations": [],
        "permanence": "unspecified",
        "content_responsibility": "unspecified",
        "evidence_json": [],
        "error_kind": type(last_error).__name__,
        "error_detail": str(last_error)[:500],
    }


TERMS_COLUMNS = (
    "url",
    "registered_domain",
    "source",
    "analyzed_at",
    "fetch_status",
    "placement_types",
    "promise_level",
    "promise_probability",
    "commercial_model",
    "price_status",
    "price_kind",
    "price_min",
    "price_max",
    "currency",
    "link_attributes",
    "placement_locations",
    "permanence",
    "content_responsibility",
    "turnaround_days_max",
    "evidence_json",
    "error_kind",
    "error_detail",
)


def _save_terms(connection: sqlite3.Connection, row: Mapping[str, Any]) -> None:
    values = dict(row)
    for key in (
        "placement_types",
        "link_attributes",
        "placement_locations",
        "evidence_json",
    ):
        values[key] = _json(values.get(key, []))
    placeholders = ",".join("?" for _ in TERMS_COLUMNS)
    with connection:
        connection.execute(
            f"INSERT OR REPLACE INTO placement_terms "
            f"({','.join(TERMS_COLUMNS)}) VALUES ({placeholders})",
            tuple(values.get(column) for column in TERMS_COLUMNS),
        )


def build_placement_terms(
    *,
    input_db: str | Path,
    out_db: str | Path,
    workers: int = 24,
    max_pages: int | None = None,
    fetch_source: str = "s3",
    config: TermsConfig | None = None,
    progress: Callable[[str], None] | None = None,
) -> TermsSummary:
    """Fetch exact archived pages and build a resumable commercial-terms DB."""
    if workers < 1:
        raise ValueError("workers must be positive")
    if max_pages is not None and max_pages < 1:
        raise ValueError("max_pages must be positive")
    if fetch_source not in ("s3", "https"):
        raise ValueError("fetch_source must be s3 or https")
    config = config or TermsConfig()
    input_path = Path(input_db)
    rows = _load_pages(input_path)
    scheduled_rows = rows[:max_pages] if max_pages is not None else rows
    connection = _init_terms_db(Path(out_db), input_db=input_path, config=config)
    if fetch_source == "s3":
        enable_s3(pool_size=max(32, workers * 2))
    completed = {
        str(row[0]) for row in connection.execute("SELECT url FROM placement_terms")
    }
    pending = [row for row in scheduled_rows if str(row["url"]) not in completed]
    if progress:
        progress(f"terms: resume={len(completed)} pending={len(pending)}")
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_extract_warc_terms, row, config): row for row in pending
        }
        for index, future in enumerate(as_completed(futures), start=1):
            _save_terms(connection, future.result())
            if progress and (index % 100 == 0 or index == len(pending)):
                elapsed = max(0.001, time.monotonic() - started)
                progress(
                    f"terms: {min(len(completed), len(scheduled_rows)) + index}/"
                    f"{len(scheduled_rows)} "
                    f"rate={index / elapsed * 60:.1f}/min"
                )
    counts = connection.execute(
        """
        SELECT COUNT(*),
               SUM(fetch_status='ok'),
               SUM(price_status IN ('advertised','advertised_mixed_currency','free')),
               SUM(promise_level!='unclear')
        FROM placement_terms
        """
    ).fetchone()
    connection.close()
    return TermsSummary(
        input_pages=len(rows),
        scheduled_pages=len(scheduled_rows),
        completed_pages=int(counts[0] or 0),
        successful_pages=int(counts[1] or 0),
        priced_pages=int(counts[2] or 0),
        explicit_promise_pages=int(counts[3] or 0),
    )


def build_terms_report(out_db: str | Path) -> dict[str, Any]:
    """Summarize term coverage and distributions without changing the DB."""
    connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = [
            dict(row) for row in connection.execute("SELECT * FROM placement_terms")
        ]
        metadata = dict(connection.execute("SELECT key,value FROM terms_meta"))
        report: dict[str, Any] = {
            "schema_version": int(metadata.get("schema_version", TERMS_SCHEMA_VERSION)),
            "extractor_version": int(
                metadata.get("extractor_version", TERMS_EXTRACTOR_VERSION)
            ),
            "pages": len(rows),
            "fetch_status": dict(Counter(str(row["fetch_status"]) for row in rows)),
            "promise_level": dict(Counter(str(row["promise_level"]) for row in rows)),
            "price_status": dict(Counter(str(row["price_status"]) for row in rows)),
            "commercial_model": dict(
                Counter(str(row["commercial_model"]) for row in rows)
            ),
            "permanence": dict(Counter(str(row["permanence"]) for row in rows)),
            "content_responsibility": dict(
                Counter(str(row["content_responsibility"]) for row in rows)
            ),
        }
        for field in (
            "placement_types",
            "link_attributes",
            "placement_locations",
        ):
            report[field] = dict(
                Counter(
                    item for row in rows for item in json.loads(str(row[field]) or "[]")
                )
            )
        report["advertised_price"] = [
            {
                "currency": str(row[0]),
                "pages": int(row[1]),
                "minimum": round(float(row[2]), 2),
                "maximum": round(float(row[3]), 2),
                "average_midpoint": round(float(row[4]), 2),
            }
            for row in connection.execute(
                """
                SELECT currency,COUNT(*),MIN(price_min),MAX(price_max),
                       AVG((price_min+price_max)/2.0)
                FROM placement_terms
                WHERE price_status='advertised'
                  AND currency IS NOT NULL
                  AND price_min IS NOT NULL
                  AND price_max IS NOT NULL
                GROUP BY currency ORDER BY currency
                """
            )
        ]
        report["quick_check"] = str(
            connection.execute("PRAGMA quick_check").fetchone()[0]
        )
        return report
    finally:
        connection.close()


def write_terms_outputs(
    out_db: str | Path,
    *,
    report_path: str | Path | None = None,
    csv_path: str | Path | None = None,
) -> dict[str, Any]:
    """Write a deterministic JSON summary and optional full terms CSV."""
    report = build_terms_report(out_db)
    if report_path:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if csv_path:
        connection = sqlite3.connect(f"file:{Path(out_db)}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            rows = list(
                connection.execute(
                    "SELECT * FROM placement_terms ORDER BY registered_domain,url"
                )
            )
        finally:
            connection.close()
        path = Path(csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            if rows:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(dict(row) for row in rows)
    return report
