"""ccTLD -> country mapping and priority-weighted budget allocation.

Lets the pipeline be pointed at specific countries (by ccTLD) and split the
crawl budget between them according to user-defined priority weights, instead
of an all-or-nothing global crawl.
"""
import json
import os

# Common ccTLD -> country name. Not exhaustive, but covers the standard ISO
# ccTLD set used by Common Crawl's url_host_tld column. Extend as needed.
CCTLD_COUNTRIES = {
    "ru": "Russia", "ua": "Ukraine", "by": "Belarus", "kz": "Kazakhstan",
    "us": "United States", "uk": "United Kingdom", "de": "Germany", "fr": "France",
    "it": "Italy", "es": "Spain", "pt": "Portugal", "nl": "Netherlands",
    "be": "Belgium", "ch": "Switzerland", "at": "Austria", "se": "Sweden",
    "no": "Norway", "dk": "Denmark", "fi": "Finland", "pl": "Poland",
    "cz": "Czech Republic", "sk": "Slovakia", "hu": "Hungary", "ro": "Romania",
    "bg": "Bulgaria", "gr": "Greece", "tr": "Turkey", "ie": "Ireland",
    "is": "Iceland", "lt": "Lithuania", "lv": "Latvia", "ee": "Estonia",
    "cn": "China", "jp": "Japan", "kr": "South Korea", "in": "India",
    "id": "Indonesia", "th": "Thailand", "vn": "Vietnam", "ph": "Philippines",
    "my": "Malaysia", "sg": "Singapore", "pk": "Pakistan", "bd": "Bangladesh",
    "br": "Brazil", "mx": "Mexico", "ar": "Argentina", "cl": "Chile",
    "co": "Colombia", "pe": "Peru", "ve": "Venezuela", "uy": "Uruguay",
    "ec": "Ecuador", "bo": "Bolivia", "py": "Paraguay", "cr": "Costa Rica",
    "pa": "Panama", "gt": "Guatemala", "hn": "Honduras", "sv": "El Salvador",
    "ni": "Nicaragua", "do": "Dominican Republic", "cu": "Cuba", "pr": "Puerto Rico",
    "ca": "Canada", "au": "Australia", "nz": "New Zealand", "za": "South Africa",
    "eg": "Egypt", "ng": "Nigeria", "ke": "Kenya", "ma": "Morocco",
    "sa": "Saudi Arabia", "ae": "United Arab Emirates", "il": "Israel",
    "ir": "Iran", "iq": "Iraq", "hk": "Hong Kong", "tw": "Taiwan",
}


def country_name(tld: str) -> str:
    return CCTLD_COUNTRIES.get((tld or "").lower(), (tld or "").upper())


def load_priorities(path: str = None, countries=None) -> dict:
    """Load {cctld: weight} from a JSON file, or default to equal weight 1.0
    for the given list of ccTLDs (or all known ccTLDs if none given)."""
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k.lower(): float(v) for k, v in raw.items()}

    tlds = countries if countries else list(CCTLD_COUNTRIES.keys())
    return {t.lower(): 1.0 for t in tlds}


def allocate_budget(priorities: dict, total: int) -> dict:
    """Split a total page/URL budget across countries proportionally to their weight."""
    weight_sum = sum(priorities.values()) or 1.0
    budgets = {}
    remaining = total
    tlds = list(priorities.keys())
    for i, tld in enumerate(tlds):
        if i == len(tlds) - 1:
            budgets[tld] = remaining
        else:
            share = int(round(total * priorities[tld] / weight_sum))
            share = min(share, remaining)
            budgets[tld] = share
            remaining -= share
    return budgets
