"""ccTLD -> country mapping and priority-weighted budget allocation.

Lets the pipeline be pointed at specific countries (by ccTLD) and split the
crawl budget between them according to user-defined priority weights, instead
of an all-or-nothing global crawl.
"""
import json
import os
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

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
    # --- Remaining ccTLDs referenced by the country-taxonomy buckets ---
    "ad": "Andorra", "al": "Albania", "am": "Armenia", "az": "Azerbaijan",
    "ba": "Bosnia and Herzegovina", "bf": "Burkina Faso", "bi": "Burundi", "bj": "Benin",
    "bv": "Bouvet Island", "cd": "DR Congo", "cf": "Central African Republic",
    "cg": "Congo", "cv": "Cape Verde", "cy": "Cyprus", "dj": "Djibouti",
    "eh": "Western Sahara", "er": "Eritrea", "ga": "Gabon", "ge": "Georgia",
    "gm": "Gambia", "gn": "Guinea", "gq": "Equatorial Guinea", "gw": "Guinea-Bissau",
    "hr": "Croatia", "io": "British Indian Ocean Territory", "km": "Comoros",
    "kp": "North Korea", "li": "Liechtenstein", "lr": "Liberia", "ls": "Lesotho",
    "lu": "Luxembourg", "mc": "Monaco", "md": "Moldova", "me": "Montenegro",
    "mk": "North Macedonia", "mo": "Macau", "mr": "Mauritania", "mt": "Malta",
    "ne": "Niger", "nu": "Niue", "re": "Reunion", "rs": "Serbia", "sc": "Seychelles",
    "sh": "Saint Helena", "si": "Slovenia", "sl": "Sierra Leone", "sm": "San Marino",
    "so": "Somalia", "ss": "South Sudan", "st": "Sao Tome and Principe", "sz": "Eswatini",
    "td": "Chad", "tg": "Togo", "tl": "Timor-Leste", "tv": "Tuvalu", "va": "Vatican",
    "yt": "Mayotte",
    # --- Latin America & Caribbean (additional) ---
    "bz": "Belize", "gy": "Guyana", "sr": "Suriname", "ht": "Haiti",
    "jm": "Jamaica", "tt": "Trinidad and Tobago", "bb": "Barbados",
    "bs": "Bahamas",
    # --- Other African countries ---
    "dz": "Algeria", "tn": "Tunisia", "ly": "Libya", "sd": "Sudan",
    "gh": "Ghana", "tz": "Tanzania", "ug": "Uganda", "et": "Ethiopia",
    "sn": "Senegal", "ci": "Cote d'Ivoire", "cm": "Cameroon", "zm": "Zambia",
    "zw": "Zimbabwe", "ao": "Angola", "mz": "Mozambique", "rw": "Rwanda",
    "bw": "Botswana", "na": "Namibia", "ml": "Mali", "mg": "Madagascar",
    "mu": "Mauritius", "mw": "Malawi",
    # --- Other Asian & Middle Eastern countries ---
    "lk": "Sri Lanka", "np": "Nepal", "mm": "Myanmar", "kh": "Cambodia",
    "la": "Laos", "mn": "Mongolia", "uz": "Uzbekistan", "kg": "Kyrgyzstan",
    "tj": "Tajikistan", "tm": "Turkmenistan", "af": "Afghanistan",
    "bt": "Bhutan", "bn": "Brunei", "mv": "Maldives", "kw": "Kuwait",
    "qa": "Qatar", "bh": "Bahrain", "om": "Oman", "jo": "Jordan",
    "lb": "Lebanon", "sy": "Syria", "ye": "Yemen", "ps": "Palestine",
}


def country_name(tld: Optional[str]) -> str:
    return CCTLD_COUNTRIES.get((tld or "").lower(), (tld or "").upper())


def load_priorities(
    path: Optional[str] = None,
    countries: Optional[Iterable[str]] = None,
) -> Dict[str, float]:
    """Load {cctld: weight} from a JSON file, or default to equal weight 1.0
    for the given list of ccTLDs (or all known ccTLDs if none given)."""
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k.lower(): float(v) for k, v in raw.items()}

    tlds = countries if countries else list(CCTLD_COUNTRIES.keys())
    return {t.lower(): 1.0 for t in tlds}


def load_category_map(path: str) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """Load a categories JSON: {"Category name": ["co", "cl", ...], ...}.

    A category groups one or more ccTLDs that share a single budget (so a
    regional bucket like "Other Africa" spanning 20 ccTLDs is filled to one
    combined limit, not per-ccTLD). Returns (categories, tld_to_category):
      categories       -- the raw {name: [tlds]} dict (preserves order/keys)
      tld_to_category  -- flat {tld: name} lookup used during discovery
    Raises if a ccTLD is listed under more than one category, since that would
    make budget accounting ambiguous.
    """
    with open(path, "r", encoding="utf-8") as f:
        categories = json.load(f)
    tld_to_category: Dict[str, str] = {}
    for name, tlds in categories.items():
        for t in tlds:
            t = t.lower()
            prev = tld_to_category.get(t)
            if prev is not None and prev != name:
                raise ValueError(
                    f"ccTLD '{t}' is assigned to two categories: '{prev}' and '{name}' "
                    f"-- each ccTLD must belong to exactly one category"
                )
            tld_to_category[t] = name
    return categories, tld_to_category


def allocate_budget(priorities: Mapping[str, float], total: int) -> Dict[str, int]:
    """Split a total page/URL budget across countries proportionally to their weight."""
    weight_sum = sum(priorities.values()) or 1.0
    budgets: Dict[str, int] = {}
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
