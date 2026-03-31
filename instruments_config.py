"""
Shared configuration: 100 instruments with ISINs and names.
Imported by seed_db.py, market_data_publisher.py, and rfq_publisher.py.
"""

_COUNTRY_PREFIXES = ["GB", "US", "DE", "FR", "JP", "NL", "SE", "CH", "AU", "CA"]

_ISSUERS = [
    "Acme Corp", "Beta Energy", "Gamma Finance", "Delta Telecom", "Epsilon Health",
    "Zeta Industrials", "Eta Retail", "Theta Media", "Iota Transport", "Kappa Mining",
    "Lambda Tech", "Mu Pharma", "Nu Utilities", "Xi Chemicals", "Omicron Food",
    "Pi Insurance", "Rho Aerospace", "Sigma Automotive", "Tau Banking", "Upsilon RE",
]

_COUPONS = ["1.5%", "2.0%", "2.5%", "3.0%", "3.5%", "4.0%", "4.5%", "5.0%"]
_MATURITIES = ["2026", "2027", "2028", "2029", "2030", "2031", "2032", "2033", "2034", "2035"]
_TYPES = ["Bond", "Note", "MTN", "Senior Note", "Sub Note"]

# Build 100 instruments deterministically
INSTRUMENTS: dict[str, str] = {}

for i in range(100):
    country = _COUNTRY_PREFIXES[i % len(_COUNTRY_PREFIXES)]
    serial = f"{i + 1:06d}"
    isin = f"{country}00B{serial}"

    issuer = _ISSUERS[i % len(_ISSUERS)]
    coupon = _COUPONS[i % len(_COUPONS)]
    maturity = _MATURITIES[i % len(_MATURITIES)]
    bond_type = _TYPES[i % len(_TYPES)]
    name = f"{issuer} {coupon} {maturity} {bond_type}"

    INSTRUMENTS[isin] = name

ISINS: list[str] = list(INSTRUMENTS.keys())
