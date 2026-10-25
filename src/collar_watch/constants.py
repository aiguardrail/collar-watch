"""
Sabitler: Robinhood Chain'deki bilinen quote asset'ler ve hook'lar.

Kullanım:
    from .constants import is_quote, hook_name
"""

# Robinhood Chain'de bilinen quote asset'ler.
# Bunlar "yeni token" değil — havuzun karşı tarafında duran para birimleri.
QUOTE_ASSETS = {
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168": "USDG",
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73": "WETH",
    # 304 havuzda görüldü, stablecoin olduğu yüksek olasılıklı ama Blockscout'ta
    # henüz doğrulanmadı. Doğrulanınca sembolü güncelle.
    "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea": "UNKNOWN_STABLE",
}

ZERO_ADDR = "0x" + "0" * 40

# Bilinen hook'lar. 0x4e34... = Pons Launchpad (915 havuz, launchpad hook'u).
# Diğerleri: bilinmeyen ama sık görülen hook'lar. Kontratları incelendikçe isim ver.
HOOK_REGISTRY = {
    "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544": "pons_v2",
    "0x2c668601193e4a081dd82604785e436aff6a2acc": "unknown_2",
    "0x992118de23b4ebe9f527dacbbb7d8d4bbbf8e044": "unknown_3",
    "0x4eb1976978756bd56802d8162f2271844924e0cc": "unknown_4",
    "0x48b8f6ad3a1b4aa477314c9a23035b8f84dde8cc": "unknown_5",
}


def is_quote(addr: str) -> bool:
    """Verilen adres bilinen bir quote asset mi?"""
    return addr.lower() in QUOTE_ASSETS


def hook_name(addr: str) -> str:
    """Hook adresini okunabilir isme çevir. Bilinmiyorsa 'unknown'."""
    a = addr.lower()
    if a == ZERO_ADDR:
        return "none"
    return HOOK_REGISTRY.get(a, "unknown")
