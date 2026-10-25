"""
Sabitler: Robinhood Chain'deki bilinen quote asset'ler ve hook'lar.

Kullanım:
    from .constants import QUOTE_ASSETS, ZERO_ADDR, is_quote, hook_name
"""

# Robinhood Chain'de bilinen quote asset'ler.
# Bunlar "yeni token" değil — havuzun karşı tarafında duran para birimleri.
QUOTE_ASSETS = {
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168": "USDG",
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73": "WETH",
    # 48h sample'da 707 havuzda quote olarak göründü, stablecoin olduğu kesin
    # ama Blockscout'ta sembolü henüz doğrulanmadı. Doğrulanınca güncelle.
    "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea": "UNKNOWN_STABLE",
    # Sample'da 51 havuzda quote, bir örnekte token olarak da görüldü.
    # Robinhood Chain'in USD9 stablecoin'i olma olasılığı yüksek.
    "0x999999990237e901c537bbd768e09562be02efa5": "USD9",
}

ZERO_ADDR = "0x" + "0" * 40

# Bilinen hook'lar. Sıralama: 48h counter-summary'deki görülme sıklığına göre.
# 0x4e34... = Pons Launchpad (2125 havuz, %26 — en büyük)
# 0xb091... = ikinci büyük (533 havuz, %6.6) — muhtemelen başka bir launchpad
# Diğerleri: bilinmeyen ama sık görülen hook'lar. Kontratları incelendikçe isim ver.
HOOK_REGISTRY = {
    "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544": "pons_v2",      # 2125 havuz
    "0xb091ec27074c7a0c1ba896c063cc677f89202888": "unknown_2_big", # 533 havuz
    "0x2c668601193e4a081dd82604785e436aff6a2acc": "unknown_3",     # 339 havuz
    "0x992118de23b4ebe9f527dacbbb7d8d4bbbf8e044": "unknown_4",     # 159 havuz
    "0x15e5e7206c661a1e881f5cd33a94eaf2328fa8cc": "unknown_5",     # 75 havuz
    "0x2dbe141bc56a66d0ff4d9e5654708d44042f20cc": "unknown_6",     # 28 havuz
    "0x4eb1976978756bd56802d8162f2271844924e0cc": "unknown_7",     # 27 havuz
    "0x037bf303462bb3cde80038a1945cd10af1ebdacc": "unknown_8",     # 26 havuz
    "0x48b8f6ad3a1b4aa477314c9a23035b8f84dde8cc": "unknown_9",     # 18 havuz
    "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044": "unknown_10",    # 14 havuz
    # Sample'da tek tük görünenler (daha az sıklıkta, doğrulanmadı):
    "0x442460d3f1d40d13eed678d540427efce7b520cc": "unknown_11",
    "0xb903533e4e011e43659816e2a763490feddba080": "unknown_12",
    "0x2743888bbfda812083eb457d4c8680642b35c0cc": "unknown_13",
    "0xe2cb330ec35a68098edf079ad872e76916f94088": "unknown_14",
}


def is_quote(addr: str) -> bool:
    """Verilen adres bilinen bir quote asset mi?"""
    return addr.lower() in QUOTE_ASSETS


def quote_name(addr: str) -> str:
    """Quote asset'in insan-okur ismini döndür. Bilinmiyorsa adresin ilk 10 karakteri."""
    a = addr.lower()
    return QUOTE_ASSETS.get(a, a[:10] + "...")


def hook_name(addr: str) -> str:
    """Hook adresini okunabilir isme çevir. Bilinmiyorsa 'unknown'."""
    a = addr.lower()
    if a == ZERO_ADDR:
        return "none"
    return HOOK_REGISTRY.get(a, "unknown")
