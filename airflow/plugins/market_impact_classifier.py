import csv
import html
import re
from collections import Counter
from pathlib import Path


TOKEN_RE = re.compile(r"[가-힣A-Za-z0-9%+-]{2,}")
POSITIVE_REGEXES = [
    ("금리인하", r"(금리|기준금리).{0,8}(인하|내린|내려|낮춘)"),
    ("규제완화", r"(규제|관세|세금|대출규제|상한제).{0,10}(완화|철회|폐지|면제|해제)"),
    ("실적개선", r"(실적|매출|영업이익|순이익|수출|수주).{0,10}(개선|증가|호조|확대|최대|반등|상향|흑자)"),
    ("수요회복", r"(수요|소비|내수|출하|생산).{0,10}(회복|반등|개선|증가|확대)"),
    ("리스크완화", r"(휴전|타결|완화|해소|정상화|안정)"),
    ("지원확대", r"(지원|보조금|추경|예산).{0,10}(확대|증액|추가)"),
]
NEGATIVE_REGEXES = [
    ("금리인상", r"(금리|기준금리).{0,8}(인상|올린|올려|높인)"),
    ("물가상승", r"(물가|인플레|원가|유가|환율).{0,10}(상승|급등|불안|자극)"),
    ("실적부진", r"(실적|매출|영업이익|순이익|수출|수주).{0,10}(감소|부진|둔화|적자|하향|악화)"),
    ("규제강화", r"(규제|관세|세금|제재|압박|대출규제).{0,10}(강화|인상|부과|확대)"),
    ("리스크확대", r"(전쟁|갈등|충돌|봉쇄|리스크|우려|불확실성|침체|경착륙|쇼크|충격)"),
    ("수요부진", r"(수요|소비|내수|출하|생산).{0,10}(둔화|위축|감소|부진|축소)"),
]
GENERIC_POSITIVE_WORDS = {
    "호조", "개선", "반등", "회복", "상향", "확대", "증가", "흑자", "최대", "수혜", "완화", "타결", "정상화",
}
GENERIC_NEGATIVE_WORDS = {
    "부진", "악화", "둔화", "감소", "적자", "우려", "충격", "급락", "긴축", "제재", "봉쇄", "갈등", "침체",
}
MACRO_SCOPE_HINTS = {
    "금리", "기준금리", "환율", "원달러", "원유", "유가", "물가", "인플레", "관세", "수출", "무역",
    "고용", "소비", "실업", "추경", "예산", "전쟁", "갈등", "대출", "가계부채", "통화정책", "연준", "한은",
}
COMPANY_SUFFIXES = [
    "전자", "화학", "금융", "증권", "은행", "카드", "그룹", "모터스", "건설", "제약", "바이오",
    "중공업", "에너지", "통신", "조선", "항공", "해운", "식품", "물산", "솔루션", "로보틱스", "홀딩스",
    "케미칼", "시스템", "사이언스", "생명과학", "테크놀로지", "테크놀러지", "엔지니어링", "네트웍스",
    "머티리얼즈", "헬스케어", "코퍼레이션", "리츠", "스팩", "인베스트먼트", "바이오텍", "로직스", "바이오팜", "지주",
]
GENERIC_COMPANY_EXCLUDES = {
    "ETF", "ETN", "IB", "시장", "경제", "에너지",
    "케미칼", "시스템", "사이언스", "생명과학", "테크놀로지", "테크놀러지", "엔지니어링", "네트웍스",
    "머티리얼즈", "헬스케어", "코퍼레이션", "리츠", "스팩", "인베스트먼트", "바이오텍", "로직스", "바이오팜", "지주",
}


# 제목 1개를 KG CSV 기반 시장영향 표준화 결과로 변환한다.
def classify_market_impact(title):
    normalized_title = re.sub(r"\s+", " ", html.unescape(title or "")).strip()
    normalized_title = re.sub(r"\s*[-|]\s*매일경제.*$", "", normalized_title)
    normalized_title = re.sub(r"\s*\|\s*MK.*$", "", normalized_title, flags=re.I).strip()
    plugin_dir = Path(__file__).resolve().parent / "mk_kg_data"
    repo_dir = Path(__file__).resolve().parents[2] / "knowledge graph" / "out"
    kg_dir = next((path for path in [plugin_dir, repo_dir] if (path / "mk_kg_nodes.csv").exists() and (path / "mk_kg_edges.csv").exists()), None)
    if kg_dir is None:
        return {
            "title": normalized_title,
            "impact_scope": None,
            "scope_evidence": None,
            "driver_category": None,
            "driver_evidence": None,
            "impact_direction": None,
            "direction_evidence": None,
            "matched_entities": None,
        }

    cache_by_dir = getattr(classify_market_impact, "_kg_cache", {})
    cache_key = str(kg_dir)
    if cache_key not in cache_by_dir:
        node_rows = list(csv.DictReader((kg_dir / "mk_kg_nodes.csv").open("r", encoding="utf-8-sig", newline="")))
        edge_rows = list(csv.DictReader((kg_dir / "mk_kg_edges.csv").open("r", encoding="utf-8-sig", newline="")))
        node_labels = {row["node_id"]: row["label"] for row in node_rows}
        factor_map = {}
        sector_map = {}
        for row in edge_rows:
            alias = node_labels.get(row["src_id"], row["src_id"].replace("ENTITY::", ""))
            if row["relation"] == "maps_to_factor":
                factor_map[alias] = node_labels.get(row["dst_id"], row["dst_id"].replace("FACTOR::", ""))
                GENERIC_COMPANY_EXCLUDES.add(alias)
            if row["relation"] == "maps_to_sector":
                sector_map[alias] = node_labels.get(row["dst_id"], row["dst_id"].replace("SECTOR::", ""))
                GENERIC_COMPANY_EXCLUDES.add(alias)
        cache_by_dir[cache_key] = {
            "factor_map": factor_map,
            "sector_map": sector_map,
            "factor_aliases": sorted(factor_map, key=len, reverse=True),
            "sector_aliases": sorted(sector_map, key=len, reverse=True),
        }
        setattr(classify_market_impact, "_kg_cache", cache_by_dir)

    cache = cache_by_dir[cache_key]
    lowered_title = normalized_title.lower()
    factor_counter = Counter()
    factor_evidence = []
    sector_evidence = []
    sectors = []
    for alias in cache["factor_aliases"]:
        if alias.lower() in lowered_title:
            factor_counter[cache["factor_map"][alias]] += 1
            if alias not in factor_evidence:
                factor_evidence.append(alias)
    for alias in cache["sector_aliases"]:
        if alias.lower() in lowered_title:
            sectors.append(cache["sector_map"][alias])
            if alias not in sector_evidence:
                sector_evidence.append(alias)
    company_terms = []
    for token in TOKEN_RE.findall(normalized_title):
        if len(token) < 3 or token in GENERIC_COMPANY_EXCLUDES:
            continue
        if any(token.endswith(suffix) for suffix in COMPANY_SUFFIXES) and token not in company_terms:
            company_terms.append(token)
    macro_hits = [alias for alias in factor_evidence if alias in MACRO_SCOPE_HINTS]
    if company_terms:
        impact_scope = "기업"
        scope_evidence = company_terms
    elif macro_hits:
        impact_scope = "시장전체"
        scope_evidence = macro_hits
    elif sector_evidence:
        impact_scope = "섹터"
        scope_evidence = sector_evidence
    elif factor_evidence:
        impact_scope = "시장전체"
        scope_evidence = factor_evidence[:3]
    else:
        impact_scope = "시장전체"
        scope_evidence = []
    if factor_counter:
        driver_category = sorted(factor_counter.items(), key=lambda item: (-item[1], item[0]))[0][0]
        driver_evidence = [alias for alias in factor_evidence if cache["factor_map"].get(alias) == driver_category][:5]
    elif sectors:
        driver_category = "산업수요/공급망"
        driver_evidence = sector_evidence[:5]
    else:
        driver_category = None
        driver_evidence = []
    positive_hits = []
    negative_hits = []
    for label, pattern in POSITIVE_REGEXES:
        if re.search(pattern, normalized_title, re.I):
            positive_hits.append(label)
    for label, pattern in NEGATIVE_REGEXES:
        if re.search(pattern, normalized_title, re.I):
            negative_hits.append(label)
    title_tokens = set(TOKEN_RE.findall(normalized_title))
    positive_hits.extend(sorted(title_tokens.intersection(GENERIC_POSITIVE_WORDS)))
    negative_hits.extend(sorted(title_tokens.intersection(GENERIC_NEGATIVE_WORDS)))
    positive_hits = list(dict.fromkeys(positive_hits))
    negative_hits = list(dict.fromkeys(negative_hits))
    if positive_hits and negative_hits:
        impact_direction = "mixed"
        direction_evidence = positive_hits[:3] + negative_hits[:3]
    elif positive_hits:
        impact_direction = "positive"
        direction_evidence = positive_hits[:5]
    elif negative_hits:
        impact_direction = "negative"
        direction_evidence = negative_hits[:5]
    else:
        impact_direction = "neutral"
        direction_evidence = []
    matched_entities = []
    for value in scope_evidence + driver_evidence + direction_evidence:
        if value not in matched_entities:
            matched_entities.append(value)
    return {
        "title": normalized_title,
        "impact_scope": impact_scope,
        "scope_evidence": ", ".join(scope_evidence) or None,
        "driver_category": driver_category,
        "driver_evidence": ", ".join(driver_evidence) or None,
        "impact_direction": impact_direction,
        "direction_evidence": ", ".join(direction_evidence) or None,
        "matched_entities": ", ".join(matched_entities) or None,
    }
