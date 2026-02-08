#!/usr/bin/env python3
"""
GEO Skin Fibrosis 数据增量更新脚本
"""

import os
import json
import time
import re
import requests
from datetime import datetime
from Bio import Entrez

NCBI_EMAIL = os.environ.get('NCBI_EMAIL', '')
NCBI_API_KEY = os.environ.get('NCBI_API_KEY', '')
MINIMAX_API_KEY = os.environ.get('MINIMAX_API_KEY', '')

SEARCH_CONFIG = {
    "keywords": [
        "skin fibrosis", "dermal fibrosis", "scleroderma skin",
        "cutaneous fibrosis", "systemic sclerosis skin",
        "keloid", "hypertrophic scar"
    ],
    "organisms": ["Homo sapiens", "Mus musculus"],
    "data_types": [
        "Expression profiling by high throughput sequencing",
        "Methylation profiling by array",
        "Methylation profiling by high throughput sequencing",
        "Genome binding/occupancy profiling by high throughput sequencing"
    ]
}

DATA_FILE = "data/geo_data.json"


def setup_entrez():
    Entrez.email = NCBI_EMAIL
    if NCBI_API_KEY:
        Entrez.api_key = NCBI_API_KEY


def build_query():
    keyword_query = " OR ".join([f'"{kw}"' for kw in SEARCH_CONFIG["keywords"]])
    org_query = " OR ".join([f'"{org}"[Organism]' for org in SEARCH_CONFIG["organisms"]])
    type_query = " OR ".join([f'"{t}"[DataSet Type]' for t in SEARCH_CONFIG["data_types"]])
    date_query = "0030[MDAT]"
    return f"({keyword_query}) AND ({org_query}) AND ({type_query}) AND {date_query}"


def search_geo():
    query = build_query()
    print(f"搜索查询: {query[:100]}...")
    handle = Entrez.esearch(db="gds", term=query, retmax=500, usehistory="y")
    results = Entrez.read(handle)
    handle.close()
    return results.get("IdList", [])


def fetch_summaries(id_list):
    if not id_list:
        return []
    handle = Entrez.esummary(db="gds", id=",".join(id_list))
    records = Entrez.read(handle)
    handle.close()
    return records


def clean_pubmed_ids(pubmed_str):
    if not pubmed_str:
        return ""
    numbers = re.findall(r'IntegerElement\((\d+)', str(pubmed_str))
    if numbers:
        return "; ".join(numbers)
    numbers = re.findall(r'\d+', str(pubmed_str))
    if numbers:
        return "; ".join(numbers)
    return str(pubmed_str)


def generate_ai_summary(title, summary, data_type):
    if not MINIMAX_API_KEY:
        return ""

    prompt = f"""请用中文为以下GEO数据集生成一个精炼的科研摘要（80-120字）：

标题: {title}
数据类型: {data_type}
研究摘要: {summary[:800]}

请直接输出中文摘要："""

    try:
        response = requests.post(
            'https://api.minimaxi.com/v1/chat/completions',
            headers={
                "Authorization": f'Bearer {MINIMAX_API_KEY}',
                "Content-Type": "application/json"
            },
            json={
                "model": "MiniMax-M2.1",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1500,
                "temperature": 0.7
            },
            timeout=60
        )
        if response.status_code == 200:
            content = response.json()["choices"][0]["message"]["content"]
            return re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
    except Exception as e:
        print(f"AI 摘要生成失败: {e}")
    return ""


def parse_record(record):
    accession = record.get("Accession", "")
    if not accession.startswith("GSE"):
        return None

    pubmed_ids = record.get("PubMedIds", [])
    pubmed_str = clean_pubmed_ids("; ".join(str(p) for p in pubmed_ids) if pubmed_ids else "")

    title = record.get("title", "")
    summary = record.get("summary", "")
    data_type = "bulk RNA-seq"

    ai_summary = generate_ai_summary(title, summary, data_type)
    if ai_summary:
        time.sleep(1)

    return {
        "Accession": accession,
        "Title": title,
        "Organism": record.get("taxon", ""),
        "Data_Type": data_type,
        "Sample_Count": record.get("n_samples", 0),
        "Platform": record.get("GPL", ""),
        "Country": "",
        "Lab": "",
        "Institute": "",
        "Contributors": "",
        "PubMed_IDs": pubmed_str,
        "Supplementary_Size": "N/A",
        "Summary": summary,
        "Overall_Design": "",
        "AI_Summary_CN": ai_summary,
        "AI_Summary": ai_summary,
        "GEO_Link": f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}",
        "Submission_Date": record.get("PDAT", ""),
    }


def main():
    print(f"开始更新 Skin Fibrosis 数据 - {datetime.now()}")

    if not NCBI_EMAIL:
        print("错误: 未设置 NCBI_EMAIL")
        return

    setup_entrez()

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    existing_accessions = {d["Accession"] for d in existing_data}
    print(f"现有数据集: {len(existing_data)}")

    id_list = search_geo()
    print(f"搜索到: {len(id_list)} 条记录")

    if not id_list:
        print("没有新数据")
        return

    summaries = fetch_summaries(id_list)

    new_count = 0
    for record in summaries:
        accession = record.get("Accession", "")
        if accession in existing_accessions or not accession.startswith("GSE"):
            continue

        parsed = parse_record(record)
        if parsed:
            existing_data.insert(0, parsed)
            existing_accessions.add(accession)
            new_count += 1
            print(f"  新增: {accession}")

    if new_count > 0:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        print(f"完成! 新增 {new_count} 条，总计 {len(existing_data)} 条")
    else:
        print("没有新数据需要添加")


if __name__ == "__main__":
    main()
