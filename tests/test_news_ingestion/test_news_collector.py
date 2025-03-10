"""
ニュース収集テスト
"""
import asyncio
import csv
from datetime import datetime
from pathlib import Path
import uuid
import json
import re
import os
import logging
import html
import importlib
from bs4 import BeautifulSoup

import pandas as pd
import pytest
from loguru import logger

from src.news_collector.collectors.nhk_collector import NHKCollector
from src.common.config import settings
from src.news_collector.collectors.base_collector import NewsItem

# ロガーの設定
logging.basicConfig(level=logging.DEBUG)  # DEBUGレベルに変更
logger = logging.getLogger(__name__)

def clean_text(text):
    """テキストをクリーニングする"""
    if not text:
        return ""
    
    # 改行コードを統一
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # バックスラッシュのエスケープを処理
    text = text.replace('\\\\', '\\')
    text = text.replace('\\/', '//')
    text = text.replace('\\"', '"')
    text = text.replace("\\'", "'")
    
    # HTMLエンティティをデコード
    text = html.unescape(text)
    
    # Unicodeエスケープシーケンスを処理
    try:
        # 正規表現を使用してUnicodeエスケープシーケンスを検出し、実際の文字に変換
        pattern = r'\\u([0-9a-fA-F]{4})'
        while re.search(pattern, text):
            text = re.sub(pattern, lambda m: chr(int(m.group(1), 16)), text)
    except Exception as e:
        logger.debug(f"Error processing Unicode escape sequences: {e}")
    
    # 連続する空白を1つに
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def clean_json(data):
    """JSONデータをクリーニングする"""
    if isinstance(data, dict):
        return {k: clean_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_json(item) for item in data]
    elif isinstance(data, str):
        return clean_text(data)
    else:
        return data

def extract_thumbnail_url(content_data):
    """raw_dataからサムネイル画像URLを抽出する"""
    if not content_data:
        return ""
    
    # raw_dataからサムネイル画像URLを抽出
    if isinstance(content_data, dict) and 'thumbnail_url' in content_data:
        return content_data['thumbnail_url']
    
    return ""

@pytest.mark.asyncio
async def test_news_collection():
    """ニュース記事の収集テスト"""
    # ロガーの設定
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    # NHKコレクターを直接インポート
    from src.news_collector.collectors.nhk_collector import NHKCollector
    
    # 収集結果を格納するリスト
    collected_news = []

    # NHKコレクターを初期化して収集処理を実行
    collector = NHKCollector()
    items = await collector.process()
    collected_news.extend(items)
    
    # 収集結果の統計情報
    stats = {
        "total_articles": len(collected_news),
        "date_range": {
            "start": min([item.publish_date.isoformat() for item in collected_news]) if collected_news else None,
            "end": max([item.publish_date.isoformat() for item in collected_news]) if collected_news else None
        }
    }
    
    # 結果を表示
    print("\nCollection Statistics:")
    print(f"Total Articles: {stats['total_articles']}")
    print(f"Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
    
    print("\nCollected News Articles:")
    for item in collected_news:
        print(f"\nTitle: {item.title}")
        print(f"URL: {item.url}")
        print(f"Published: {item.publish_date}")
        
        # HTML Content Preview
        html_preview = item.content[:300] if item.content else ""
        print(f"HTML Content Preview: {html_preview}")
        
        # HTML Tags Analysis
        soup = BeautifulSoup(item.content, 'html.parser')
        tags = set([tag.name for tag in soup.find_all()])
        print(f"HTML Tags: {', '.join(tags)}")
        
        # Count specific tags
        tag_counts = {}
        for tag_name in ['h1', 'h2', 'h3', 'p', 'ul', 'li', 'img', 'video', 'div', 'article']:
            count = len(soup.find_all(tag_name))
            if count > 0:
                tag_counts[tag_name] = count
        print(f"Tag Counts: {tag_counts}")
        
        # Structured Content Preview
        if item.raw_data and 'content' in item.raw_data and 'structured_content' in item.raw_data['content']:
            structured_content = item.raw_data['content']['structured_content']
            if structured_content:
                print(f"Structured Content:")
                if 'sections' in structured_content:
                    for i, section in enumerate(structured_content['sections']):
                        print(f"  Section {i+1}:")
                        if 'heading' in section:
                            print(f"    Heading: {section['heading']} (Level: {section.get('level', 'N/A')})")
                        if 'content' in section:
                            if isinstance(section['content'], list):
                                print(f"    List Items: {len(section['content'])} items")
                                for j, item in enumerate(section['content'][:3]):  # 最初の3つのアイテムのみ表示
                                    print(f"      - {item}")
                                if len(section['content']) > 3:
                                    print(f"      ... and {len(section['content']) - 3} more items")
                            else:
                                content_preview = section['content'][:100] + "..." if len(section['content']) > 100 else section['content']
                                print(f"    Content: {content_preview}")
                
                if 'images' in structured_content and structured_content['images']:
                    print(f"  Images: {len(structured_content['images'])} found")
                    for i, img in enumerate(structured_content['images'][:2]):  # 最初の2つの画像のみ表示
                        print(f"    - {img}")
                    if len(structured_content['images']) > 2:
                        print(f"    ... and {len(structured_content['images']) - 2} more images")
                
                if 'videos' in structured_content and structured_content['videos']:
                    print(f"  Videos: {len(structured_content['videos'])} found")
                    for i, video in enumerate(structured_content['videos'][:2]):  # 最初の2つの動画のみ表示
                        print(f"    - {video}")
                    if len(structured_content['videos']) > 2:
                        print(f"    ... and {len(structured_content['videos']) - 2} more videos")
        
        # Thumbnail URL
        if hasattr(item, 'structured_content') and item.structured_content and 'thumbnail_url' in item.structured_content:
            thumbnail_url = item.structured_content['thumbnail_url']
            if thumbnail_url:
                print(f"Thumbnail URL: {thumbnail_url}")
    
    # 結果をCSVファイルに保存
    if collected_news:
        # 結果を保存するディレクトリを作成
        results_dir = os.path.join(os.path.dirname(__file__), "results")
        os.makedirs(results_dir, exist_ok=True)
        
        # タイムスタンプ
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # CSVファイルに保存
        df = pd.DataFrame([{
            "id": "",
                    "media_id": item.media_id,
                    "title": clean_text(item.title),
            "url": item.url,
            "content": clean_text(item.content),
            "publish_date": item.publish_date,
                    "category_id": item.category_id,
                    "topic_id": item.topic_id,
                    "author": item.author,
            "collected_at": datetime.now()
        } for item in collected_news])
        
        csv_path = os.path.join(results_dir, f"collected_news_{timestamp}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
        
        # 構造化されたコンテンツとサムネイル画像URLをJSONファイルに保存
        structured_data = [{
            "title": item.title,
            "url": str(item.url),  # URLを文字列として扱う
            "publish_date": item.publish_date.isoformat(),
            "structured_content": item.raw_data['content']['structured_content'] if item.raw_data and 'content' in item.raw_data and 'structured_content' in item.raw_data['content'] else {},
            "thumbnail_url": item.raw_data['content']['thumbnail_url'] if item.raw_data and 'content' in item.raw_data and 'thumbnail_url' in item.raw_data['content'] else ""
        } for item in collected_news]
        
        # 構造化データをクリーニング
        structured_data = clean_json(structured_data)
        
        json_path = os.path.join(results_dir, f"collected_news_structured_{timestamp}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(structured_data, f, ensure_ascii=False, indent=2)
        
        # 統計情報をJSONファイルに保存
        stats_path = os.path.join(results_dir, f"stats_{timestamp}.json")
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Results saved to {csv_path}")
        logger.info(f"Structured content saved to {json_path}")
        logger.info(f"Stats saved to {stats_path}")

    logger.info(f"Collected {len(collected_news)} articles")

if __name__ == "__main__":
    asyncio.run(test_news_collection()) 