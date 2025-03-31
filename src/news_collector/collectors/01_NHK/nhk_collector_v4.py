"""
NHKニュースコレクター V4
Base_collectorから呼び出される形式に改修
"""
from datetime import datetime, timedelta, timezone
import json
import re
from typing import List, Dict, Any, Optional
from pathlib import Path
import os
import csv

from bs4 import BeautifulSoup, Tag
from loguru import logger
import aiohttp
import feedparser
import html
from email.utils import parsedate_to_datetime
import pandas as pd

from src.news_collector.collectors.base_collector_v2 import NewsItem


async def get_news(feed_info: Dict[str, Any], session: Optional[aiohttp.ClientSession] = None, **kwargs) -> Dict[str, Any]:
    """ニュースを取得する関数：ニュース記事にアクセスしてコンテンツを取得する機能はこちらで持つ。
    
    Args:
        feed_info (Dict[str, Any]): フィード情報（source_link, media_idを含む）
        session (Optional[aiohttp.ClientSession]): 既存のセッション。Noneの場合は新規に作成
        **kwargs: その他のオプション引数
        
    Returns:
        Dict[str, Any]: 収集結果
    """
    session_created = False
    try:
        # フィード情報から必要な情報を取得
        source_link = feed_info.get('source_link')
        media_id = feed_info.get('media_id')
        news_category = feed_info.get('news_category', 0)  # デフォルト値として0を設定
        
        # カテゴリIDがnanの場合は0を設定
        if pd.isna(news_category):
            news_category = 0
        
        if not source_link or not media_id:
            logger.error("Required feed information is missing")
            return {'error': 'Required feed information is missing'}
        
        # セッションを作成または再利用
        if session is None:
            session = aiohttp.ClientSession()
            session_created = True
        
        # ニュース記事を収集
        items = []
        async with session.get(source_link) as response:
            feed = feedparser.parse(await response.text())
            for entry in feed.entries:
                try:
                    # 記事IDを取得
                    article_id = entry.link.split('/')[-1].replace('.html', '')
                    
                    # 記事本文を取得
                    async with session.get(entry.link) as article_response:
                        article_html = await article_response.text()
                        soup = BeautifulSoup(article_html, 'html.parser')
                        
                        # タイトルを取得
                        title = _clean_text(entry.title)
                        
                        # 記事の本文部分を取得
                        content_html = ""
                        content_text = ""
                        ##本スクリプトでは様々な記事形式に対応するため3パターンの方法を用意する。#1をデフォルトとし、エラーが出る場合＃2、#3を実行する
                        # 本文を取得する方法1: content--detail-bodyクラスを持つ要素から取得（デフォルトはこれ）
                        content_element = soup.select_one('.content--detail-body')
                        if content_element:
                            # スクリプトタグを削除
                            for script in content_element.select('script'):
                                script.extract()
                            
                            # 本文のテキストを取得
                            content_text = _clean_text(content_element.get_text())
                            
                            # HTML形式の内容を構築
                            content_html = '<article class="nhk-article">'
                            content_html += f'<h1>{title}</h1>'
                            content_html += '<div class="nhk-article-content">'
                            
                            # セクションごとに処理
                            sections = content_element.select('.content--body')
                            if sections:
                                for section in sections:
                                    # H2タグがあれば追加
                                    h2 = section.select_one('.body-title')
                                    if h2:
                                        h2_text = _clean_text(h2.get_text())
                                        if h2_text:
                                            content_html += f'<h2>{h2_text}</h2>'
                                    
                                    # 本文を追加
                                    body_text = section.select_one('.body-text')
                                    if body_text:
                                        p_text = _clean_text(body_text.get_text())
                                        if p_text:
                                            content_html += f'{p_text}<br //><br />'
                            else:
                                # セクションがない場合は本文全体を使用
                                content_html += f'{content_text}'
                            
                            content_html += '</div></article>'
                        
                        # 本文を取得する方法2: スクリプトタグから取得
                        if not content_html:
                            script_tags = soup.select('script')
                            detail_prop_script = None
                            for script in script_tags:
                                if script.string and '__DetailProp__' in script.string:
                                    detail_prop_script = script
                                    break
                            
                            if detail_prop_script:
                                # スクリプトの内容から本文を抽出
                                script_content = detail_prop_script.string
                                
                                # more フィールドを抽出
                                more_match = re.search(r'more:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                if more_match:
                                    more_content = _clean_text(more_match.group(1))
                                    # <br /> タグを改行に変換
                                    more_content = more_content.replace('<br />', '\n').replace('<br/>', '\n')
                                    
                                    # HTML形式の内容を構築
                                    content_html = '<article class="nhk-article">'
                                    content_html += f'<h1>{title}</h1>'
                                    content_html += '<div class="nhk-article-content">'
                                    
                                    # 段落に分割して追加
                                    paragraphs = more_content.split('\n')
                                    for p in paragraphs:
                                        p = p.strip()
                                        if p:
                                            content_html += f'{p}<br //><br />'
                                    
                                    content_html += '</div></article>'
                                    content_text = more_content
                        
                        # 本文を取得する方法3: news_textクラスを持つ要素から取得
                        if not content_html:
                            news_text = soup.select_one('.news_text')
                            if news_text:
                                # スクリプトタグを削除
                                for script in news_text.select('script'):
                                    script.extract()
                                
                                # 本文のテキストを取得
                                content_text = _clean_text(news_text.get_text())
                                
                                # HTML形式の内容を構築
                                content_html = '<article class="nhk-article">'
                                content_html += f'<h1>{title}</h1>'
                                content_html += '<div class="nhk-article-content">'
                                
                                # H2タグを含む見出しを取得
                                headings = news_text.find_all('h2')
                                for h2 in headings:
                                    h2_text = _clean_text(h2.get_text())
                                    if h2_text:
                                        content_html += f'<h2>{h2_text}</h2>'
                                
                                # 段落を取得
                                paragraphs = news_text.select('p')
                                if paragraphs:
                                    for p in paragraphs:
                                        p_text = _clean_text(p.get_text())
                                        if p_text:
                                            content_html += f'{p_text}<br //><br />'
                                else:
                                    # 段落がない場合は本文全体を使用
                                    content_html += f'{content_text}'
                                
                                content_html += '</div></article>'
                        
                        # 本文が取得できなかった場合
                        if not content_html:
                            content_html = f'<article class="nhk-article"><h1>{title}</h1><div class="nhk-article-content"></div></article>'
                            content_text = title
                        
                        # 公開日時を取得
                        publish_date = None
                        if hasattr(entry, 'published'):
                            try:
                                publish_date = parsedate_to_datetime(entry.published)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid published date format: {e}")
                        
                        if not publish_date:
                            publish_date = datetime.now()
                        
                        # NewsItemを作成
                        item = NewsItem(
                            media_id=media_id,
                            title=title,
                            url=entry.link,
                            content=content_html,
                            publish_date=publish_date,
                            category_id=news_category,
                            topic_id=None,
                            author=None,
                            source_id=feed_info.get('source_id')
                        )
                        items.append(item)
                except Exception as e:
                    logger.error(f"Error processing article {entry.link}: {e}")
                    continue
        
        # 結果をCSVファイルに保存
        if items:
            # 出力先ディレクトリを設定
            output_dir = Path('tests/test_news_ingestion_part2/export_data')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 現在の日時を取得してファイル名を生成
            now = datetime.now()
            source_id = feed_info.get('source_id', 'unknown')
            file_name = now.strftime(f'%m%d%H%M_{source_id}_news_export.csv')
            output_file = output_dir / file_name
            
            # 日本時間（JST）のタイムゾーンを設定
            jst = timezone(timedelta(hours=9))
            now_jst = datetime.now(jst)
            timestamp_jst = now_jst.strftime("%Y-%m-%d %H:%M:%S %Z")
            
            # CSVファイルに書き込む
            try:
                # 指定されたカラム形式でDataFrameを作成
                df = pd.DataFrame([{
                    'id': '',
                    'media_id': item.media_id,
                    'source_id': item.source_id if item.source_id is not None else "",
                    'title': item.title,
                    'url': item.url,
                    'content': item.content,
                    'publish_date': item.publish_date.strftime("%Y-%m-%d %H:%M:%S") if item.publish_date else "",
                    'category_id': item.category_id if item.category_id is not None else "",
                    'topic_id': item.topic_id if item.topic_id is not None else "",
                    'author': item.author if item.author else "",
                    'collected_at': timestamp_jst  # タイムゾーン情報を含む形式に変更
                } for item in items])
                
                # CSVファイルに保存
                df.to_csv(
                    output_file,
                    index=False,
                    encoding='utf-8',  # UTF-8エンコーディングを使用
                    quoting=csv.QUOTE_ALL,
                    errors='replace'  # エンコードできない文字は置換
                )
                logger.info(f"{len(df)} news items saved to {output_file}")
                
                # フィード情報をログに出力（デバッグ用）
                logger.debug(f"Processed feed: {source_link}, Category: {news_category}, Source ID: {source_id}")
                
                return {
                    'collector': '01_NHK.nhk_collector_v4',
                    'feed_info': feed_info,
                    'items': items,
                    'output_file': str(output_file),
                    'metadata': {
                        'source': 'NHK',
                        'category': news_category,
                        'source_id': source_id,
                        'item_count': len(items)
                    }
                }
            except Exception as e:
                logger.error(f"Error saving to CSV: {e}")
                return {'error': str(e)}
        
        return {
            'collector': '01_NHK.nhk_collector_v4',
            'feed_info': feed_info,
            'items': items,
            'output_file': None,
            'metadata': {
                'source': 'NHK',
                'category': news_category,
                'source_id': source_id,
                'item_count': len(items) if items else 0
            }
        }
    
    except Exception as e:
        logger.error(f"Error in get_news: {e}")
        return {'error': str(e)}
    
    finally:
        # セッションを閉じる（自分で作成した場合のみ）
        if session_created and session:
            await session.close()


async def main(feeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Base_collectorから呼び出される関数：受け取ったフィード情報を元にニュース取得処理を実行
    
    Args:
        feeds (List[Dict[str, Any]]): Base_collectorから渡されるフィード情報のリスト
    
    Returns:
        List[Dict[str, Any]]: 各フィードの収集結果のリスト
    """
    try:
        if not feeds:
            logger.warning("No feeds provided to process")
            return []
        
        logger.info(f"Processing {len(feeds)} feeds")
        
        # セッションを作成（全フィードで共有）
        session = aiohttp.ClientSession()
        
        results = []
        try:
            # 各フィードからニュースを取得
            for feed in feeds:
                try:
                    # 同じセッションを再利用してget_news関数を呼び出す
                    result = await get_news(feed, session=session)
                    results.append(result)
                    logger.info(f"Processed feed {feed.get('source_link')} with category {feed.get('news_category')}")
                except Exception as e:
                    logger.error(f"Error processing feed {feed}: {e}")
                    continue
        finally:
            # セッションを閉じる
            await session.close()
        
        return results
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return []


def _clean_text(text: str) -> str:
    """テキストをクリーニングする関数
    
    Args:
        text (str): クリーニング対象のテキスト
        
    Returns:
        str: クリーニング後のテキスト
    """
    if not text:
        return ""
    
    # HTMLエンティティをデコード
    text = html.unescape(text)
    
    # 余分な空白を削除
    text = re.sub(r'\s+', ' ', text)
    
    # 前後の空白を削除
    text = text.strip()
    
    return text 