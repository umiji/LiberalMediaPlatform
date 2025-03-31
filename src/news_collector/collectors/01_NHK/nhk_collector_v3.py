"""
NHKニュースコレクター（改修版 v3）
"""
import csv
import os
import asyncio
from datetime import datetime
import json
import re
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path

from bs4 import BeautifulSoup, Tag
from loguru import logger
import aiohttp
import feedparser
import html
from email.utils import parsedate_to_datetime
import pandas as pd
from pydantic import HttpUrl

from src.news_collector.collectors.base_collector import BaseCollector, NewsItem


class NhkCollectorV3(BaseCollector):
    """NHKニュースコレクター（改修版 v3）"""

    def __init__(self, csv_path: str = None):
        """初期化
        
        Args:
            csv_path (str, optional): import_page_masterのCSVファイルパス。
                                     指定がない場合はデフォルトパスを使用。
        """
        # 親クラスの初期化（ダミー値を設定。実際の値はCSVから取得する）
        super().__init__(
            media_id=1,  # ダミー値。実際の値はCSVから取得
            rss_url="",  # ダミー値。実際の値はCSVから取得
            csv_path=csv_path
        )
        self.base_url = "http://www3.nhk.or.jp"
        self.session = None
        self.logger = logger  # loggerをインスタンス変数として設定

    async def collect(self, session=None):
        """NHKからニュースを収集する
        
        Args:
            session (aiohttp.ClientSession, optional): 既存のセッション
            
        Returns:
            List[NewsItem]: 収集したニュース記事のリスト
        """
        try:
            if session is None:
                self.session = aiohttp.ClientSession()
            else:
                self.session = session
            
            all_items = []
            # BaseCollectorの_load_active_feedsメソッドを使用してアクティブなフィードを取得
            active_feeds = self._load_active_feeds()
            
            for feed_info in active_feeds:
                try:
                    # feed_infoは辞書形式
                    media_id = feed_info['media_id']
                    source_link = feed_info['source_link']
                    category_id = feed_info['news_category']
                    
                    if not source_link:
                        self.logger.warning(f"Skipping feed with no RSS URL: {feed_info}")
                        continue
                    
                    self.logger.info(f"Collecting news from {source_link} (media_id={media_id}, category_id={category_id})")
                    
                    items = await self._collect_from_feed(feed_info)
                    all_items.extend(items)
                except Exception as e:
                    self.logger.error(f"Error collecting from feed {feed_info}: {e}")
            
            return all_items
        except Exception as e:
            self.logger.error(f"Error in collect: {e}")
            return []
        finally:
            if self.session and session is None:  # 自分で作成したセッションのみを閉じる
                await self.session.close()
                self.session = None

    async def _collect_from_feed(self, feed_info):
        """特定のフィードからニュースを収集する
        
        Args:
            feed_info (Dict): フィード情報
            
        Returns:
            List[NewsItem]: 収集したニュース記事のリスト
        """
        media_id = feed_info.get('media_id')
        rss_url = feed_info.get('source_link')
        category_id = feed_info.get('news_category')
        source_id = feed_info.get('source_id')
        
        # カテゴリIDがNoneの場合、デフォルト値として3（社会カテゴリ）を設定
        if category_id is None or category_id == '':
            category_id = 3
            self.logger.warning(f"Category ID is not provided for {rss_url}, using default category_id=3")
        
        if not rss_url:
            self.logger.warning(f"No RSS URL provided for media_id {media_id}")
            return []
        
        try:
            feed = feedparser.parse(rss_url)
            collected_items = []
            
            for entry in feed.entries:
                try:
                    # RSSフィードから基本情報を抽出
                    title = entry.get('title', '')
                    url = entry.get('link', '')
                    published = entry.get('published', '')
                    
                    # 必須フィールドが欠けている場合はスキップ
                    if not (title and url):
                        continue
                    
                    # 公開日時を解析
                    publish_date = self._parse_date(published)
                    
                    # 記事ページからコンテンツを抽出
                    content, structured_content = await self._extract_content(url)
                    
                    # RSSフィードからタイトルを使用
                    if not title and structured_content and 'title' in structured_content:
                        title = structured_content['title']
                    
                    # raw_dataを構築
                    raw_data = {
                        'media_id': media_id,
                        'title': title,
                        'url': url,
                        'content': content,
                        'publish_date': publish_date,
                        'category_id': category_id,
                        'source_id': source_id
                    }
                    
                    # 追加のメタデータを抽出
                    if structured_content:
                        # トピックIDを抽出
                        topic_id = None
                        if structured_content.get('sections'):
                            for section in structured_content['sections']:
                                if section.get('id'):
                                    topic_id = section.get('id')
                                    break
                        
                        raw_data['topic_id'] = topic_id
                        
                        # 著者を抽出
                        author = structured_content.get('author', '')
                        raw_data['author'] = author
                    
                    # NewsItemオブジェクトを作成
                    news_item = NewsItem(
                        media_id=media_id,
                        title=title,
                        url=url,
                        content=content,
                        publish_date=publish_date,
                        category_id=category_id,
                        topic_id=raw_data.get('topic_id'),
                        author=raw_data.get('author', ''),
                        raw_data=raw_data
                    )
                    
                    collected_items.append(news_item)
                except Exception as e:
                    self.logger.error(f"Error processing entry {entry.get('title', 'Unknown')}: {str(e)}")
                    continue
            
            return collected_items
        except Exception as e:
            self.logger.error(f"Error collecting from feed {rss_url}: {str(e)}")
            return []

    async def transform(self, items: List[NewsItem]) -> List[NewsItem]:
        """NHK固有のデータ変換
        
        Args:
            items (List[NewsItem]): 変換対象のニュース記事リスト
            
        Returns:
            List[NewsItem]: 変換済みのニュース記事リスト
        """
        transformed_items = []
        for item in items:
            # 必要に応じてデータを変換
            # 例: カテゴリIDの設定
            if not item.category_id and item.raw_data and 'metadata' in item.raw_data and 'category' in item.raw_data['metadata']:
                item.category_id = item.raw_data['metadata']['category']
            
            transformed_items.append(item)
        
        return transformed_items

    async def validate(self, items: List[NewsItem]) -> List[NewsItem]:
        """収集したニュースを検証
        
        Args:
            items (List[NewsItem]): 検証対象のニュース記事リスト
            
        Returns:
            List[NewsItem]: 検証済みのニュース記事リスト
        """
        validated_items = []
        for item in items:
            try:
                # 必須フィールドの存在確認
                if not all([item.title, item.url]):
                    logger.warning(f"Missing required fields in item: {item}")
                    continue

                # URLの有効性確認（オプション）
                if self.session:
                    try:
                        async with self.session.head(str(item.url), timeout=5) as response:
                            if response.status != 200:
                                logger.warning(f"Invalid URL: {item.url}")
                                # 無効なURLでも記事は有効とする（警告のみ）
                    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                        logger.warning(f"Error checking URL {item.url}: {e}")
                        # エラーが発生しても記事は有効とする（警告のみ）

                validated_items.append(item)
            except Exception as e:
                logger.error(f"Error validating item {item}: {e}")
                continue

        return validated_items

    async def _extract_content(self, url: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """記事のコンテンツと構造化コンテンツを抽出する
        
        Args:
            url (str): 記事のURL
            
        Returns:
            Tuple[str, Optional[Dict[str, Any]]]: (コンテンツ, 構造化コンテンツ)
        """
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch article: {url}, status: {response.status}")
                    return "", None
                
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # 記事の本文を取得
                content_html = ""
                article_element = soup.select_one('article.nhk-article')
                if article_element:
                    # 記事のHTMLを取得
                    content_html = str(article_element)
                
                # スクリプトコンテンツを抽出
                script_content = self._extract_script_content(html_content)
                
                # 構造化コンテンツを抽出
                structured_content = None
                if script_content:
                    body_match = re.search(r'"body"\s*:\s*(\[.*?\])', script_content, re.DOTALL)
                    if body_match:
                        try:
                            body_json = json.loads(body_match.group(1))
                            structured_content = self._process_body_content(body_json)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse body JSON: {e}")
                
                # 記事の本文が取得できなかった場合は、HTMLから抽出を試みる
                if not content_html:
                    content_html, structured_content = self._extract_content_from_html(soup, url)
                
                return content_html, structured_content
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {e}")
            return "", None

    def _extract_script_content(self, html_content):
        """HTMLからスクリプトコンテンツを抽出する
        
        Args:
            html_content (str): HTML内容
            
        Returns:
            str: 抽出されたスクリプトコンテンツ
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        for script in soup.find_all('script'):
            if script.string and '__DetailProp__' in script.string:
                self.logger.debug("Found __DetailProp__ script tag")
                return script.string
        return None
        
    def _process_body_content(self, body_json):
        """ボディJSONを処理して構造化コンテンツを作成する
        
        Args:
            body_json (List): ボディJSON
            
        Returns:
            Dict: 構造化コンテンツ
        """
        structured_content = {
            "sections": [],
            "images": [],
            "videos": []
        }
        
        if not body_json:
            return structured_content
            
        for item in body_json:
            if isinstance(item, dict):
                if "type" in item and item["type"] == "text" and "content" in item:
                    structured_content["sections"].append({
                        "content": item["content"]
                    })
                elif "type" in item and item["type"] == "image" and "url" in item:
                    structured_content["images"].append({
                        "url": item["url"],
                        "caption": item.get("caption", "")
                    })
                elif "type" in item and item["type"] == "video" and "url" in item:
                    structured_content["videos"].append({
                        "url": item["url"],
                        "caption": item.get("caption", "")
                    })
        
        return structured_content

    def _extract_content_from_html(self, soup, url):
        """HTMLから記事のコンテンツを抽出する
        
        Args:
            soup (BeautifulSoup): BeautifulSoupオブジェクト
            url (str): 記事のURL
            
        Returns:
            Tuple[str, Dict]: (コンテンツHTML, 構造化コンテンツ)
        """
        content_html = ""
        structured_content = {
            "sections": [],
            "images": [],
            "videos": []
        }
        
        try:
            # タイトルを取得
            title_element = soup.select_one('h1.title, h1.content--title, .article-title h1')
            title = title_element.get_text(strip=True) if title_element else ""
            
            # 本文を取得
            main_content = None
            
            # 複数のセレクタを試す
            selectors = [
                'article.nhk-article',
                '.content--detail-body',
                '.news_text',
                '.body-text',
                '#main article section > section > div > div > section:first-child',
                '#main article section > section > div > p'
            ]
            
            for selector in selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    self.logger.debug(f"Found content using selector: {selector}")
                    break
            
            if main_content:
                # スクリプトタグを削除
                for script in main_content.select('script'):
                    script.extract()
                
                # HTML形式の内容を構築
                content_html = '<article class="nhk-article">'
                content_html += f'<h1>{title}</h1>'
                content_html += f'<div class="nhk-article-content">{str(main_content)}</div>'
                content_html += '</article>'
                
                # 構造化コンテンツを抽出
                # 段落を抽出
                paragraphs = main_content.select('p')
                for paragraph in paragraphs:
                    paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                    if paragraph_text:
                        structured_content["sections"].append({
                            "content": paragraph_text
                        })
                
                # 画像を抽出
                images = main_content.select('img')
                for image in images:
                    if image.has_attr('src'):
                        img_src = image['src']
                        # 相対パスの場合はベースURLを追加
                        if img_src.startswith('/'):
                            img_src = f"{self.base_url}{img_src}"
                        structured_content["images"].append({
                            "url": img_src,
                            "caption": image.get('alt', '')
                        })
            else:
                # 本文が見つからない場合はタイトルとURLのみの内容を返す
                content_html = f'<article class="nhk-article"><h1>{title}</h1><p><a href="{url}">{url}</a></p></article>'
                structured_content["sections"].append({
                    "content": f"{title} {url}"
                })
        except Exception as e:
            self.logger.error(f"Error extracting content from HTML: {e}")
            content_html = f'<article class="nhk-article"><p>Error extracting content: {str(e)}</p></article>'
        
        return content_html, structured_content

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """日付文字列をdatetimeオブジェクトに変換する
        
        Args:
            date_str (str): 日付文字列
            
        Returns:
            Optional[datetime]: 変換後のdatetimeオブジェクト、変換できない場合はNone
        """
        if not date_str:
            return None
            
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            pass
            
        # 他の日付形式を試す
        date_formats = [
            "%Y-%m-%dT%H:%M:%S%z",  # ISO 8601
            "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO 8601 with microseconds
            "%Y-%m-%d %H:%M:%S",  # Standard format
            "%Y年%m月%d日 %H時%M分",  # Japanese format
            "%Y年%m月%d日",  # Japanese date only
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        self.logger.warning(f"Could not parse date: {date_str}")
        return datetime.now()  # 解析できない場合は現在時刻を返す

    def _guess_category(self, category: str) -> int:
        """カテゴリ名からカテゴリIDを推測する
        
        Args:
            category (str): カテゴリ名
            
        Returns:
            int: カテゴリID
        """
        # カテゴリ名とIDのマッピング
        category_mapping = {
            '政治': 1,
            '経済': 2,
            '社会': 3,
            '国際': 4,
            'スポーツ': 5,
            '科学・文化': 6,
            'エンタメ': 7,
            'IT・科学': 8,
            'politics': 1,
            'business': 2,
            'economy': 2,
            'society': 3,
            'international': 4,
            'world': 4,
            'sports': 5,
            'culture': 6,
            'entertainment': 7,
            'science': 8,
            'tech': 8,
            '国会': 1,  # 政治カテゴリとして扱う
        }

        # マッピングに存在しない場合は社会カテゴリ(3)とする
        return category_mapping.get(category.lower() if isinstance(category, str) else '', 3)
    
    def _clean_text(self, text: str) -> str:
        """テキストからエスケープ文字を除去し、適切に整形する
        
        Args:
            text (str): 処理対象のテキスト
            
        Returns:
            str: 処理済みのテキスト
        """
        if not text:
            return ""
        
        # HTMLエンティティをデコード
        text = html.unescape(text)
        
        # Unicodeエスケープシーケンスを処理
        try:
            # 複数回処理して、ネストされたエスケープシーケンスも処理する
            pattern = r'\\u([0-9a-fA-F]{4})'
            while re.search(pattern, text):
                text = re.sub(pattern, lambda m: chr(int(m.group(1), 16)), text)
        except Exception as e:
            self.logger.debug(f"Error processing Unicode escape sequences: {e}")
        
        # 不正なエスケープシーケンスを修正
        text = text.replace('\\/', '//')
        text = text.replace('\\n', '\n')
        text = text.replace('\\t', '\t')
        text = text.replace('\\r', '\r')
        text = text.replace('\\"', '"')
        text = text.replace("\\'", "'")
        text = text.replace('\\\\', '\\')
        
        # 連続する空白を1つにまとめる
        text = re.sub(r'\s+', ' ', text)
        
        # 前後の空白を削除
        text = text.strip()
        
        return text 