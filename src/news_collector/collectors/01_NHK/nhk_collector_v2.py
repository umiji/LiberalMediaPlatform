"""
NHKニュースコレクター（改修版）
"""
import csv
import os
import asyncio
from datetime import datetime
import json
import re
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from bs4 import BeautifulSoup, Tag
from loguru import logger
import aiohttp
import feedparser
import html
from email.utils import parsedate_to_datetime
import pandas as pd
from pydantic import HttpUrl

from .base_collector import BaseCollector, NewsItem


class NHKCollectorV2(BaseCollector):
    """NHKニュースコレクター（改修版）"""

    def __init__(self, csv_path: str = None):
        """
        Args:
            csv_path (str, optional): import_page_masterのCSVファイルパス。
                                     指定がない場合はデフォルトパスを使用。
        """
        # 親クラスの初期化（一時的なダミー値を設定）
        super().__init__(
            media_id=1,
            rss_url=""  # 後で上書きするのでダミー値
        )
        self.base_url = "http://www3.nhk.or.jp"
        
        # CSVファイルのパスを設定
        if csv_path is None:
            self.csv_path = Path("data/temporary/import_page_master_20250310.csv")
        else:
            self.csv_path = Path(csv_path)
        
        # 収集したニュースを保存するためのリスト
        self.collected_news = []
        
        # RSSフィードのリストを初期化
        self.rss_feeds = []

    def _load_active_feeds(self) -> List[Tuple[int, str, Optional[int]]]:
        """
        CSVファイルからアクティブなフィードを読み込む
        
        Returns:
            List[Tuple[int, str, Optional[int]]]: (media_id, source_link, category_id)のリスト
        """
        active_feeds = []
        
        try:
            # CSVファイルを読み込む
            df = pd.read_csv(self.csv_path)
            
            # activeカラムが'Yes'のものだけをフィルタリング
            active_df = df[df['active'] == 'Yes']
            
            # NHKのメディア（media_id=1）だけをフィルタリング
            nhk_df = active_df[active_df['media_id'] == 1]
            
            # 必要なカラムを抽出
            for _, row in nhk_df.iterrows():
                media_id = int(row['media_id'])
                source_link = row['Source_link']
                
                # news_categoryが空でない場合は整数に変換
                category_id = None
                if pd.notna(row['news_category']):
                    category_id = int(row['news_category'])
                
                active_feeds.append((media_id, source_link, category_id))
            
            logger.info(f"Loaded {len(active_feeds)} active feeds from {self.csv_path}")
        except Exception as e:
            logger.error(f"Error loading active feeds from {self.csv_path}: {e}")
        
        return active_feeds

    async def collect(self) -> List[NewsItem]:
        """ニュース記事を収集する"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        # アクティブなフィードを読み込む
        active_feeds = self._load_active_feeds()
        
        if not active_feeds:
            logger.warning("No active feeds found")
            return []
        
        all_items = []
        
        # 各フィードからニュースを収集
        for media_id, rss_url, category_id in active_feeds:
            logger.info(f"Collecting news from {rss_url} (media_id={media_id}, category_id={category_id})")
            
            # RSSフィードのURLを設定
            self.rss_url = rss_url
            self.media_id = media_id
            
            items = []
            try:
                async with self.session.get(rss_url) as response:
                    feed = feedparser.parse(await response.text())
                    for entry in feed.entries:
                        try:
                            # 記事IDを取得
                            article_id = entry.link.split('/')[-1].replace('.html', '')

                            # 記事本文を取得
                            async with self.session.get(entry.link) as article_response:
                                article_html = await article_response.text()
                                soup = BeautifulSoup(article_html, 'html.parser')

                                # タイトルを取得
                                title = self._clean_text(entry.title)
                                
                                # サムネイル画像のURLを取得
                                thumbnail_url = ""
                                
                                # 記事の本文部分を構造化されたHTML形式で取得
                                content_html = ""
                                content_text = ""
                                structured_content = {
                                    'sections': [],
                                    'images': [],
                                    'videos': []
                                }
                                
                                # スクリプトタグを探す
                                script_tags = soup.select('script')
                                detail_prop_script = None
                                for script in script_tags:
                                    if script.string and '__DetailProp__' in script.string:
                                        detail_prop_script = script
                                        logger.debug(f"Found script tag with __DetailProp__")
                                        # スクリプトの内容の一部をログに出力
                                        if script.string:
                                            script_preview = script.string[:200] + "..." if len(script.string) > 200 else script.string
                                            logger.debug(f"Script content preview: {script_preview}")
                                        break
                                
                                # スクリプトタグから記事の内容を抽出
                                if detail_prop_script:
                                    try:
                                        # __DetailProp__の値を抽出
                                        script_content = detail_prop_script.string
                                        
                                        # タイトルを抽出
                                        title_match = re.search(r'title:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                        if not title_match:
                                            # 別のパターンを試す
                                            title_match = re.search(r'"title"\s*:\s*"([^"]+)"', script_content)
                                        if title_match:
                                            title = self._clean_text(title_match.group(1))
                                            logger.debug(f"Extracted title from script: {title}")
                                        
                                        # サムネイル画像URLを抽出
                                        img_match = re.search(r'img:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                        if not img_match:
                                            # 別のパターンを試す
                                            img_match = re.search(r'"img"\s*:\s*"([^"]+)"', script_content)
                                        if img_match:
                                            img_path = img_match.group(1)
                                            # 相対パスの場合はベースURLを追加
                                            if img_path.startswith('/'):
                                                thumbnail_url = f"{self.base_url}/{img_path}"
                                            else:
                                                thumbnail_url = f"{self.base_url}/{img_path}"
                                            structured_content['thumbnail_url'] = thumbnail_url
                                            logger.debug(f"Extracted thumbnail URL from script: {thumbnail_url}")
                                        
                                        # サマリーを抽出
                                        summary_match = re.search(r'summary:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                        if not summary_match:
                                            # 別のパターンを試す
                                            summary_match = re.search(r'"summary"\s*:\s*"([^"]+)"', script_content)
                                        if summary_match:
                                            summary = self._clean_text(summary_match.group(1))
                                            structured_content['sections'].append({
                                                'heading': 'サマリー',
                                                'level': 1,
                                                'content': [summary]
                                            })
                                            logger.debug(f"Extracted summary from script: {summary}")
                                        
                                        # 本文を抽出
                                        body_match = re.search(r'body:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                        if not body_match:
                                            # 別のパターンを試す
                                            body_match = re.search(r'"body"\s*:\s*"([^"]+)"', script_content)
                                        if body_match:
                                            body = self._clean_text(body_match.group(1))
                                            structured_content['sections'].append({
                                                'heading': '本文',
                                                'level': 2,
                                                'content': [body]
                                            })
                                            content_text += body
                                            logger.debug(f"Extracted body from script: {body[:100]}...")
                                        
                                        # 公開日時を抽出
                                        publish_date_match = re.search(r'publishedAt:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                        if publish_date_match:
                                            publish_date_str = publish_date_match.group(1)
                                            try:
                                                publish_date = datetime.fromisoformat(publish_date_str.replace('Z', '+00:00'))
                                                logger.debug(f"Extracted publish date from script: {publish_date}")
                                            except ValueError:
                                                # フォールバック: エントリーの公開日時を使用
                                                publish_date = parsedate_to_datetime(entry.published)
                                                logger.debug(f"Using fallback publish date: {publish_date}")
                                        else:
                                            # フォールバック: エントリーの公開日時を使用
                                            publish_date = parsedate_to_datetime(entry.published)
                                            logger.debug(f"Using fallback publish date: {publish_date}")
                                        
                                        # カテゴリを抽出
                                        # CSVから取得したcategory_idを優先的に使用
                                        if category_id is not None:
                                            logger.debug(f"Using category_id from CSV: {category_id}")
                                        else:
                                            # スクリプトからカテゴリを抽出
                                            category_match = re.search(r'category:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                            if category_match:
                                                category = category_match.group(1)
                                                category_id = self._guess_category(category)
                                                logger.debug(f"Extracted category from script: {category} -> {category_id}")
                                    except Exception as e:
                                        logger.error(f"Error extracting data from script: {e}")
                                else:
                                    # スクリプトタグが見つからない場合は、HTMLから直接抽出
                                    logger.debug("Script tag not found, extracting from HTML")
                                    
                                    # 公開日時を取得
                                    publish_date = parsedate_to_datetime(entry.published)
                                    
                                    # 本文を取得
                                    content_section = soup.select_one('div.content--detail-main')
                                    if not content_section:
                                        # 別のセレクタを試す
                                        content_section = soup.select_one('main article')
                                        logger.debug(f"Trying alternative selector 'main article': {content_section is not None}")
                                    
                                    if not content_section:
                                        # さらに別のセレクタを試す
                                        content_section = soup.select_one('.body-text')
                                        logger.debug(f"Trying alternative selector '.body-text': {content_section is not None}")
                                    
                                    if content_section:
                                        # 構造化されたコンテンツを抽出
                                        structured_content = self._extract_structured_content(content_section)
                                        
                                        # HTMLとテキストを取得
                                        content_html = str(content_section)
                                        content_text = content_section.get_text(strip=True)
                                        logger.debug(f"Extracted content text preview: {content_text[:100]}...")
                                    else:
                                        logger.warning(f"Could not find content section for {entry.link}")
                                    
                                    # カテゴリを取得
                                    if category_id is None:
                                        category_element = soup.select_one('header .content--header-category')
                                        if category_element:
                                            category = category_element.get_text(strip=True)
                                            category_id = self._guess_category(category)
                                            logger.debug(f"Extracted category from HTML: {category} -> {category_id}")
                                
                                # 生データを作成
                                raw_data = {
                                    'metadata': {
                                        'article_id': article_id,
                                        'category': category_id,
                                        'thumbnail_url': thumbnail_url,
                                        'publish_date': publish_date.isoformat(),
                                    },
                                    'structured_content': structured_content,
                                    'html': {
                                        'content': content_html
                                    },
                                    'source': {
                                        'feed': self.rss_url,
                                        'url': entry.link
                                    }
                                }

                                # NewsItemを作成
                                if not content_html:
                                    logger.warning(f"No content HTML found for {entry.link}, using default value")
                                
                                item = NewsItem(
                                    media_id=media_id,  # CSVから取得したmedia_id
                                    title=title,
                                    url=entry.link,
                                    content=content_html or "No content available",  # HTML形式で保存、空の場合はデフォルト値を設定
                                    publish_date=publish_date,
                                    category_id=category_id,  # CSVから取得したcategory_id
                                    topic_id=None,     # オプショナル
                                    author=None,       # オプショナル
                                    raw_data=raw_data
                                )
                                items.append(item)
                        except Exception as e:
                            logger.error(f"Error processing article {entry.link}: {e}")
                            continue
            except Exception as e:
                logger.error(f"Error fetching RSS feed {rss_url}: {e}")
            
            # 収集したアイテムを全体のリストに追加
            all_items.extend(items)
            logger.info(f"Collected {len(items)} items from {rss_url}")
        
        return all_items

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
            if not item.category_id and 'category' in item.raw_data.get('metadata', {}):
                item.category_id = item.raw_data['metadata']['category']
            
            transformed_items.append(item)
        
        return transformed_items

    async def validate(self, items: List[NewsItem]) -> List[NewsItem]:
        """収集したニュースを検証（オーバーライド）

        Args:
            items (List[NewsItem]): 検証対象のニュース記事リスト

        Returns:
            List[NewsItem]: 検証済みのニュース記事リスト
        """
        validated_items = []
        for item in items:
            try:
                # 必須フィールドの存在確認（contentは必須としない）
                if not all([item.title, item.url]):
                    logger.warning(f"Missing required fields in item: {item}")
                    continue

                # URLの有効性確認
                if not self.session:
                    raise RuntimeError("Session is not initialized")
                
                try:
                    async with self.session.head(str(item.url), timeout=5) as response:
                        if response.status != 200:
                            logger.warning(f"Invalid URL: {item.url}")
                            continue
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout checking URL: {item.url}")
                    # タイムアウトしても記事は有効とする
                    pass

                validated_items.append(item)
            except Exception as e:
                logger.error(f"Error validating item {item}: {e}")
                continue

        return validated_items

    async def save_to_csv(self, items: List[NewsItem]) -> str:
        """Save news items to CSV file

        Args:
            items: List of NewsItem objects

        Returns:
            Path to the saved CSV file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create results directory if it doesn't exist
        results_dir = Path("tests/test_news_ingestion/results")
        results_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to CSV
        csv_path = results_dir / f"collected_news_{timestamp}.csv"
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['media_id', 'category_id', 'title', 'url', 'publish_date', 'content', 'author']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for item in items:
                writer.writerow({
                    'media_id': item.media_id,
                    'category_id': item.category_id,
                    'title': item.title,
                    'url': str(item.url),  # Convert HttpUrl to string
                    'publish_date': item.publish_date.isoformat() if item.publish_date else None,
                    'content': item.content[:500] + '...' if item.content and len(item.content) > 500 else item.content,
                    'author': item.author,
                })
        
        # Save structured content to JSON
        json_path = results_dir / f"collected_news_structured_{timestamp}.json"
        
        # Convert HttpUrl objects to strings before serializing to JSON
        structured_data = []
        for item in items:
            item_dict = item.dict()
            # Convert HttpUrl objects to strings
            item_dict = self._convert_httpurl_to_str(item_dict)
            structured_data.append(item_dict)

        with open(json_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(structured_data, jsonfile, ensure_ascii=False, indent=2)
        
        # Save statistics
        stats_path = results_dir / f"stats_{timestamp}.json"
        stats = {
            "total_items": len(items),
            "timestamp": timestamp,
            "collector": "NHKCollectorV2",
        }
        
        with open(stats_path, 'w', encoding='utf-8') as statsfile:
            json.dump(stats, statsfile, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(items)} items to {csv_path}")
        return str(csv_path)

    async def process(self) -> List[NewsItem]:
        """ニュース収集の一連の処理を実行し、CSVに保存

        Returns:
            List[NewsItem]: 処理済みのニュース記事リスト
        """
        try:
            # ニュースの収集
            items = await self.collect()
            logger.info(f"Collected {len(items)} items from {self.__class__.__name__}")

            # データの検証
            validated_items = await self.validate(items)
            logger.info(f"Validated {len(validated_items)} items from {self.__class__.__name__}")

            # データの変換
            transformed_items = await self.transform(validated_items)
            logger.info(f"Transformed {len(transformed_items)} items from {self.__class__.__name__}")

            # CSVに保存
            await self.save_to_csv(transformed_items)

            return transformed_items
        except Exception as e:
            logger.error(f"Error in processing {self.__class__.__name__}: {e}")
            raise

    def _extract_structured_content(self, content_section) -> Dict[str, Any]:
        """
        記事の本文部分から構造化されたコンテンツを抽出する
        
        Args:
            content_section: 記事の本文部分のBeautifulSoupオブジェクト
            
        Returns:
            Dict[str, Any]: 構造化されたコンテンツ
        """
        result = {
            'sections': [],
            'images': [],
            'videos': []
        }
        
        if not content_section:
            return result
        
        # 画像URLを抽出
        for img in content_section.select('img'):
            if img.has_attr('src'):
                img_src = img['src']
                # 相対パスの場合はベースURLを追加
                if img_src.startswith('/'):
                    img_src = f"{self.base_url}{img_src}"
                result['images'].append(img_src)
        
        # 動画URLを抽出
        for video in content_section.select('video'):
            if video.has_attr('src'):
                video_src = video['src']
                # 相対パスの場合はベースURLを追加
                if video_src.startswith('/'):
                    video_src = f"{self.base_url}{video_src}"
                result['videos'].append(video_src)
        
        # 現在のセクション
        current_section = None
        
        # 要素を順番に処理
        for element in content_section.children:
            if not isinstance(element, Tag):
                continue
            
            # 見出し要素
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                # 新しいセクションを作成
                current_section = {
                    'heading': self._clean_text(element.get_text()),
                    'level': int(element.name[1]),
                    'content': []
                }
                result['sections'].append(current_section)
            
            # パラグラフ要素
            elif element.name == 'p':
                text = self._clean_text(element.get_text())
                if text:
                    if current_section:
                        current_section['content'].append(text)
                    else:
                        # セクションがない場合は新しいセクションを作成
                        current_section = {
                            'heading': '本文',
                            'level': 1,
                            'content': [text]
                        }
                        result['sections'].append(current_section)
        
        return result

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
            'tech': 8
        }

        # マッピングに存在しない場合は社会カテゴリ(3)とする
        return category_mapping.get(category, 3) 
        
    def _clean_html(self, html_text: str) -> str:
        """HTMLテキストからエスケープ文字を適切に処理する
        
        Args:
            html_text (str): 処理対象のHTMLテキスト
            
        Returns:
            str: 処理済みのHTMLテキスト
        """
        if not html_text:
            return ""
            
        # 不正なエスケープシーケンスを修正
        html_text = html_text.replace('\\/', '//')
        
        # HTMLエンティティをデコード
        html_text = html.unescape(html_text)
        
        return html_text
        
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
            logger.debug(f"Error processing Unicode escape sequences: {e}")
        
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

    def _convert_httpurl_to_str(self, obj):
        """Convert HttpUrl objects to strings in a nested structure
        
        Args:
            obj: Object to convert
            
        Returns:
            Object with HttpUrl converted to strings
        """
        if isinstance(obj, HttpUrl):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._convert_httpurl_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_httpurl_to_str(item) for item in obj]
        else:
            return obj 