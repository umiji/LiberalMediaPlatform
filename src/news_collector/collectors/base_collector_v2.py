"""
ニュースコレクターの基底クラス V2
CSVからデータを読み込み、アクティブなフィード情報を提供する
"""
from typing import List, Dict, Any, Optional, Callable, Tuple
from pathlib import Path
from datetime import datetime
import importlib
import pandas as pd
from loguru import logger
import os
import sys
import importlib.util
import csv

from .base_collector import NewsItem


class BaseCollectorV2:
    """ニュースコレクターの基底クラス V2"""

    def __init__(self, csv_path: str = None):
        """初期化
        
        Args:
            csv_path (str, optional): import_page_masterのCSVファイルパス。
                                     指定がない場合はデフォルトパスを使用。
        """
        # CSVファイルのパスを設定
        if csv_path is None:
            self.csv_path = Path("data/temporary/import_page_master_20250310.csv")
        else:
            self.csv_path = Path(csv_path)
        
        # データフレームを初期化
        self.feeds_df = None
        # 読み込んだコレクターを保持する辞書
        self.collector_modules = {}

    def load_active_feeds(self) -> List[Dict[str, Any]]:
        """CSVファイルからアクティブなフィード情報を読み込む
        
        Returns:
            List[Dict[str, Any]]: アクティブなフィード情報のリスト
            必要なデータ (source_id, media_id, news_category, source_link, script_file_name) を含む
        """
        try:
            # CSVファイルが存在するか確認
            if not self.csv_path.exists():
                logger.error(f"CSV file not found: {self.csv_path}")
                return []
            
            # CSVファイルを読み込む
            self.feeds_df = pd.read_csv(self.csv_path)
            
            # データ型を適切に変換
            # source_id, media_id, news_categoryを数値型に変換
            for col in ['source_id', 'media_id', 'news_category']:
                if col in self.feeds_df.columns:
                    self.feeds_df[col] = pd.to_numeric(self.feeds_df[col], errors='coerce')
            
            # activeカラムをブール値に変換
            if 'active' in self.feeds_df.columns:
                self.feeds_df['active'] = self.feeds_df['active'].map({'TRUE': True, 'FALSE': False})
                if self.feeds_df['active'].dtype != bool:
                    # 文字列以外の場合はブール値に変換
                    self.feeds_df['active'] = self.feeds_df['active'].astype(bool)
            
            # activeカラムがTRUEのレコードのみ抽出し、必要なカラムのみ選択
            required_columns = ['source_id', 'media_id', 'news_category', 'source_link', 'script_file_name']
            active_feeds = self.feeds_df[
                (self.feeds_df['active'] == True) & 
                (self.feeds_df['news_category'].notna()) &  # news_categoryがNULLでない
                (self.feeds_df['news_category'] != '') &    # news_categoryが空文字でない
                (
                    (self.feeds_df['source_type'] != 'RSS') |  # source_typeがRSSでない場合は全て含める
                    (
                        (self.feeds_df['source_type'] == 'RSS') &  # source_typeがRSSの場合
                        (self.feeds_df['source_link'].notna()) &   # source_linkがNULLでない
                        (self.feeds_df['source_link'] != '') &     # source_linkが空文字でない
                        (self.feeds_df['script_file_name'].notna()) &  # script_file_nameがNULLでない
                        (self.feeds_df['script_file_name'] != '')      # script_file_nameが空文字でない
                    )
                )
            ][required_columns].to_dict('records')
            
            logger.info(f"Loaded {len(active_feeds)} active feeds from {self.csv_path}")
            return active_feeds
        except Exception as e:
            logger.error(f"Error loading active feeds: {e}")
            return []
    
    def get_feeds_by_script_name(self, script_name: str) -> List[Dict[str, Any]]:
        """特定のスクリプト名に関連するアクティブなフィード情報を取得する
        
        Args:
            script_name (str): スクリプトファイル名
            
        Returns:
            List[Dict[str, Any]]: 指定されたスクリプト名に関連するアクティブなフィード情報のリスト
        """
        if self.feeds_df is None:
            # データフレームがまだ読み込まれていない場合は読み込む
            self.load_active_feeds()
            
        if self.feeds_df is None:
            # それでも読み込めなかった場合は空リストを返す
            return []
            
        try:
            required_columns = ['source_id', 'media_id', 'news_category', 'source_link', 'script_file_name']
            filtered_feeds = self.feeds_df[
                (self.feeds_df['active'] == True) & 
                (self.feeds_df['script_file_name'].notna()) &  # script_file_nameがNaNでないものを選択
                (self.feeds_df['script_file_name'] == script_name)
            ][required_columns].to_dict('records')
            
            logger.info(f"Found {len(filtered_feeds)} active feeds for script {script_name}")
            return filtered_feeds
        except Exception as e:
            logger.error(f"Error getting feeds by script name: {e}")
            return []
    
    def get_feeds_by_media_id(self, media_id: int) -> List[Dict[str, Any]]:
        """特定のメディアIDに関連するアクティブなフィード情報を取得する
        
        Args:
            media_id (int): メディアID
            
        Returns:
            List[Dict[str, Any]]: 指定されたメディアIDに関連するアクティブなフィード情報のリスト
        """
        if self.feeds_df is None:
            # データフレームがまだ読み込まれていない場合は読み込む
            self.load_active_feeds()
            
        if self.feeds_df is None:
            # それでも読み込めなかった場合は空リストを返す
            return []
            
        try:
            required_columns = ['source_id', 'media_id', 'news_category', 'source_link', 'script_file_name']
            filtered_feeds = self.feeds_df[
                (self.feeds_df['active'] == True) & 
                (self.feeds_df['script_file_name'].notna()) &  # script_file_nameがNaNでないものを選択
                (self.feeds_df['media_id'] == media_id)
            ][required_columns].to_dict('records')
            
            logger.info(f"Found {len(filtered_feeds)} active feeds for media_id {media_id}")
            return filtered_feeds
        except Exception as e:
            logger.error(f"Error getting feeds by media_id: {e}")
            return []
    
    def load_collector_module(self, script_name: str) -> Optional[Any]:
        """コレクターモジュールを読み込む

        Args:
            script_name (str): スクリプト名（例: '01_NHK.nhk_collector_v4'）

        Returns:
            Optional[Any]: コレクターモジュール
        """
        try:
            # スクリプト名から拡張子を除去
            if script_name.endswith('.py'):
                script_name = script_name[:-3]
            
            # スクリプト名からフォルダ名とファイル名を分離
            folder_name, file_name = script_name.split('.')
            
            # スクリプトのパスを構築
            script_path = os.path.join('src', 'news_collector', 'collectors', folder_name, f"{file_name}.py")
            logger.debug(f"Loading module from path: {script_path}")
            
            # モジュールを読み込む
            spec = importlib.util.spec_from_file_location(script_name, script_path)
            if spec is None or spec.loader is None:
                logger.error(f"Failed to load module spec for {script_name}")
                return None
                
            module = importlib.util.module_from_spec(spec)
            sys.modules[script_name] = module
            spec.loader.exec_module(module)
            
            logger.info(f"Successfully loaded collector module: {script_name}")
            return module
            
        except Exception as e:
            logger.error(f"Error loading collector module {script_name}: {e}")
            return None

    def get_collector_function(self, module: Any, function_name: str = 'get_news') -> Optional[Any]:
        """コレクター関数を取得する

        Args:
            module (Any): コレクターモジュール
            function_name (str, optional): 関数名. Defaults to 'get_news'.

        Returns:
            Optional[Any]: コレクター関数
        """
        try:
            if hasattr(module, function_name):
                return getattr(module, function_name)
            else:
                logger.error(f"Function {function_name} not found in module")
                return None
        except Exception as e:
            logger.error(f"Error getting collector function: {e}")
            return None

    async def execute_collector(self, feed_info: Dict[str, Any], collector_func: Any) -> Dict[str, Any]:
        """コレクターを実行する

        Args:
            feed_info (Dict[str, Any]): フィード情報
            collector_func (Any): コレクター関数

        Returns:
            Dict[str, Any]: 実行結果
        """
        try:
            logger.info(f"Executing collector: {feed_info.get('script_file_name')}")
            result = await collector_func(feed_info)
            return result
        except Exception as e:
            logger.error(f"Error executing collector: {e}")
            return {'error': str(e)}

    async def execute_collectors_for_feeds(self, feeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """フィードに対してコレクターを実行する

        Args:
            feeds (List[Dict[str, Any]]): フィードのリスト

        Returns:
            List[Dict[str, Any]]: 実行結果のリスト
        """
        results = []
        all_items = []  # 全コレクターから得られたアイテムを格納するリスト
        
        for feed in feeds:
            try:
                # スクリプト名を取得
                script_name = feed.get('script_file_name')
                if not script_name:
                    logger.error(f"Script name not found in feed info: {feed}")
                    continue
                
                # コレクターモジュールを読み込む
                module = self.load_collector_module(script_name)
                if not module:
                    continue
                
                # コレクター関数を取得
                collector_func = self.get_collector_function(module)
                if not collector_func:
                    continue
                
                # コレクターを実行
                result = await self.execute_collector(feed, collector_func)
                results.append(result)
                
                # 結果からアイテムを取得して統合リストに追加
                if isinstance(result, dict) and 'items' in result and result['items']:
                    all_items.extend(result['items'])
                
            except Exception as e:
                logger.error(f"Error processing feed {feed}: {e}")
                results.append({'error': str(e)})
        
        logger.info(f"Executed {len(results)} collectors")
        
        # 統合CSVファイルに保存
        if all_items:
            try:
                # 出力先ディレクトリを設定
                output_dir = Path('tests/test_news_ingestion_part2/export_data')
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # 現在の日時を取得してファイル名を生成
                now = datetime.now()
                file_name = now.strftime('%m%d%H%M_integrated_news.csv')
                output_file = output_dir / file_name
                
                # CSVファイルに書き込む
                import csv
                import pandas as pd
                
                # 指定されたカラム形式でDataFrameを作成
                df = pd.DataFrame([{
                    'id': '',
                    'media_id': item.media_id,
                    'title': item.title,
                    'url': item.url,
                    'content': item.content,
                    'publish_date': item.publish_date.strftime("%Y-%m-%d %H:%M:%S") if item.publish_date else "",
                    'category_id': item.category_id if item.category_id is not None else "",
                    'topic_id': item.topic_id if item.topic_id is not None else "",
                    'author': item.author if item.author else "",
                    'collected_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                } for item in all_items])
                
                # CSVファイルに保存
                df.to_csv(
                    output_file,
                    index=False,
                    encoding='utf-8',  # UTF-8エンコーディングを使用
                    quoting=csv.QUOTE_ALL,
                    errors='replace'  # エンコードできない文字は置換
                )
                
                logger.info(f"Integrated {len(all_items)} news items from {len(results)} collectors into {output_file}")
                
                # 統合ファイルの情報を返すための結果オブジェクトを作成
                integrated_result = {
                    'integrated_output_file': str(output_file),
                    'total_items': len(all_items),
                    'collector_count': len(results)
                }
                
                # 統合結果をresultsに追加
                results.append(integrated_result)
            except Exception as e:
                logger.error(f"Error saving integrated results to CSV: {e}")
        
        return results 