"""
BaseCollectorV2の動的コレクター機能のテスト
"""
import os
import sys
import asyncio
from pathlib import Path

# プロジェクトのルートディレクトリをPYTHONPATHに追加
sys.path.insert(0, os.path.abspath("."))

from loguru import logger
from src.news_collector.collectors.base_collector_v2 import BaseCollectorV2


async def run_collectors(collector: BaseCollectorV2, feeds: list) -> None:
    """コレクターを実行する関数
    
    Args:
        collector (BaseCollectorV2): コレクターインスタンス
        feeds (list): フィードのリスト
    """
    try:
        results = await collector.execute_collectors_for_feeds(feeds)
        logger.info(f"{len(results)}件のコレクターを実行しました")
        
        # 結果を表示
        for result in results:
            if isinstance(result, dict):
                if 'error' in result:
                    logger.error(f"Error in collector: {result['error']}")
                elif 'integrated_output_file' in result:
                    # 統合CSVファイルの情報を表示
                    logger.info("=== 統合CSVファイル情報 ===")
                    logger.info(f"ファイル: {result.get('integrated_output_file')}")
                    logger.info(f"総記事数: {result.get('total_items')}件")
                    logger.info(f"コレクター数: {result.get('collector_count')}件")
                    logger.info("=====================")
                else:
                    logger.info(f"Collector: {result.get('collector')}")
                    logger.info(f"Feed info: {result.get('feed_info')}")
                    logger.info(f"Output file: {result.get('output_file')}")
                    logger.info(f"Number of items: {len(result.get('items', []))}")
                    logger.info("---")
            else:
                logger.warning(f"Unexpected result type: {type(result)}")
    except Exception as e:
        logger.error(f"エラー: {e}")


async def main():
    """メイン関数"""
    # ログの設定
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    # CSVファイルのパス
    csv_path = "data/temporary/import_page_master_20250310.csv"
    
    # BaseCollectorV2のインスタンスを作成
    collector = BaseCollectorV2(csv_path=csv_path)
    
    # アクティブなフィードを読み込む
    active_feeds = collector.load_active_feeds()
    logger.info(f"{len(active_feeds)}件のフィードを読み込みました")
    
    # コレクターを実行
    await run_collectors(collector, active_feeds)
    
    logger.info("完了")


if __name__ == "__main__":
    asyncio.run(main()) 