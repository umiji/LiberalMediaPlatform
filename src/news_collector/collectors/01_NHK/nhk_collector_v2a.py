"""
NHKニュースコレクター V2a
テスト用のサンプルコレクター
"""
from loguru import logger


async def get_news(feed_info=None, **kwargs):
    """ニュースを取得する関数
    
    Args:
        feed_info (dict, optional): フィード情報
        **kwargs: その他のオプション引数
        
    Returns:
        dict: 収集結果
    """
    logger.info(f"NHK Collector V2a - Getting news from feed: {feed_info}")
    
    # 実際にはここでニュースを取得する処理を行う
    # このサンプルでは単にフィード情報を返す
    
    result = {
        'collector': 'nhk_collector_v2a',
        'feed_info': feed_info,
        'items': [
            {
                'title': 'テストニュース1',
                'url': 'https://example.com/news/1',
                'content': 'これはテストニュース1の内容です。'
            },
            {
                'title': 'テストニュース2',
                'url': 'https://example.com/news/2',
                'content': 'これはテストニュース2の内容です。'
            }
        ],
        'metadata': {
            'source': 'NHK',
            'category': feed_info.get('news_category') if feed_info else None
        }
    }
    
    return result 