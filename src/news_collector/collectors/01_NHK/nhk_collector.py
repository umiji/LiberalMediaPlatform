"""
NHKニュースコレクター
"""
from datetime import datetime
import json
import re
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup, Tag
from loguru import logger
import aiohttp
import feedparser
import html
from email.utils import parsedate_to_datetime

from .base_collector import BaseCollector, NewsItem


class NHKCollector(BaseCollector):
    """NHKニュースコレクター"""

    def __init__(self):
        super().__init__(
            media_id=1,
            rss_url="https://www.nhk.or.jp/rss/news/cat0.xml"
        )
        self.base_url = "http://www3.nhk.or.jp"

    async def collect(self) -> List[NewsItem]:
        """ニュース記事を収集する"""
        if not self.session:
            self.session = aiohttp.ClientSession()

        items = []
        async with self.session.get(self.rss_url) as response:
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
                                break
                        
                        # スクリプトタグから記事の内容を抽出
                        if detail_prop_script:
                            try:
                                # __DetailProp__の値を抽出
                                script_content = detail_prop_script.string
                                
                                # タイトルを抽出
                                title_match = re.search(r'title:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                if title_match:
                                    title = self._clean_text(title_match.group(1))
                                    logger.debug(f"Extracted title from script: {title}")
                                
                                # サムネイル画像URLを抽出
                                img_match = re.search(r'img:\s*[\'"]([^\'"]+)[\'"]', script_content)
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
                                if summary_match:
                                    summary = self._clean_text(summary_match.group(1))
                                    structured_content['sections'].append({
                                        'heading': 'サマリー',
                                        'level': 2,
                                        'content': summary
                                    })
                                    logger.debug(f"Extracted summary from script")
                                
                                # 本文を抽出 (more フィールド)
                                more_match = re.search(r'more:\s*[\'"]([^\'"]+)[\'"]', script_content)
                                if more_match:
                                    more_content = self._clean_text(more_match.group(1))
                                    # <br /> タグを改行に変換
                                    more_content = more_content.replace('<br />', '\n').replace('<br/>', '\n')
                                    
                                    # HTML形式の内容を構築
                                    content_html = '<article class="nhk-article">'
                                    content_html += f'<h1>{title}</h1>'
                                    content_html += f'<div class="nhk-article-content">{more_content}</div>'
                                    
                                    # body フィールドを抽出
                                    body_match = re.search(r'body:\s*(\[.*?\}(?=\s*\])\s*\])', script_content, re.DOTALL)
                                    if body_match:
                                        body_content = body_match.group(1)
                                        # detailType と title を抽出
                                        detail_types = re.findall(r'detailType:\s*[\'"]([^\'"]+)[\'"]', body_content)
                                        detail_titles = re.findall(r'title:\s*[\'"]([^\'"]+)[\'"]', body_content)
                                        detail_texts = re.findall(r'text:\s*[\'"]([^\'"]+)[\'"]', body_content)
                                        detail_imgs = re.findall(r'img:\s*[\'"]([^\'"]+)[\'"]', body_content)
                                        
                                        # 各セクションを処理
                                        for i in range(len(detail_titles)):
                                            if i < len(detail_titles):
                                                section_title = self._clean_text(detail_titles[i])
                                                content_html += f'<h2>{section_title}</h2>'
                                                
                                                # 構造化されたコンテンツにセクションを追加
                                                section_content = ""
                                                if i < len(detail_texts) and detail_texts[i]:
                                                    section_content = self._clean_text(detail_texts[i])
                                                    # <br /> タグを改行に変換
                                                    section_content = section_content.replace('<br />', '\n').replace('<br/>', '\n')
                                                    content_html += f'<p>{section_content}</p>'
                                                
                                                structured_content['sections'].append({
                                                    'heading': section_title,
                                                    'level': 2,
                                                    'content': section_content
                                                })
                                                
                                                # 画像を追加
                                                if i < len(detail_imgs) and detail_imgs[i]:
                                                    img_path = detail_imgs[i]
                                                    # 相対パスの場合はベースURLを追加
                                                    if img_path.startswith('/'):
                                                        img_url = f"{self.base_url}/{img_path}"
                                                    else:
                                                        img_url = f"{self.base_url}/{img_path}"
                                                    content_html += f'<img src="{img_url}" alt="{section_title}">'
                                                    structured_content['images'].append(img_url)
                                    
                                    content_html += '</article>'
                                    
                                    # テキスト形式の内容を取得
                                    soup_content = BeautifulSoup(content_html, 'html.parser')
                                    content_text = soup_content.get_text(strip=True)
                                    
                                    logger.debug(f"Successfully extracted content from script tag")
                            except Exception as e:
                                logger.error(f"Failed to parse script tag: {e}")
                        
                        # スクリプトタグから情報を抽出できなかった場合は、HTMLから抽出
                        if not content_html:
                            # 指定されたXPath `//*[@id="main"]/article[2]/section/section/div/div/section[1]` に相当する要素を取得
                            main_element = soup.select_one('#main')
                            if main_element:
                                # article[2]要素を取得
                                article_elements = main_element.select('article')
                                article_content = None
                                if len(article_elements) >= 2:
                                    article_content = article_elements[1]  # 0-indexedなので2番目は[1]
                                    logger.debug(f"Found article[2] element")
                                elif article_elements:
                                    # 2番目がない場合は最後のarticle要素を使用
                                    article_content = article_elements[-1]
                                    logger.debug(f"Using last article element instead of article[2]")
                                
                                # デバッグ情報
                                logger.debug(f"Found {len(article_elements)} article elements")
                                
                                if article_content:
                                    logger.debug(f"Using article element: {article_content.get('class', ['unknown'])}")
                                    
                                    # section/section/div/div/section[1]要素を取得
                                    target_element = article_content.select_one('section > section > div > div > section:first-child')
                                    if target_element:
                                        logger.debug(f"Found target element using XPath")
                                        
                                        # スクリプトタグを削除
                                        for script in target_element.select('script'):
                                            script.extract()
                                        
                                        # HTML形式の内容を構築
                                        content_html = '<article class="nhk-article">'
                                        content_html += f'<h1>{title}</h1>'
                                        content_html += f'<div class="nhk-article-content">{str(target_element)}</div>'
                                        content_html += '</article>'
                                        
                                        # 構造化されたコンテンツに本文を追加
                                        paragraphs = target_element.select('p')
                                        if paragraphs:
                                            logger.debug(f"Found {len(paragraphs)} paragraph elements in target element")
                                            for paragraph in paragraphs:
                                                paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                structured_content['sections'].append({
                                                    'content': paragraph_text
                                                })
                                        
                                        # 画像要素を取得
                                        images = target_element.select('img')
                                        for image in images:
                                            if image.has_attr('src'):
                                                img_src = image['src']
                                                # 相対パスの場合はベースURLを追加
                                                if img_src.startswith('/'):
                                                    img_src = f"{self.base_url}{img_src}"
                                                
                                                # 構造化されたコンテンツに画像を追加
                                                structured_content['images'].append(img_src)
                                    else:
                                        logger.debug(f"Target element not found using XPath, trying alternative methods")
                                        
                                        # 新しいXPathパスを試す: //*[@id="main"]/article[2]/section/section/div/p
                                        # BeautifulSoupでは直接XPathを使用できないため、セレクタに変換
                                        # まず、mainタグを探す
                                        main_element = soup.select_one('#main')
                                        if main_element:
                                            # 直接XPathパスに対応するセレクタを使用
                                            xpath_elements = main_element.select('article:nth-child(2) section > section > div > p')
                                            
                                            # content--summaryクラスを持つ要素も検索
                                            content_summary_elements = main_element.select('.content--summary')
                                            
                                            # content--bodyを含む要素を検索
                                            content_body_elements = main_element.select('[class*="content--body"]')
                                            
                                            # 新しいXPathパス //*[@id="main"]/article[2]/section/section/div/div/section[1] に対応する要素を検索
                                            new_xpath_elements = main_element.select('article:nth-child(2) section > section > div > div > section:nth-child(1)')
                                            
                                            if xpath_elements or content_summary_elements or content_body_elements or new_xpath_elements:
                                                logger.debug(f"Found {len(xpath_elements)} paragraph elements using exact XPath")
                                                if content_summary_elements:
                                                    logger.debug(f"Found {len(content_summary_elements)} elements with content--summary class")
                                                if content_body_elements:
                                                    logger.debug(f"Found {len(content_body_elements)} elements with content--body class")
                                                if new_xpath_elements:
                                                    logger.debug(f"Found {len(new_xpath_elements)} elements using new XPath")
                                                
                                                # HTML形式の内容を構築
                                                content_html = '<article class="nhk-article">'
                                                content_html += f'<h1>{title}</h1>'
                                                content_html += '<div class="nhk-article-content">'
                                                
                                                # content--summaryクラスの要素を先に追加
                                                for summary_element in content_summary_elements:
                                                    # スクリプトタグを削除
                                                    for script in summary_element.select('script'):
                                                        script.extract()
                                                    
                                                    # 見出し要素を取得
                                                    headings = summary_element.select('h1, h2, h3, h4, h5, h6')
                                                    if headings:
                                                        logger.debug(f"Found {len(headings)} heading elements in content--summary element")
                                                        for heading in headings:
                                                            heading_text = self._clean_text(heading.get_text(strip=True))
                                                            heading_level = int(heading.name[1])
                                                            content_html += f'<h{heading_level}>{heading_text}</h{heading_level}>'
                                                            
                                                            # 構造化されたコンテンツに見出しを追加
                                                            structured_content['sections'].append({
                                                                'heading': heading_text,
                                                                'level': heading_level,
                                                                'content': []
                                                            })
                                                    
                                                    summary_text = self._clean_text(summary_element.get_text(strip=True))
                                                    content_html += f'<div class="content--summary">{summary_text}</div>'
                                                    
                                                    # 構造化されたコンテンツにサマリーを追加
                                                    # 直前に見出しがある場合は、その見出しのコンテンツに追加
                                                    if structured_content['sections'] and 'heading' in structured_content['sections'][-1] and isinstance(structured_content['sections'][-1]['content'], list):
                                                        structured_content['sections'][-1]['content'] = summary_text
                                                    else:
                                                        # 見出しがない場合は新しいセクションを作成
                                                        structured_content['sections'].append({
                                                            'heading': 'サマリー',
                                                            'level': 2,
                                                            'content': summary_text
                                                        })
                                                
                                                # 新しいXPathパスの要素を追加
                                                for section_element in new_xpath_elements:
                                                    # スクリプトタグを削除
                                                    for script in section_element.select('script'):
                                                        script.extract()
                                                    
                                                    # 見出し要素を取得
                                                    headings = section_element.select('h1, h2, h3, h4, h5, h6')
                                                    if headings:
                                                        logger.debug(f"Found {len(headings)} heading elements in new_xpath_elements")
                                                        for heading in headings:
                                                            heading_text = self._clean_text(heading.get_text(strip=True))
                                                            heading_level = int(heading.name[1])
                                                            content_html += f'<h{heading_level}>{heading_text}</h{heading_level}>'
                                                            
                                                            # 構造化されたコンテンツに見出しを追加
                                                            structured_content['sections'].append({
                                                                'heading': heading_text,
                                                                'level': heading_level,
                                                                'content': []
                                                            })
                                                    
                                                    # セクション内の段落を取得
                                                    section_paragraphs = section_element.select('p')
                                                    if section_paragraphs:
                                                        logger.debug(f"Found {len(section_paragraphs)} paragraphs in section element")
                                                        for paragraph in section_paragraphs:
                                                            paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                            content_html += f'<p>{paragraph_text}</p>'
                                                            
                                                            # 構造化されたコンテンツに段落を追加
                                                            # 直前に見出しがある場合は、その見出しのコンテンツに追加
                                                            if structured_content['sections'] and 'heading' in structured_content['sections'][-1] and isinstance(structured_content['sections'][-1]['content'], list):
                                                                structured_content['sections'][-1]['content'].append(paragraph_text)
                                                            else:
                                                                # 見出しがない場合は新しいセクションを作成
                                                                structured_content['sections'].append({
                                                                    'content': paragraph_text
                                                                })
                                                    else:
                                                        # 段落がない場合はセクション全体のテキストを取得
                                                        section_text = self._clean_text(section_element.get_text(strip=True))
                                                        content_html += f'<section>{section_text}</section>'
                                                        
                                                        # 構造化されたコンテンツにセクションを追加
                                                        structured_content['sections'].append({
                                                            'content': section_text
                                                        })
                                                
                                                # content--bodyクラスを持つ要素を追加
                                                for body_element in content_body_elements:
                                                    # スクリプトタグを削除
                                                    for script in body_element.select('script'):
                                                        script.extract()
                                                    
                                                    # 要素のクラス名を取得
                                                    class_name = ' '.join(body_element.get('class', []))
                                                    
                                                    # 見出し要素を取得
                                                    headings = body_element.select('h1, h2, h3, h4, h5, h6')
                                                    if headings:
                                                        logger.debug(f"Found {len(headings)} heading elements in content--body element")
                                                        for heading in headings:
                                                            heading_text = self._clean_text(heading.get_text(strip=True))
                                                            heading_level = int(heading.name[1])
                                                            content_html += f'<h{heading_level}>{heading_text}</h{heading_level}>'
                                                            
                                                            # 構造化されたコンテンツに見出しを追加
                                                            structured_content['sections'].append({
                                                                'heading': heading_text,
                                                                'level': heading_level,
                                                                'content': []
                                                            })
                                                    
                                                    # 段落を取得
                                                    body_paragraphs = body_element.select('p')
                                                    if body_paragraphs:
                                                        logger.debug(f"Found {len(body_paragraphs)} paragraphs in content--body element")
                                                        for paragraph in body_paragraphs:
                                                            paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                            content_html += f'<p>{paragraph_text}</p>'
                                                            
                                                            # 構造化されたコンテンツに段落を追加
                                                            # 直前に見出しがある場合は、その見出しのコンテンツに追加
                                                            if structured_content['sections'] and 'heading' in structured_content['sections'][-1] and isinstance(structured_content['sections'][-1]['content'], list):
                                                                structured_content['sections'][-1]['content'].append(paragraph_text)
                                                            else:
                                                                # 見出しがない場合は新しいセクションを作成
                                                                structured_content['sections'].append({
                                                                    'content': paragraph_text
                                                                })
                                                    else:
                                                        # 段落がない場合は要素全体のテキストを取得
                                                        body_text = self._clean_text(body_element.get_text(strip=True))
                                                        content_html += f'<div class="{class_name}">{body_text}</div>'
                                                        
                                                        # 構造化されたコンテンツに本文を追加
                                                        structured_content['sections'].append({
                                                            'content': body_text
                                                        })
                                                
                                                # 各段落を追加
                                                for paragraph in xpath_elements:
                                                    # スクリプトタグを削除
                                                    for script in paragraph.select('script'):
                                                        script.extract()
                                                    
                                                    paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                    content_html += f'<p>{paragraph_text}</p>'
                                                    
                                                    # 構造化されたコンテンツに段落を追加
                                                    structured_content['sections'].append({
                                                        'content': paragraph_text
                                                    })
                                                
                                                content_html += '</div></article>'
                                                logger.debug(f"Created content HTML using combined patterns")
                                            else:
                                                # 元の方法を試す
                                                new_target_elements = article_content.select('section > section > div > p')
                                                if new_target_elements:
                                                    logger.debug(f"Found {len(new_target_elements)} paragraph elements using alternative XPath")
                                                    
                                                    # HTML形式の内容を構築
                                                    content_html = '<article class="nhk-article">'
                                                    content_html += f'<h1>{title}</h1>'
                                                    content_html += '<div class="nhk-article-content">'
                                                    
                                                    # 各段落を追加
                                                    for paragraph in new_target_elements:
                                                        # スクリプトタグを削除
                                                        for script in paragraph.select('script'):
                                                            script.extract()
                                                        
                                                        paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                        content_html += f'<p>{paragraph_text}</p>'
                                                        
                                                        # 構造化されたコンテンツに段落を追加
                                                        structured_content['sections'].append({
                                                            'content': paragraph_text
                                                        })
                                                    
                                                    content_html += '</div></article>'
                                                    logger.debug(f"Created content HTML using alternative XPath pattern")
                                                else:
                                                    # body-textクラスを持つ要素を探す
                                                    body_text_element = article_content.select_one('.body-text')
                                                    if body_text_element:
                                                        logger.debug(f"Found body-text element")
                                                        # スクリプトタグを削除
                                                        for script in body_text_element.select('script'):
                                                            script.extract()
                                                        
                                                        # HTML形式の内容を構築
                                                        content_html = '<article class="nhk-article">'
                                                        content_html += f'<h1>{title}</h1>'
                                                        content_html += f'<div class="nhk-article-content">{str(body_text_element)}</div>'
                                                        content_html += '</article>'
                                                        
                                                        # 構造化されたコンテンツにbody-textの内容を追加
                                                        paragraphs = body_text_element.select('p')
                                                        if paragraphs:
                                                            logger.debug(f"Found {len(paragraphs)} paragraph elements in body-text")
                                                            for paragraph in paragraphs:
                                                                paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                                structured_content['sections'].append({
                                                                    'content': paragraph_text
                                                                })
                                                    else:
                                                        # section/section 要素を取得
                                                        section_elements = article_content.select('section > section')
                                                        if section_elements:
                                                            logger.debug(f"Found {len(section_elements)} section > section elements")
                                                            # 最初のsection > section要素を使用
                                                            content_section = section_elements[0]
                                                        else:
                                                            # section > section がない場合は、article要素自体を使用
                                                            content_section = article_content
                                                            logger.debug(f"No section > section elements found, using article element")
                                                        
                                                        # スクリプトタグを削除
                                                        for script in content_section.select('script'):
                                                            script.extract()
                                                        
                                                        # content--detail-bodyクラスを持つ要素を探す
                                                        content_body = content_section.select_one('.content--detail-body')
                                                        if content_body:
                                                            logger.debug(f"Found content--detail-body element")
                                                            
                                                            # HTML形式の内容を構築
                                                            content_html = '<article class="nhk-article">'
                                                            content_html += f'<h1>{title}</h1>'
                                                            content_html += f'<div class="nhk-article-content">{str(content_body)}</div>'
                                                            content_html += '</article>'
                                                            
                                                            # 構造化されたコンテンツにcontent--detail-bodyの内容を追加
                                                            paragraphs = content_body.select('p')
                                                            if paragraphs:
                                                                for paragraph in paragraphs:
                                                                    paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                                    structured_content['sections'].append({
                                                                        'content': paragraph_text
                                                                    })
                                                        else:
                                                            # news_textクラスを持つ要素を探す
                                                            news_text = content_section.select_one('.news_text')
                                                            if news_text:
                                                                logger.debug(f"Found news_text element")
                                                                
                                                                # HTML形式の内容を構築
                                                                content_html = '<article class="nhk-article">'
                                                                content_html += f'<h1>{title}</h1>'
                                                                content_html += f'<div class="nhk-article-content">{str(news_text)}</div>'
                                                                content_html += '</article>'
                                                                
                                                                # 構造化されたコンテンツにnews_textの内容を追加
                                                                paragraphs = news_text.select('p')
                                                                if paragraphs:
                                                                    for paragraph in paragraphs:
                                                                        paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                                        structured_content['sections'].append({
                                                                            'content': paragraph_text
                                                                        })
                                                            else:
                                                                # 見出し要素を取得
                                                                headings = content_section.select('h1, h2, h3, h4, h5, h6')
                                                                
                                                                # HTML形式の内容を構築
                                                                content_html = '<article class="nhk-article">'
                                                                content_html += f'<h1>{title}</h1>'
                                                                content_html += '<div class="nhk-article-content">'
                                                                
                                                                for heading in headings:
                                                                    heading_text = self._clean_text(heading.get_text(strip=True))
                                                                    heading_level = int(heading.name[1])
                                                                    content_html += f'<h{heading_level}>{heading_text}</h{heading_level}>'
                                                                    
                                                                    # 構造化されたコンテンツに見出しを追加
                                                                    structured_content['sections'].append({
                                                                        'heading': heading_text,
                                                                        'level': heading_level,
                                                                        'content': []
                                                                    })
                                                                
                                                                # 段落要素を取得
                                                                paragraphs = content_section.select('p')
                                                                for paragraph in paragraphs:
                                                                    paragraph_text = self._clean_text(paragraph.get_text(strip=True))
                                                                    content_html += f'<p>{paragraph_text}</p>'
                                                                    
                                                                    # 構造化されたコンテンツに段落を追加
                                                                    if structured_content['sections']:
                                                                        # 最後のセクションに追加
                                                                        last_section = structured_content['sections'][-1]
                                                                        if isinstance(last_section['content'], list):
                                                                            last_section['content'].append(paragraph_text)
                                                                        else:
                                                                            last_section['content'] = paragraph_text
                                                                    else:
                                                                        # セクションがない場合は、新しいセクションを作成
                                                                        structured_content['sections'].append({
                                                                            'content': paragraph_text
                                                                        })
                                                                
                                                                # リスト要素を取得
                                                                lists = content_section.select('ul, ol')
                                                                for list_element in lists:
                                                                    list_items = list_element.select('li')
                                                                    list_items_text = [self._clean_text(li.get_text(strip=True)) for li in list_items]
                                                                    
                                                                    if list_element.name == 'ul':
                                                                        content_html += '<ul>'
                                                                        for item in list_items_text:
                                                                            content_html += f'<li>{item}</li>'
                                                                        content_html += '</ul>'
                                                                    else:  # ol
                                                                        content_html += '<ol>'
                                                                        for item in list_items_text:
                                                                            content_html += f'<li>{item}</li>'
                                                                        content_html += '</ol>'
                                                                    
                                                                    # 構造化されたコンテンツにリストを追加
                                                                    if structured_content['sections']:
                                                                        # 最後のセクションに追加
                                                                        last_section = structured_content['sections'][-1]
                                                                        last_section['content'] = list_items_text
                                                                    else:
                                                                        # セクションがない場合は、新しいセクションを作成
                                                                        structured_content['sections'].append({
                                                                            'content': list_items_text
                                                                        })
                                                                
                                                                content_html += '</div></article>'
                            
                            # 記事の本文部分が取得できない場合は、タイトルとURLを含むHTML形式の内容を返す
                            if not content_html:
                                content_html = f'<article class="nhk-article"><h1>{title}</h1><p><a href="{entry.link}">{entry.link}</a></p></article>'
                                content_text = f"{title} {entry.link}"
                                logger.debug(f"Created content from title and URL")
                        
                        # テキスト形式の内容を取得（まだ取得していない場合）
                        if content_html and not content_text:
                            soup_content = BeautifulSoup(content_html, 'html.parser')
                            content_text = soup_content.get_text(strip=True)
                        
                        # 公開日時を取得
                        publish_date = None
                        if detail_prop_script:
                            try:
                                # スクリプトの内容から日付を抽出
                                datetime_match = re.search(r'datetime:\s*[\'"]([^\'"]+)[\'"]', detail_prop_script.string)
                                if datetime_match:
                                    datetime_str = datetime_match.group(1)
                                    # ISO形式の日付文字列をdatetimeオブジェクトに変換
                                    publish_date = datetime.fromisoformat(datetime_str)
                                    logger.debug(f"Extracted publish date from script: {publish_date}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid datetime format: {e}")
                        
                        if not publish_date and hasattr(entry, 'published'):
                            try:
                                # RFC822形式の日付文字列をdatetimeオブジェクトに変換
                                publish_date = parsedate_to_datetime(entry.published)
                                logger.debug(f"Using published date from RSS: {publish_date}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid published date format: {e}")
                        
                        # それでも日付が取得できなかった場合は現在時刻を使用
                        if not publish_date:
                            publish_date = datetime.now()
                            logger.debug(f"Using current time as publish date: {publish_date}")

                    # カテゴリの推測
                    category_id = None
                    if hasattr(entry, 'category'):
                        category_id = self._guess_category(entry.category)

                    # raw_dataを構築
                    raw_data = {
                        'article_id': article_id,
                        'metadata': {
                                'title': title,
                            'published_at': publish_date.isoformat(),
                            'category': category_id
                        },
                        'content': {
                            'main_text': content_text,
                                'thumbnail_url': thumbnail_url,
                                'structured_content': structured_content
                        },
                        'source': {
                            'media_name': 'NHK',
                            'url': entry.link
                        }
                    }

                    # NewsItemを作成
                    item = NewsItem(
                        media_id=self.media_id,
                            title=title,
                        url=entry.link,
                            content=content_html,  # HTML形式で保存
                        publish_date=publish_date,
                        category_id=category_id,
                        topic_id=None,     # オプショナル
                        author=None,       # オプショナル
                        raw_data=raw_data
                    )
                    items.append(item)
                except Exception as e:
                    logger.error(f"Error processing article {entry.link}: {e}")
                    continue

        return items

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
                        if isinstance(current_section['content'], list):
                            current_section['content'].append(text)
                        else:
                            current_section['content'] = text
                    else:
                        # セクションがない場合は、新しいセクションを作成
                        current_section = {
                            'content': text
                        }
                        result['sections'].append(current_section)
            
            # リスト要素
            elif element.name in ['ul', 'ol']:
                list_items = []
                for li in element.select('li'):
                    list_items.append(self._clean_text(li.get_text()))
                
                if list_items:
                    if current_section:
                        current_section['content'] = list_items
                    else:
                        # セクションがない場合は、新しいセクションを作成
                        current_section = {
                            'content': list_items
                        }
                        result['sections'].append(current_section)
        
        return result

    def _guess_category(self, category: str) -> int:
        """カテゴリ名からカテゴリIDを推測

        Args:
            category (str): カテゴリ名

        Returns:
            int: 推測されたカテゴリID
        """
        # カテゴリーのマッピング
        category_mapping = {
            "政治": 1,
            "経済": 2,
            "社会": 3,
            "国際": 4,
            "スポーツ": 5,
            "科学・文化": 6,
            "国会": 1,  # 政治カテゴリとして扱う
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