#!/usr/bin/env python3.8

from __future__ import absolute_import

import datetime
import json
import logging
import os
import re

import atoma.simple
import attr

from sfmutils.warc_iter import BaseWarcIter, IterItem
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from dateutil.parser import parse as date_parse
from readabilipy import simple_json_from_html_string
from warcio.archiveiterator import WARCIterator


log = logging.getLogger(__name__)


class BrowsertrixWarcIter(BaseWarcIter):

    FEED_TYPE_PATTERN = re.compile(r'(?i)^\s*application/(atom|rss)\+xml(?:\s*;.*)?')
    HTML_TYPE_PATTERN = re.compile(r'(?i)^\s*(?:text/html|application/xhtml\+xml)(?:\s*;.*)?')

    def __init__(self, filepaths, limit_user_ids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_user_ids = limit_user_ids

    def _select_record(self, url):
        return True

    def _item_iter(self, url, json_obj):
        yield None, None, None, json_obj

    @staticmethod
    def item_types():
        return ['page_json_metadata', 'capture_metadata',
                'rss_atom_feeds', 'html_metadata',
                'page_all_metadata']

    @property
    def line_oriented(self):
        return False

    def _select_item(self, item):
        return False

    def process_record(self, record, url, limit_item_types):
        if ('capture_metadata' in limit_item_types
            and record.rec_headers['WARC-Type'] == 'response'):
            date = date_parse(record.rec_headers['WARC-Date'])
            ip_address = record.rec_headers['WARC-IP-Address']
            yield 'capture_metadata', url, date, \
                {'url': url, 'date': str(date), 'ip': ip_address}
        if ('page_json_metadata' in limit_item_types
            and record.rec_headers['WARC-Type'] == 'metadata'
            and record.rec_headers['Content-Type'] == "application/json"):
            yield 'page_json_metadata', url, \
                date_parse(record.rec_headers['WARC-Date']), \
                json.loads(record.content_stream().read().decode('utf-8'))
        if ('rss_atom_feeds' in limit_item_types
            and record.rec_headers['WARC-Type'] == 'response'):
            content_type = record.http_headers.get_header('content-type')
            feed_type = None
            if content_type:
                m = BrowsertrixWarcIter.FEED_TYPE_PATTERN.match(content_type)
                if m:
                    feed_type = m.group(1).lower()
            content = record.content_stream().read()
            if not feed_type:
                # catch feeds by MIME magic ('<rss ...>' or '<feed ...>')
                # in case the HTTP header is absent or erroneous
                if b'<rss ' in content[0:1024]:
                    feed_type = 'rss'
                elif b'<feed ' in content[0:1024]:
                    feed_type = 'atom'
            if feed_type:
                yield 'rss_atom_feeds', url, \
                    date_parse(record.rec_headers['WARC-Date']), \
                    BrowsertrixWarcIter.feed_to_dict(feed_type, url, content)
        if ('html_metadata' in limit_item_types
            and record.rec_headers['WARC-Type'] == 'response'):
            content_type = record.http_headers.get_header('content-type')
            if content_type and BrowsertrixWarcIter.HTML_TYPE_PATTERN.match(content_type):
                log.debug('Parsing record to extract metadata: %s', url)
                content = record.content_stream().read()
                for encoding in EncodingDetector(content, is_html=True).encodings:
                    # take the first detected encoding
                    break
                soup = BeautifulSoup(content, 'lxml', from_encoding=encoding)
                for script in soup(['script', 'style']):
                    script.extract()
                text = soup.get_text(' ', strip=True)
                article = simple_json_from_html_string(text)

                date = date_parse(record.rec_headers['WARC-Date'])
                ip_address = record.rec_headers['WARC-IP-Address']

                metadata = {'url': url, 'ip': ip_address, 'title': None,
                            'text': text, 'article': article}

                if soup.head and soup.head.title:
                    metadata['title'] = soup.head.title.get_text(' ', strip=True)

                metafields = {}
                for meta in soup.findAll("meta"):
                    for (name, name_attr, value_attr, add_metadata) in [
                        ('og:title', 'property', 'content', 'title'),
                        ('og:url', 'property', 'content', None),
                        ('og:image', 'property', 'content', None),
                        ('og:description', 'property', 'content', None),
                        ('twitter:site', 'property', 'content', None),
                        ('twitter:creator', 'property', 'content', None),
                        # publication/creation/modification date
                        ('pubdate', 'name', 'content', 'date-published'),
                        ('publishdate', 'name', 'content', 'date-published'),
                        ('timestamp', 'name', 'content', 'date-published'),
                        ('dc.date.issued', 'name', 'content', 'date-published'),
                        ('article:published_time', 'property', 'content', 'date-published'), 
                        ('date', 'name', 'content', 'date-published'), 
                        ('bt:pubdate', 'property', 'content', 'date-published'),
                        ('sailthru.date', 'name', 'content', 'date-published'),
                        ('article.published', 'name', 'content', 'date-published'),
                        ('published-date', 'name', 'content', 'date-published'),
                        ('article.created', 'name', 'content', 'date-published'),
                        ('date_published', 'name', 'content', 'date-published'),
                        ('datepublished', 'itemprop', 'content', 'date-published'),
                        ('datecreated', 'itemprop', 'content', 'date-published'),
                        ('date', 'http-equiv', 'content', 'date-published')
                       ]:
                        if name == meta.get(name_attr, '').lower():
                            val = meta.get(value_attr, '').strip()
                            if val:
                                if name not in metafields:
                                    metafields[name] = val
                                if add_metadata and add_metadata not in metadata:
                                    metadata[add_metadata] = val

                if metafields:
                    metadata['meta'] = metafields

                if 'title' not in metadata:
                    h1 = soup.find('h1')
                    if h1:
                        metadata['title'] = h1.get_text(' ', strip=True)

                yield 'html_metadata', url, date, metadata

    def iterate_warc_files(self, filepaths):
        for filepath in filepaths:
            log.info("Iterating over %s", filepath)
            filename = os.path.basename(filepath)
            with open(filepath, 'rb') as f:
                yield_count = 0
                for record_count, record in enumerate((r for r in WARCIterator(f))):
                    self._debug_counts(filename, record_count, yield_count, by_record_count=True)

                    record_url = record.rec_headers.get_header('WARC-Target-URI')
                    if self._select_record(record_url):
                        yield record, record_url

    def iter(self, limit_item_types=None, dedupe=False, item_date_start=None, item_date_end=None):
        """
        :return: Iterator returning IterItems.
        """
        for item_type in limit_item_types:
            if item_type not in self.item_types():
                log.error("Unknown item type to extract: %s - supported types: %s",
                          item_type, self.item_types())
                return

        if 'page_all_metadata' in limit_item_types:
            # a combination of page_json_metadata and html_metadata
            items = []
            for record, record_url in self.iterate_warc_files(self.filepaths):
                items += list(self.process_record(record, record_url,
                                                  ['capture_metadata', 'page_json_metadata', 'html_metadata']))
            pages = dict()
            for item_type, item_id, item_date, item in items:
                if item_type == 'page_json_metadata':
                    pages[item_id] = (item_date, item)
            for item_type, item_id, item_date, item in items:
                if item_id not in pages:
                    continue
                if item_type == 'capture_metadata':
                    pages[item_id][1]['capture'] = {'ip': item['ip'], 'date': item['date']}
                elif item_type == 'html_metadata':
                    page = pages[item_id][1]
                    if 'text' in page and page['text'] == True:
                        # add real text (repair result of bug in customized browsertrix driver)
                        if item['text']:
                            page['text'] = item['text']
                    if 'article' not in page and item['article']:
                        page['article'] = {}
                        for to_, from_, func_ in [
                                ('title', 'title', None),
                                ('byline', 'byline', None),
                                ('date', 'date', None),
                                ('content', 'content', None),
                                ('textContent', 'plain_text',
                                 lambda l: '\n'.join(map(lambda i: i['text'], l)))]:
                            if from_ in item['article']:
                                val = item['article'][from_]
                                if func_:
                                    val = func_(val)
                                page['article'][to_] = val
                    if 'meta' in item and 'meta' not in page:
                        page['meta'] = item['meta']

            for item_id, (item_date, item) in pages.items():
                if 'text' in item and item['text'] == True:
                    item['text'] = ''
                yield IterItem(item_type, item_id, item_date, record_url, item)
            # remove page_all_metadata
            limit_item_types = list(filter(lambda i: i != 'page_all_metadata', limit_item_types))
            if not limit_item_types:
                return

        for record, record_url in self.iterate_warc_files(self.filepaths):
            for item_type, item_id, item_date, item in self.process_record(record, record_url, limit_item_types):
                yield IterItem(item_type, item_id, item_date, record_url, item)

    @staticmethod
    def attr_to_json(item) -> dict:
        name_map = {
            'articles': 'items',
            'published_at': 'date_published',
            'updated_at': 'date_modified',
            'link': 'url',
            'duration': 'duration_in_seconds',
            'content': 'content_html'
        }
        result = {}
        for field in attr.fields(type(item)):
            value = getattr(item, field.name, None)
            name = field.name
            if name in name_map:
                name = name_map[name]
            if isinstance(item, atoma.simple.Feed):
                if name == 'subtitle':
                    continue
                elif name == 'url':
                    name = 'home_page_url'
            if isinstance(value, (list, tuple)):
                result[name] = [BrowsertrixWarcIter.attr_to_json(elem)
                                      for elem in value]
            elif isinstance(value, dict):
                result[name] = {
                    field_name: BrowsertrixWarcIter.attr_to_json(field_value)
                    for (field_name, field_value) in value.values()
                }
            elif attr.has(value):
                value = BrowsertrixWarcIter.attr_to_json(value)
                result[name] = value
            elif isinstance(value, (datetime.date, datetime.datetime)):
                result[name] = value.isoformat()
            elif isinstance(value, datetime.timedelta):
                value.total_seconds()
            elif not value:
                pass # ignore None/null
            else:
                result[name] = value
        return result

    @staticmethod
    def feed_to_dict(feed_type, url, content) -> dict:
        # represent RSS and Atom feeds as JSON,
        # cf. https://jsonfeed.org/mappingrssandatom
        try:
            f = atoma.simple.simple_parse_bytes(content)
            jf = BrowsertrixWarcIter.attr_to_json(f)
            jf['feed_url'] = url
            jf['version'] = 'https://jsonfeed.org/version/1.1'
            return jf
        except Exception as e:
            log.warning("Failed to parse feed: %s", e)


if __name__ == "__main__":
    BrowsertrixWarcIter.main(BrowsertrixWarcIter)
