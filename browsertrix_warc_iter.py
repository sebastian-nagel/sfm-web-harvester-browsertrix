#!/usr/bin/env python3.8

from __future__ import absolute_import

import json
import logging
import os

from sfmutils.warc_iter import BaseWarcIter, IterItem
from dateutil.parser import parse as date_parse
from warcio.archiveiterator import WARCIterator


log = logging.getLogger(__name__)


class BrowsertrixWarcIter(BaseWarcIter):
    def __init__(self, filepaths, limit_user_ids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_user_ids = limit_user_ids

    def _select_record(self, url):
        return True

    def _item_iter(self, url, json_obj):
        yield None, None, None, json_obj

    @staticmethod
    def item_types():
        return ['page_json_metadata', 'capture_metadata']

    @property
    def line_oriented(self):
        return False

    def _select_item(self, item):
        return False

    def process_record(self, record, url, limit_item_types):
        if ('capture_metadata' in limit_item_types and record.rec_headers['WARC-Type'] == 'response'):
            date = date_parse(record.rec_headers['WARC-Date'])
            yield 'capture_metadata', url, date, {'url': url, 'date': str(date)}
        if ('page_json_metadata' in limit_item_types
            and record.rec_headers['WARC-Type'] == 'metadata'
            and record.rec_headers['Content-Type'] == "application/json"):
            yield 'page_json_metadata', url, \
                date_parse(record.rec_headers['WARC-Date']), json.loads(record.content_stream().read().decode('utf-8'))


    def iter(self, limit_item_types=None, dedupe=False, item_date_start=None, item_date_end=None):
        """
        :return: Iterator returning IterItems.
        """
        for item_type in limit_item_types:
            if item_type not in self.item_types():
                log.error("Unknown item type to extract: %s - supported types: %s",
                          item_type, self.item_types())
                return

        for filepath in self.filepaths:
            log.info("Iterating over %s", filepath)
            filename = os.path.basename(filepath)
            with open(filepath, 'rb') as f:
                yield_count = 0
                for record_count, record in enumerate((r for r in WARCIterator(f))):
                    self._debug_counts(filename, record_count, yield_count, by_record_count=True)

                    record_url = record.rec_headers.get_header('WARC-Target-URI')
                    if self._select_record(record_url):
                        for item_type, item_id, item_date, item in self.process_record(record, record_url, limit_item_types):
                            yield IterItem(item_type, item_id, item_date, record_url, item)


if __name__ == "__main__":
    BrowsertrixWarcIter.main(BrowsertrixWarcIter)
