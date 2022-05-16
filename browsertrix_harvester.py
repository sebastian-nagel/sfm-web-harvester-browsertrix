#!/usr/bin/env python3

from __future__ import absolute_import
import datetime
import logging
import json
import os
import random
import re
import subprocess
import time
import uuid

from io import BytesIO

import psutil
import warcio

from sfmutils.harvester import BaseHarvester, Msg
from sfmutils.utils import safe_string

log = logging.getLogger(__name__)

QUEUE = "browsertrix_harvester"
ROUTING_KEY = "harvest.start.web.web_crawl_browsertrix"

# rotate WARC if max size (1 GiB) is reached
MAX_WARC_FILE_SIZE = 2**30
# skip records exceeding maximum (50 MB, compressed)
MAX_WARC_RECORD_SIZE = 50 * 2**20

class BrowsertrixHarvester(BaseHarvester):

    def __init__(self, working_path, stream_restart_interval_secs=30 * 60, mq_config=None, debug=False,
                 debug_warcprox=False, tries=1):

        use_warcprox = False
        BaseHarvester.__init__(self, working_path, mq_config=mq_config,
                               stream_restart_interval_secs=stream_restart_interval_secs,
                               use_warcprox=use_warcprox, debug=debug, debug_warcprox=debug_warcprox,
                               tries=tries)

    def harvest_seeds_test(self):
        log.info("Not running crawl")
        self.result.harvest_counter["pages"] += 1
        self.result.increment_stats("pages")

    def harvest_seeds(self):
        assert len(self.message.get("seeds", [])) == 1
        seed = self.message.get("seeds")[0]
        seed_url = seed.get("token")

        browsertrix_args = self.message.get("options", {}).get("browsertrix_args", "")
        browsertrix_args = re.split(r'\s+', browsertrix_args)
        collection_id = uuid.uuid4().hex
        browsertrix_args = ['crawl', '--collection', collection_id, *browsertrix_args, '--url', seed_url]

        self.init_collection(collection_id)

        try:
            res = subprocess.run(browsertrix_args,
                                 text=True, capture_output=True,
                                 timeout=60*60*3, # timeout after 3h
                                 cwd='/crawls')

            self.log_stats(collection_id)
            if self.debug:
                log.debug(">" * 40)
                log.debug("Stdout:\n%s\n", res.stdout)
                time.sleep(1)
                log.debug("Stderr:\n%s\n", res.stderr)
                time.sleep(1)
                log.debug("<" * 40)

            if res.returncode == 0:
                log.info("Crawl succeeded")
                self.crawl_result_to_warc(collection_id, seed_url, browsertrix_args, res)
                self.update_page_list(collection_id)
                self.cleanup_warcs(collection_id)
            else:
                msg = "Crawl failed with exit value {}, stderr:\n{}".format(
                    res.returncode,
                    str('\n'.join(res.stderr.rsplit('\n', 20)[-20:])));
                log.error(msg)
                self.result.errors.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
                # TODO: wrap WARC files also for failed crawls, maybe only few pages failed?

        except subprocess.TimeoutExpired as e:
            log.warn("Crawl timed out: %s", e)
            if self.debug:
                log.debug(">" * 40)
                log.debug("Stdout:\n%s\n", e.stdout)
                time.sleep(1)
                log.debug("Stderr:\n%s\n", e.stderr)
                time.sleep(1)
                log.debug("<" * 40)
            self.crawl_result_to_warc(collection_id, seed_url, browsertrix_args, res)
            self.update_page_list(collection_id)
            self.cleanup_warcs(collection_id)

        except Exception as e:
            log.exception("Crawl failed with exception", exc_info=e)
            msg = "Crawl failed with exception {}".format(e)
            self.result.errors.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))

        finally:
            # child processes of browsertrix-crawler are still running or terminated:
            # reap zombie processes or kill running processes
            # TODO: should be fixed by
            #       https://github.com/webrecorder/browsertrix-crawler/commit/e7d3767
            for child in psutil.Process(os.getpid()).children(recursive=True):
                log.debug("Waiting for child process %d (%s) to terminate", child.pid, child.name())
                try:
                    child.wait(1)
                except psutil.TimeoutExpired:
                    log.debug("Killing child process %d (%s)", child.pid, child.name())
                    child.kill()
                    child.wait(1)

    def log_stats(self, collection_id):
        stats_file = os.path.join('/crawls/collections', collection_id, 'stats.json')
        if os.path.exists(stats_file):
            with open(stats_file) as stats:
                log.info("Browsertrix stats: %s\n", stats.read())

    def init_collection(self, collection_id):
        collection_dir = os.path.join('/crawls/collections', collection_id)
        if os.path.isdir(collection_dir):
            os.rmdir(collection_dir)
        os.makedirs(collection_dir, exist_ok=True)
        self.write_page_list(collection_id)

    def update_page_list(self, collection_id):
        all_captures = self.state_store.get_state(__name__, 'page.captures')
        if not all_captures:
            all_captures = dict()
        captures_file = os.path.join('/crawls/collections', collection_id, 'captures.jsonl')
        if os.path.exists(captures_file):
            with open(captures_file) as new_captures:
                for capture in new_captures:
                    capture = json.loads(capture)
                    all_captures[capture['url']] = capture['timestamp']
        self.state_store.set_state(__name__, 'page.captures', all_captures)

    def write_page_list(self, collection_id):
        page_list = []
        all_captures = self.state_store.get_state(__name__, 'page.captures')
        if all_captures:
            for url in all_captures:
                page_list.append(url)
        page_list_file = os.path.join('/crawls/collections', collection_id, 'urls-seen.json')
        with open(page_list_file, 'w', encoding='utf-8') as stream:
            json.dump(page_list, stream, ensure_ascii=False)

    class RotatingWarcWriter():
        """write into rotating WARC files"""

        # TODO: addressed by
        #   https://github.com/webrecorder/browsertrix-crawler/pull/33
        #   However,
        #    - need to put everything into WARC files, including screenshots and pages.jsonl
        #    - need to remove over-sized WARC records

        def __init__(self, message_id, warc_temp_dir):
            # WARC file name pattern from https://github.com/internetarchive/warcprox/blob/f19ead00587633fe7e6ba6e3292456669755daaf/warcprox/writer.py#L69
            self.random_token = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz0123456789', 8))
            self.time_stamp = BrowsertrixHarvester.RotatingWarcWriter.warcprox_timestamp17()
            self.serial_no = -1
            self.message_id = message_id
            self.warc_temp_dir = warc_temp_dir

            self.warc_file_name = None
            self.warc = None
            self.warc_writer = None
            self.next_warc_writer()

        @staticmethod
        def warcprox_timestamp17():
            """ copied from warcprox.timestamp17() """
            now = datetime.datetime.utcnow()
            return '{:%Y%m%d%H%M%S}{:03d}'.format(now, now.microsecond//1000)

        @staticmethod
        def get_warc_file_name(_id, ts, serial_no, rt):
            return '{}-{}-{:05d}-{}.warc.gz'.format(
                safe_string(_id), ts, serial_no, rt)

        def next_warc_writer(self):
            if self.warc:
                self.warc.close()
                self.warc = None
                self.warc_writer = None

            self.serial_no += 1
            self.warc_file_name = BrowsertrixHarvester.RotatingWarcWriter.get_warc_file_name(
                self.message_id, self.time_stamp, self.serial_no, self.random_token)

            log.info("Writing to %s", self.warc_file_name)
            self.warc = open(os.path.join(self.warc_temp_dir, self.warc_file_name), 'wb')
            self.warc_writer = warcio.WARCWriter(self.warc, gzip=True)
            warc_info = {
                "software": "Social Feed Manager 2.5 (https://gwu-libraries.github.io/sfm-ui/), Browsertrix Crawler 0.5.0 (https://github.com/webrecorder/browsertrix-crawler), PyWB 2.6.7 (https://github.com/webrecorder/pywb/), warcio 1.7.4 (https://github.com/webrecorder/warcio)",
                "format": "WARC File Format 1.1",
                "conformsTo": "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/",
            }
            # write warcinfo record
            self.warc_writer.write_record(self.warc_writer.create_warcinfo_record(self.warc_file_name, warc_info))

        def write_data(self, data):
            if len(data) > MAX_WARC_RECORD_SIZE:
                # TODO: should inspect WARC data to ensure that no WARC record exceeds limit
                log.warn("WARC data exceeds limit of 50 MiB")
            if (self.warc.tell() + len(data)) > MAX_WARC_FILE_SIZE:
                # start next WARC file if 1 GB would be reached
                self.next_warc_writer()
            self.warc.write(data)


    def crawl_result_to_warc(self, collection_id, seed_url, brtrix_args, brtrix_res):
        warc_dir = os.path.join('/crawls/collections', collection_id, 'archive')
        pages_file = os.path.join('/crawls/collections', collection_id, 'pages/pages.jsonl')
        warc_files = []
        try:
            warc_files = os.listdir(warc_dir)
            log.info("WARC files: %s", warc_files)
        except FileNotFoundError as e:
            msg = "Failed to read crawl output (WARC files): {}".format(e)
            log.exception(msg)
            self.result.warnings.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
            if os.path.exists(pages_file):
                pass # continue to log the capture errors
            else:
                return

        # write resulting WARC file(s)
        w = BrowsertrixHarvester.RotatingWarcWriter(self.message["id"], self.warc_temp_dir)

        # pages.jsonl : write one metadata record for every captured page
        with open(pages_file) as pages:
            for line in pages:
                line = line.rstrip('\r\n')
                page = json.loads(line)
                if 'url' in page:
                    record = w.warc_writer.create_warc_record(page['url'], 'metadata',
                                                              payload=BytesIO(line.encode('utf-8')),
                                                              warc_content_type='application/json')
                    w.warc_writer.write_record(record)
                    if 'title' in page and page['title'] == 'Pywb Error':
                        self.result.harvest_counter["page_errors"] += 1
                        self.result.increment_stats("page_errors")
                        msg = "Failed to capture page: %s" % page['text']
                        log.warning(msg)
                        if 'seed' in page and page['seed']:
                            msg = "Failed to capture seed page: %s" % page['text']
                            self.result.errors.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
                        else:
                            self.result.warnings.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
                    else:
                        self.result.harvest_counter["pages"] += 1
                        self.result.increment_stats("pages")

        # screenshots
        scrsh_warc_dir = os.path.join('/crawls/collections', collection_id, 'screenshots')
        try:
            screenshot_warc_files = filter(lambda f: f.endswith('.warc.gz'),
                                           os.listdir(scrsh_warc_dir))
            log.info("Screenshot WARC files: %s", screenshot_warc_files)
            for warc_file in screenshot_warc_files:
                warc_input = os.path.join(scrsh_warc_dir, warc_file)
                with open(warc_input, 'rb') as win:
                    w.write_data(win.read())
        except FileNotFoundError as e:
            msg = "Failed to read screenshots: {}".format(e)
            log.exception(msg)

        # WARC files
        for warc_file in warc_files:
            warc_input = os.path.join(warc_dir, warc_file)
            with open(warc_input, 'rb') as win:
                w.write_data(win.read())

    def cleanup_warcs(self, collection_id):
        """clean up /crawls/collections/<id> in container:
        - remove WARC files from archive/ and screenshots/
         keep the rest for post-debugging: entire folder is cleaned up on startup"""
        collection_dir = os.path.join('/crawls/collections/', collection_id)
        for warc_dir in ['archive', 'screenshots']:
            warc_dir = os.path.join(collection_dir, warc_dir)
            try:
                for warc_file in os.listdir(warc_dir):
                    if warc_file.endswith('.warc.gz'):
                        os.remove(os.path.join(warc_dir, warc_file))
            except:
                pass

    def process_warc(self, warc_filepath):
        # Note: pages are counted while processing pages.jsonl
        pass


if __name__ == "__main__":
    BrowsertrixHarvester.main(BrowsertrixHarvester, QUEUE, [ROUTING_KEY])


