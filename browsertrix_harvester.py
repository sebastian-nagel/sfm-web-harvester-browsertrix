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


class BrowsertrixHarvester(BaseHarvester):

    def __init__(self, working_path, stream_restart_interval_secs=30 * 60, mq_config=None, debug=False,
                 debug_warcprox=False, tries=1):

        use_warcprox = False
        BaseHarvester.__init__(self, working_path, mq_config=mq_config,
                               stream_restart_interval_secs=stream_restart_interval_secs,
                               use_warcprox=use_warcprox, debug=debug, debug_warcprox=debug_warcprox,
                               tries=tries)

    @staticmethod
    def get_warc_file_name(_id, ts, serial_no, rt):
        return '{}-{}-{:05d}-{}.warc.gz'.format(
            safe_string(_id), ts, serial_no, rt)

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

        try:
            res = subprocess.run(browsertrix_args,
                                 text=True, capture_output=True,
                                 cwd='/crawls')

            if True: #self.debug:
                log.debug(">" * 40)
                log.debug("Stdout: %s\n", res.stdout)
                time.sleep(1)
                log.debug("Stderr: %s\n", res.stderr)
                time.sleep(1)
                log.debug("<" * 40)

            if res.returncode == 0:
                log.info("Crawl succeeded")
                self.crawl_result_to_warc(collection_id, seed_url, browsertrix_args, res)
            else:
                msg = "Crawl failed with exit value {}, stderr:\n{}".format(
                    res.returncode,
                    str('\n'.join(res.stderr.rsplit('\n', 20)[-20:])));
                log.error(msg)
                self.result.warnings.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
                # TODO: failed harvest reported?

        except Exception as e:
            log.exception("Crawl failed with exception", exc_info=e)
            msg = "Crawl failed with exception {}".format(e)
            self.result.warnings.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))

        finally:
            # child processes of browsertrix-crawler are still running or terminated:
            # reap zombie processes or kill running processes
            for child in psutil.Process(os.getpid()).children(recursive=True):
                log.debug("Waiting for child process %d (%s) to terminate", child.pid, child.name())
                try:
                    child.wait(1)
                except psutil.TimeoutExpired:
                    log.debug("Killing child process %d (%s)", child.pid, child.name())
                    child.kill()
                    child.wait(1)
            # TODO: cleanup of /crawl/collection/<id>

    @staticmethod
    def warcprox_timestamp17():
        """ copied from warcprox.timestamp17() """
        now = datetime.datetime.utcnow()
        return '{:%Y%m%d%H%M%S}{:03d}'.format(now, now.microsecond//1000)

    def crawl_result_to_warc(self, collection_id, seed_url, brtrix_args, brtrix_res):
        warc_dir = os.path.join('/crawls/collections', collection_id, 'archive')
        try:
            warc_files = os.listdir(warc_dir)
            log.info("WARC files: %s", warc_files)
        except FileNotFoundError as e:
            msg = "Failed to read crawl output (WARC files): {}".format(e)
            log.exception(msg)
            self.result.warnings.append(Msg("crawl_{}".format(collection_id), msg, seed_id=seed_url))
            return

        # write resulting WARC file
        # WARC file name pattern from https://github.com/internetarchive/warcprox/blob/f19ead00587633fe7e6ba6e3292456669755daaf/warcprox/writer.py#L69
        random_token = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz0123456789', 8))
        time_stamp = BrowsertrixHarvester.warcprox_timestamp17()
        serial_no = 0

        warc_file_name = BrowsertrixHarvester.get_warc_file_name(
            self.message["id"], time_stamp, serial_no, random_token)

        # write into single WARC file
        # TODO: will be addressed by
        #   https://github.com/webrecorder/browsertrix-crawler/pull/33
        log.info("Writing to %s", warc_file_name)
        warc = open(os.path.join(self.warc_temp_dir, warc_file_name), 'wb')
        writer = warcio.WARCWriter(warc, gzip=True)
        warc_info = {
            "software": "Social Feed Manager 2.3 (https://gwu-libraries.github.io/sfm-ui/), Browsertrix Crawler 0.3.0 (https://github.com/webrecorder/browsertrix-crawler), PyWB 2.5.0 (https://github.com/webrecorder/pywb/), warcio 1.7.4 (https://github.com/webrecorder/warcio)",
            "format": "WARC File Format 1.1",
            "conformsTo": "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/",
        }
        # warcinfo record
        writer.write_record(writer.create_warcinfo_record(warc_file_name, warc_info))

        # pages.jsonl
        with open(os.path.join('/crawls/collections', collection_id, 'pages/pages.jsonl'), 'rb') as pages:
            pages_payload = pages.read()
            record = writer.create_warc_record(seed_url, 'metadata',
                                               payload=BytesIO(pages_payload),
                                               warc_content_type='application/x-ndjson')
            writer.write_record(record)

        # screenshots
        warc_dir = os.path.join('/crawls/collections', collection_id, 'screenshots')
        try:
            screenshot_warc_files = filter(lambda f: f.endswith('.warc.gz'),
                                           os.listdir(warc_dir))
            log.info("Screenshot WARC files: %s", screenshot_warc_files)
            for warc_file in screenshot_warc_files:
                warc_input = os.path.join(warc_dir, warc_file)
                with open(warc_input, 'rb') as win:
                    data = win.read()
                    if (warc.tell() + len(data)) > 2**30:
                        # start next WARC file if 1 GB would be reached
                        warc.close()
                        serial_no += 1
                        warc_file_name = BrowsertrixHarvester.get_warc_file_name(
                            self.message["id"], time_stamp, serial_no, random_token)
                        warc = open(os.path.join(self.warc_temp_dir, warc_file_name), 'wb')
                        writer = warcio.WARCWriter(warc, gzip=True)
                        writer.write_record(writer.create_warcinfo_record(warc_file_name, warc_info))
                    warc.write(data)
        except FileNotFoundError as e:
            msg = "Failed to read screenshots: {}".format(e)
            log.exception(msg)

        # WARC files
        for warc_file in warc_files:
            warc_input = os.path.join(warc_dir, warc_file)
            with open(warc_input, 'rb') as win:
                data = win.read()
                if (warc.tell() + len(data)) > 2**30:
                    # start next WARC file if 1 GB would be reached
                    warc.close()
                    serial_no += 1
                    warc_file_name = BrowsertrixHarvester.get_warc_file_name(
                        self.message["id"], time_stamp, serial_no, random_token)
                    warc = open(os.path.join(self.warc_temp_dir, warc_file_name), 'wb')
                    writer = warcio.WARCWriter(warc, gzip=True)
                    writer.write_record(writer.create_warcinfo_record(warc_file_name, warc_info))
                warc.write(data)

    def process_warc(self, warc_filepath):
        pass # TODO


if __name__ == "__main__":
    BrowsertrixHarvester.main(BrowsertrixHarvester, QUEUE, [ROUTING_KEY])
