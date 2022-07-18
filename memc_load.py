#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

from multiprocessing import Queue, Process, Array, current_process
from itertools import islice

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 10000
PARSERS_NUM = 2
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.info("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            MemCacheConnections().get_client(memc_addr).set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    # line = line.decode()
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_gz(file, batch_queue):
    logging.info('Processing %s' % file)
    fd = gzip.open(file, 'rt')
    batch = list(islice(fd, BATCH_SIZE))
    while batch:
        batch_queue.put((file, batch))
        batch = list(islice(fd, BATCH_SIZE))
    batch_queue.put((file, ['EOF']))


def process_batch(batch, device_memc, options):
    logging.info('Process %s: working on batch' % current_process())
    errors, processed = 0, 0
    for line in batch:
        line = line.strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue
        ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
        if ok:
            processed += 1
        else:
            errors += 1
    return processed, errors


def add_statistic(processed, errors, file,
                  file_stats_processed, file_stats_errors, file_stats_map):
    ix = file_stats_map[file]
    file_stats_processed[ix] += processed
    file_stats_errors[ix] += errors


def parser(batch_queue: Queue, device_memc, options,
           file_stats_processed, file_stats_errors, file_stats_map):
    #memc_clients = dict((key, memcache.Client([address]))
    #                    for key, address in device_memc.items())
    while 1:
        file, batch = batch_queue.get()
        logging.info('Process %s: get batch' % current_process())
        if not batch:
            logging.info('Process %s: empty batch, exiting' % current_process())
            return
        elif batch[0] == 'EOF':
            logging.info('Process %s: this is EOF batch' % current_process())
            logging.info('Ending %s' % file)
            #dot_rename(file)
        else:
            processed, errors = process_batch(batch, device_memc, options)
            add_statistic(processed, errors, file,
                            file_stats_processed, file_stats_errors, file_stats_map)


def show_statistic(file_stats_processed, file_stats_errors, file_stats_map):
    for file, ix in file_stats_map.items():
        errors = file_stats_errors[ix]
        processed = file_stats_processed[ix]
        if not processed:
            continue
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("File: {}: Acceptable error rate {}. Successfull load".format(file, err_rate))
        else:
            logging.error("File: {}: High error rate ({} > {}). Failed load".format(file,
                                                                                err_rate, NORMAL_ERR_RATE))


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    # Memcached clients
    #memc_clients = dict((key, memcache.Client([address]))
    #                    for key, address in device_memc.items())

    batch_queue = Queue()

    # Getting files list and shared arrays for statistic on files
    files = list(glob.iglob(options.pattern))
    file_stats_map = {file: ix for ix, file in enumerate(files)}
    file_stats_processed = Array('i', [0 for _ in range(len(files))])
    file_stats_errors = Array('i', [0 for _ in range(len(files))])

    # Create parsers pool
    parsers = []
    for i in range(PARSERS_NUM):
        p = Process(target=parser, args=(batch_queue,
                                         device_memc,
                                         options,
                                         file_stats_processed,
                                         file_stats_errors,
                                         file_stats_map))
        p.start()
        parsers.append(p)
    #parser(batch_queue, memc_clients, device_memc, options, file_stats_processed, file_stats_errors, file_stats_map)

    # Sending batches to Queue
    for file in files:
        process_gz(file, batch_queue)

    # Put ending batches to Queue
    for _ in range(PARSERS_NUM):
        batch_queue.put(('', list()))

    # join parsers
    for p in parsers:
        p.join()

    show_statistic(file_stats_processed, file_stats_errors, file_stats_map)


def prototest():
    logging.info('Starting test')
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    # op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--pattern", action="store", default="/mnt/data/tmp/otuspy/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(#filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
