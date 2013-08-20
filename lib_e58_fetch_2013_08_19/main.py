#!/usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
#
# Copyright 2013 Andrej Antonov <polymorphm@gmail.com>.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

assert str is not bytes

import argparse
import csv
import queue
from . import e58_fetch

THREAD_COUNT = 5

class CsvCtx:
    pass

def csv_write_header(csv_ctx):
    csv_ctx.writer.writerow((
            'title',
            'director',
            'uaddress',
            'address',
            'phone',
            'email',
            'www',
            'work',
            'rubriks',
            ))
    csv_ctx.fd.flush()

def csv_write_data(csv_ctx, fetch_data):
    csv_ctx.writer.writerow((
            fetch_data['title'],
            fetch_data['director'],
            fetch_data['uaddress'],
            fetch_data['address'],
            fetch_data['phone'],
            fetch_data['email'],
            fetch_data['www'],
            fetch_data['work'],
            fetch_data['rubriks'],
            ))
    csv_ctx.fd.flush()

def on_scheduled(event_queue, url):
    print('scheduled: {!r}'.format(url))

def on_begin(event_queue, url):
    print('begin: {!r}'.format(url))

def on_fetch(event_queue, csv_ctx, url, fetch_data):
    print('fetched: {!r}'.format(url))
    
    csv_write_data(csv_ctx, fetch_data)

def on_error(event_queue, url, err_type, err_msg):
    print('error: {!r} :{!r}: {}'.format(url, err_type, err_msg))

def on_continue(event_queue):
    print('*** continue! ***')

def on_done(event_queue):
    print('*** done! ***')

def main():
    parser = argparse.ArgumentParser(
            description='utility for fetching firm database of Penza city',
            )
    
    parser.add_argument(
            'out_path',
            metavar='OUT-FILE-PATH',
            help='file path to output. format CSV',
            )
    
    args = parser.parse_args()
    
    csv_ctx = CsvCtx()
    csv_ctx.fd = open(args.out_path, 'w', encoding='utf-8', newline='')
    csv_ctx.writer = csv.writer(csv_ctx.fd)
    csv_write_header(csv_ctx)
    
    event_queue = queue.Queue(maxsize=100)
    bulk_data_ctx = e58_fetch.BulkDataCtx()
    e58_fetch.init_bulk_data_ctx(bulk_data_ctx)
    
    def bulk_data_fetch_start():
        def on_scheduled_wrapper(url):
            event_queue.put(('scheduled', url))
        
        def on_begin_wrapper(url):
            event_queue.put(('begin', url))
        
        def on_fetch_wrapper(url, fetch_data):
            event_queue.put(('fetch', url, fetch_data))
        
        def on_error_wrapper(url, err_type, err_msg):
            event_queue.put(('error', url, err_type, err_msg))
        
        def on_done_wrapper():
            event_queue.put(('done',))
        
        e58_fetch.bulk_data_fetch(
                bulk_data_ctx,
                thread_count=THREAD_COUNT,
                on_scheduled=on_scheduled_wrapper,
                on_fetch=on_fetch_wrapper,
                on_begin=on_begin_wrapper,
                on_error=on_error_wrapper,
                on_done=on_done_wrapper,
                )
    
    bulk_data_fetch_start()
    
    while True:
        event = event_queue.get()
        try:
            if event[0] == 'done':
                try:
                    bulk_data_fetch_start()
                    on_continue(event_queue)
                except StopIteration:
                    on_done(event_queue)
                    break
            elif event[0] == 'scheduled':
                on_scheduled(event_queue, event[1])
            elif event[0] == 'begin':
                on_begin(event_queue, event[1])
            elif event[0] == 'fetch':
                on_fetch(event_queue, csv_ctx, event[1], event[2])
            elif event[0] == 'error':
                on_error(event_queue, event[1], event[2], event[3])
        finally:
            event_queue.task_done()
