import csv
import datetime

import logging

import socket

import sys

import time

from pathlib import Path

from typing import List, Union, Optional

# noinspection PyUnresolvedReferences

from prometheus_client.core import Gauge, Histogram  # noinspection PyUnresolvedReferences

from prometheus_client.core import GaugeMetricFamily, REGISTRY, CounterMetricFamily

# noinspection PyUnresolvedReferences

from prometheus_client import start_http_server

import argparse

DEFAULT_ROWS = [{'name': 'Jack', 'age': '15', 'id': 1024}, {'name': 'JackJill', 'age': '12', 'id': 123},
                {'name': 'Jung', 'age': '45', 'id': 4567}, {'name': 'Juck', 'age': '15', 'id': 345},
                {'name': 'Jill', 'age': '34', 'id': 687}]

log = logging.getLogger(__name__)


def read_csv_path(path, fieldnames=None):
    csv_path = Path(path) if not isinstance(path, Path) else path
    with csv_path.open('r', encoding='utf-8') as f:
        if fieldnames:
            rows = list(csv.DictReader(f, fieldnames=fieldnames))
        else:
            rows = list(map(dict, csv.DictReader(f)))
    if not rows:
        raise RuntimeError(f'No data found in the {path}')
    return rows


def parser():
    parser = argparse.ArgumentParser(prog='Prometheus custom exporter',
                                     description='Random example to create a prometheus custom exporter')

    parser.add_argument("--csv_file_path", '-c',
                        help="Provide the file path containing key value")

    parser.add_argument("--host", 'c', help="Provide the prometheus hostname")
    parser.add_argument("--port", 'p', default=9092, help="Provide the prometheus host port")

    parser.include_common_args('verbose', parallel - 8)
    return parser


def main():
    args = parser().parse_args()

    host = args.host or socket.gethostname()

    start_http_server(int(args.port), addr=host)

    log.info(f'Listening at http://{host}:{args.port}/metrics')
    print(METRIC_PATH)

    REGISTRY.register(MetricsCollector())
    while True:
        time.sleep(10)
        print('SLEEPING 10s........')


class MetricsCollector:
    """
    Reads a list of dictionary and adds the id value of every employee in the dict as a metric along with name and age as labels
    """

    def _init__(self, filepath=None):
        self.filepath = filepath

    def collect(self):
        """

        :return: 
        """
        rows = read_csv_path(filepath) if self.filepath else DEFAULT_ROWS
        gauge = GaugeMetricFamily("employee_id_metrics", "Details of employees", labels=['name', 'age'])
        for item in rows:
            name = item['name']
            age = item['age']
            value = item['id']
            metric_timestamp = round(time.time() * 1000)
            gauge.add_metric((name, age), value, metric_timestamp)
            print(f'sending id={value} for {name}')
            yield gauge


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()
