#
# Copyright 2017-2018 Red Hat, Inc.
# Copyright 2012-2013 eNovance <licensing@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import time
import msgpack
import select
import sys

import cotyledon
from oslo_config import cfg
from oslo_log import log
import oslo_messaging
from stevedore import named

from ceilometer.i18n import _
from ceilometer import messaging
from ceilometer import sample

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


LOG = log.getLogger(__name__)


OPTS = [
    cfg.BoolOpt('ack_on_event_error',
                default=True,
                help='Acknowledge message when event persistence fails.'),
    cfg.MultiStrOpt('messaging_urls',
                    default=[],
                    secret=True,
                    help="Messaging URLs to listen for notifications. "
                         "Example: rabbit://user:pass@host1:port1"
                         "[,user:pass@hostN:portN]/virtual_host "
                         "(DEFAULT/transport_url is used if empty). This "
                         "is useful when you have dedicate messaging nodes "
                         "for each service, for example, all nova "
                         "notifications go to rabbit-nova:5672, while all "
                         "cinder notifications go to rabbit-cinder:5672."),
    cfg.IntOpt('expiration',
               help='Number of seconds to wait before deleting samples.'
               ),
    cfg.MultiStrOpt('pipelines',
                    default=['meter', 'event'],
                    help="Select which pipeline managers to enable to "
                    " generate data"),
]


EXCHANGES_OPTS = [
    cfg.MultiStrOpt('notification_control_exchanges',
                    default=['nova', 'glance', 'neutron', 'cinder', 'heat',
                             'keystone', 'sahara', 'trove', 'zaqar', 'swift',
                             'ceilometer', 'magnum', 'dns', 'ironic', 'aodh'],
                    deprecated_group='DEFAULT',
                    deprecated_name="http_control_exchanges",
                    help="Exchanges name to listen for notifications."),
]

# TODO: Cache the samples properly
serverData = list()

class Server(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        for d in serverData:
            self.wfile.write(bytes(d, "utf-8"))

class PrometheusExportService(cotyledon.Service):
    """Prometheus export service.
    """

    def __init__(self, worker_id, conf, coordination_id=None):
        super(PrometheusExportService, self).__init__(worker_id)
        # TODO: parse these (and other) values from config
        socket_port = 4209
        http_port = 4201
        print("STARTING")

        self.startup_delay = worker_id
        self.conf = conf
        self.listeners = []
        self.end = False

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('', 4209))
        self.listener = threading.Thread(target=self.run_listener)

        self.sender = threading.Thread(target=self.run_server)
        self.webServer = HTTPServer(("0.0.0.0", 4201), Server)

    @staticmethod
    def parse_sample(s):
        print("PARSING")
        data = ""
        doc_done = set()
        metric_type = None
        if s["counter_type"] == sample.TYPE_CUMULATIVE:
            metric_type = "counter"
        elif s["counter_type"] == sample.TYPE_GAUGE:
            metric_type = "gauge"

        curated_sname = s["counter_name"].replace(".", "_")

        if metric_type and curated_sname not in doc_done:
            data += "# TYPE %s %s\n" % (curated_sname, metric_type)
            doc_done.add(curated_sname)

        # NOTE(jwysogla): should we uncomment this?
        # NOTE(sileht): prometheus pushgateway doesn't allow to push
        # timestamp_ms
        #
        # timestamp_ms = (
        #     s.get_iso_timestamp().replace(tzinfo=None) -
        #     datetime.utcfromtimestamp(0)
        # ).total_seconds() * 1000
        # data += '%s{resource_id="%s"} %s %d\n' % (
        #     curated_sname, s.resource_id, s.volume, timestamp_ms)

        data += '%s{resource_id="%s", project_id="%s"} %s\n' % (
            curated_sname, s["resource_id"], s["project_id"], s["counter_volume"])
        return data

    def run_listener(self):
        while not self.end:
            message = ""
            ready = [[]]
            while not self.end and not ready[0]:
                ready = select.select([self.socket], [], [], 5)
            if ready[0]:
                message = self.socket.recv(2048)
                sample = msgpack.loads(message)
                parsed = self.parse_sample(sample)
                serverData.append(parsed)

    def run_server(self):
        self.webServer.serve_forever()

    def run(self):
        self.listener.start()
        self.sender.start()

    def terminate(self):
        self.end = True
        self.socket.close()
        self.listener.join()
        self.webServer.shutdown()
        self.webServer.server_close()
        self.sender.join()

        super(PrometheusExportService, self).terminate()
