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

import threading
import etcd
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
etcd_host = "192.168.122.13"
etcd_port = 2379
etcd_client = etcd.Client(host=etcd_host, port=etcd_port)

class Server(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        #for d in serverData:
        #    self.wfile.write(bytes(d, "utf-8"))
        keys = etcd_client.get("/ceilometer/keys")
        for key in keys.children:
            try:
                metric = etcd_client.get("/ceilometer/" + key.value).value
                self.wfile.write(bytes(metric, "utf-8"))
            except:
                print("Key: " + key.value + " doesn't exist")

class PrometheusExportService(cotyledon.Service):
    """Prometheus export service.
    """

    def __init__(self, worker_id, conf, coordination_id=None):
        super(PrometheusExportService, self).__init__(worker_id)
        # TODO: parse these (and other) values from config
        http_port = 4201
        print("STARTING")

        self.startup_delay = worker_id
        self.conf = conf

        self.sender = threading.Thread(target=self.run_server)
        self.webServer = HTTPServer(("0.0.0.0", 4201), Server)


    def run_server(self):
        self.webServer.serve_forever()

    def run(self):
        self.sender.start()

    def terminate(self):
        self.webServer.shutdown()
        self.webServer.server_close()
        self.sender.join()

        super(PrometheusExportService, self).terminate()
