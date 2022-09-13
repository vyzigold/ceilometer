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

import cotyledon
from oslo_config import cfg
from oslo_log import log

from ceilometer.i18n import _

import threading
import etcd
from http.server import BaseHTTPRequestHandler, HTTPServer


LOG = log.getLogger(__name__)

OPTS = [
    cfg.BoolOpt('remove_after_scrape',
                default=False,
                help='Remove metrics from etcd after they get scraped'),
    cfg.StrOpt('etcd_host',
               default='localhost',
               help='Etcd host to read samples from'),
    cfg.IntOpt('etcd_port',
               default=2379,
               help='Etcd port to read samples from'),
    cfg.IntOpt('http_port',
               default=4201,
               help='Port on which to serve metrics for prometheus to scrape')
]


class PrometheusExportService(cotyledon.Service):
    """Prometheus export service.
    """

    def __init__(self, worker_id, conf, coordination_id=None):
        super(PrometheusExportService, self).__init__(worker_id)
        self.conf = conf

        etcd_host = conf.prometheus_exporter.etcd_host
        etcd_port = conf.prometheus_exporter.etcd_port
        http_port = conf.prometheus_exporter.http_port
        remove_after_scrape = conf.prometheus_exporter.remove_after_scrape

        self.etcd_client = etcd.Client(host=etcd_host, port=etcd_port)

        # verify the client can connect to etcd
        try:
            self.etcd_client.write("/ceilometer/test", "test", ttl=1)
        except etcd.EtcdConnectionFailed:
            LOG.error("Prometheus exporter cannot connect to etcd")

        self.sender = threading.Thread(target=self.run_server)
        handler = self.get_handler(self.etcd_client,
                                   remove_after_scrape)
        self.webServer = HTTPServer(("0.0.0.0", http_port),
                                    handler)

    def get_handler(self, etcd_client, delete=False):
        class Handler(BaseHTTPRequestHandler):

            def displayMetrics(self, root):
                for metric_name in root.children:
                    dir_name = metric_name.key
                    try:
                        metric_type = etcd_client.get(dir_name + "/type").value
                        self.wfile.write(bytes(metric_type, "utf-8"))

                        data = etcd_client.get(dir_name + "/data")
                        for d in data.children:
                            self.wfile.write(bytes(d.value, "utf-8"))
                        if delete:
                            self.etcd_client.delete(dir_name + "/data",
                                                    recursive=True)
                            self.etcd_client.delete(dir_name + "/timestamp",
                                                    recursive=True)
                    except etcd.EtcdKeyNotFound:
                        LOG.warning("Couldn't get data from etcd for "
                                    "metric named: " + dir_name)

            def do_GET(self):
                if self.path == "/metrics":
                    root = None
                    try:
                        root = etcd_client.get("/ceilometer")
                    except etcd.EtcdKeyNotFound:
                        # There are no data in etcd, display an empty page
                        return
                    except etcd.EtcdConnectionFailed:
                        LOG.error("Prometheus exporter cannot connect to etcd")
                        self.send_response(500)
                        self.end_headers()
                        return

                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()

                    self.displayMetrics(root)
                else:
                    self.send_response(404)
                    self.end_headers()
        return Handler

    def run_server(self):
        self.webServer.serve_forever()

    def run(self):
        self.sender.start()

    def terminate(self):
        self.webServer.shutdown()
        self.webServer.server_close()
        self.sender.join()

        super(PrometheusExportService, self).terminate()
