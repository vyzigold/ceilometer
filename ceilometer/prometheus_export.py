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
import redis
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

r = redis.Redis(
        host='127.0.0.1',
        port=6379)

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


        self.sender = threading.Thread(target=self.run_server)
        handler = self.get_handler(
                                   remove_after_scrape)
        self.webServer = HTTPServer(("0.0.0.0", http_port),
                                    handler)

    def get_handler(self, delete=False):
        class Handler(BaseHTTPRequestHandler):

            def displayMetrics(self):
                metric_names = r.lrange("ceilometer_metric_names", 0, -1)
                for name in metric_names:
                    name_str = name.decode("utf-8")
                    metric_type = r.get("ceilometer_" + name_str + "_type")
                    self.wfile.write(metric_type)
                    keys = r.lrange("ceilometer_" + name_str + "_keys", 0, -1)
                    for key in keys:
                        metric = r.get("ceilometer_" + key.decode("utf-8"))
                        if metric is None:
                            # delete old key from the list
                            r.lrem("ceilometer_" + name_str + "_keys", 1, key.decode("utf-8"))
                        else:
                            self.wfile.write(metric)

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()

                self.displayMetrics()
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
