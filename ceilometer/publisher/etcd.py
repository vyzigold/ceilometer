#
# Copyright 2016 IBM
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

from ceilometer import sample
import ceilometer
from ceilometer import publisher
from urllib import parse
from oslo_log import log

import etcd

LOG = log.getLogger(__name__)

class EtcdPublisher(publisher.ConfigPublisherBase):
    """Publish metering data to Prometheus Pushgateway endpoint

    This dispatcher inherits from all options of the http dispatcher.

    To use this publisher for samples, add the following section to the
    /etc/ceilometer/pipeline.yaml file or simply add it to an existing
    pipeline::

          - name: meter_file
            meters:
                - "*"
            publishers:
                - etcd://host:2379?timeout=1

    """

    def __init__(self, conf, parsed_url):
        super(EtcdPublisher, self).__init__(conf, parsed_url)

        if not parsed_url.hostname:
            raise ValueError('The hostname of an endpoint for '
                             'EtcdPublisher is required')

        # non-numeric port from the url string will cause a ValueError
        # exception when the port is read. Do a read to make sure the port
        # is valid, if not, ValueError will be thrown.
        parsed_url.port

        # Handling other configuration options in the query string
        params = parse.parse_qs(parsed_url.query)
        self.timeout = self._get_param(params, 'timeout', 5, int)
        self.max_retries = self._get_param(params, 'max_retries', 2, int)
        self.etcd_client = etcd.Client(host=parsed_url.hostname,
                port=parsed_url.port)

        LOG.debug('EtcdPublisher for endpoint %s is initialized!' %
                  parsed_url.hostname)

    @staticmethod
    def _get_param(params, name, default_value, cast=None):
        try:
            return cast(params.pop(name)[-1]) if cast else params.pop(name)[-1]
        except (ValueError, TypeError, KeyError):
            LOG.debug('Default value %(value)s is used for %(name)s' %
                      {'value': default_value, 'name': name})
            return default_value

    @staticmethod
    def get_metric_key(s):
        return '%s%s%s' % (s.name, s.resource_id, s.project_id)

    def publish_samples(self, samples):
        if not samples:
            return
        data = ""
        doc_done = set()

        for s in samples:
            metric_type = None
            if s.type == sample.TYPE_CUMULATIVE:
                metric_type = "counter"
            elif s.type == sample.TYPE_GAUGE:
                metric_type = "gauge"

            curated_sname = s.name.replace(".", "_")

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

            data = '%s{resource_id="%s", project_id="%s"} %s\n' % (
                curated_sname, s.resource_id, s.project_id, s.volume)
            key = self.get_metric_key(s)
            self.etcd_client.write("/ceilometer/" + key, data)
            self.etcd_client.write("/ceilometer/keys", key, append=True)


    @staticmethod
    def publish_events(events):
        raise NotImplementedError
