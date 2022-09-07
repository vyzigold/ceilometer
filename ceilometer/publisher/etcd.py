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
from ceilometer import publisher
from urllib import parse
from oslo_log import log

import etcd
from datetime import datetime

LOG = log.getLogger(__name__)

class EtcdPublisher(publisher.ConfigPublisherBase):
    """Publish metering data to etcd

    To use this publisher for samples, add the following section to the
    /etc/ceilometer/pipeline.yaml file or simply add it to an existing
    pipeline::

          - name: meter_file
            meters:
                - "*"
            publishers:
                - etcd://host:port?expiration=1

    Following directory structure is created inside etcd:
    /ceilometer/<metric_name>/timestamp/<concatenated_metric_labels>
        - when the metric was read
    /ceilometer/<metric_name>/data/<concatenated_metric_labels>
        - a string representing the metric in prometheus compatible syntax
    /ceilometer/<metric_name>/type
        - a string representing the type in prometheus compatible syntax
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

        params = parse.parse_qs(parsed_url.query)
        self.expiration = self._get_param(params, 'expiration', 60, int)
        self.etcd_client = etcd.Client(host=parsed_url.hostname,
                port=parsed_url.port)

        # Try to write to etcd to determine if the client is able to connect
        # with the provided host and port

        try:
            self.etcd_client.write("/ceilometer/test", test, ttl=1)
        except etcd.EtcdConnectionFailed:
            raise ValueError('Cannot connect to etcd using the '
                             'provided host and port values')

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
        return '%s%s' % (s.resource_id, s.project_id)

    def update_key(self, location, key):
        print("UPDATING")
        print(location)
        print(key)
        existing_keys = self.etcd_client.get("location")
        keys_list = [i.value for i in existing_keys.children]
        if key not in keys_list:
            self.etcd_client.write("location", key, append=True, ttl=self.expiration)
        else:
            self.etcd_client.refresh(existing_keys.children[keys_list.index(key)].key, ttl=self.expiration)

    def publish_samples(self, samples):
        """Send a metering message for publishing

        :param samples: Samples from pipeline after transformation
        """
        if not samples:
            return

        for s in samples:
            data = ""
            metric_type = None
            if s.type == sample.TYPE_CUMULATIVE:
                metric_type = "counter"
            elif s.type == sample.TYPE_GAUGE:
                metric_type = "gauge"

            curated_sname = s.name.replace(".", "_")

            metric_type = "# TYPE %s %s\n" % (curated_sname, metric_type)

            timestamp_ms = (
                s.get_iso_timestamp().replace(tzinfo=None) -
                datetime.utcfromtimestamp(0)
            ).total_seconds() * 1000
            data += '%s{resource_id="%s", project_id="%s"} %s %d\n' % (
                curated_sname, s.resource_id, s.project_id, s.volume, timestamp_ms)

            key = self.get_metric_key(s)

            # don't write older metric
            try:
                ts = self.etcd_client.get("/ceilometer/" + curated_sname + "/timestamp/" + key).value
                if ts > timestamp_ms:
                    continue
            except etcd.EtcdKeyNotFound:
                pass
            self.etcd_client.write("/ceilometer/" + curated_sname + "/data/" + key, data, ttl=self.expiration)
            self.etcd_client.write("/ceilometer/" + curated_sname + "/timestamp/" + key, timestamp_ms, ttl=self.expiration)
            self.etcd_client.write("/ceilometer/" + curated_sname + "/type", metric_type, ttl=self.expiration)
            self.etcd_client.refresh("/ceilometer/" + curated_sname, ttl=self.expiration)

    @staticmethod
    def publish_events(events):
        raise NotImplementedError
