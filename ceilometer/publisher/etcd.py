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

import redis
from datetime import datetime

LOG = log.getLogger(__name__)

r = redis.Redis(
        host='127.0.0.1',
        port=6379)


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

        # Try to write to etcd to determine if the client is able to connect
        # with the provided host and port


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
    def get_metric_key(curated_sname, s):
        return '%s%s%s' % (curated_sname, s.resource_id, s.project_id)

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
                curated_sname, s.resource_id, s.project_id,
                s.volume, timestamp_ms)

            # "ceilometer_metric_names" - list of all curated_snames
            # "ceilometer_<metric_name>_type" - type of the metric in prometheus format
            # "ceilometer_<metric_name>_keys" - list of keys of that metric (same name, different labels)
            # "ceilometer_<key_from_above>" - metric in prometheus format
            # "ceilometer_<key_from_above>_timestamp" - timestamp of that metric

            key = self.get_metric_key(curated_sname, s)

            if r.lpos("ceilometer_metric_names", curated_sname) is None:
                r.lpush("ceilometer_metric_names", curated_sname)
            r.set("ceilometer_" + curated_sname + "_type", metric_type)
            if r.lpos("ceilometer_" + curated_sname + "_keys", key) is None:
                r.lpush("ceilometer_" + curated_sname + "_keys", key)
            previous_ts = r.get("ceilometer_" + key + "_timestamp")
            # don't write older metric
            if previous_ts is not None and int(float(previous_ts)) > timestamp_ms:
                continue

            pipe = r.pipeline()
            pipe.setex("ceilometer_" + key, 300, data)
            pipe.setex("ceilometer_" + key + "_timestamp", 300, timestamp_ms)
            pipe.execute()

            # cleanup the lists when the receiving side finds out, that the keys expired? So not here?

    @staticmethod
    def publish_events(events):
        raise NotImplementedError
