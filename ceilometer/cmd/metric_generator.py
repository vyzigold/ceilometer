#!/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012-2014 Julien Danjou
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

"""Command line tool for creating meter for Ceilometer.
"""
import logging
import sys

from oslo_config import cfg
from oslo_utils import timeutils

from ceilometer.pipeline import sample as sample_pipe
from ceilometer import sample
from ceilometer import service

class MultiChoicesOpt(cfg.Opt):
    def __init__(self, name, choices=None, **kwargs):
        super(MultiChoicesOpt, self).__init__(
            name, type=DeduplicatedCfgList(choices), **kwargs)
        self.choices = choices

    def _get_argparse_kwargs(self, group, **kwargs):
        """Extends the base argparse keyword dict for multi choices options."""
        kwargs = super(MultiChoicesOpt, self)._get_argparse_kwargs(group)
        kwargs['nargs'] = '+'
        choices = kwargs.get('choices', self.choices)
        if choices:
            kwargs['choices'] = choices
        return kwargs


class DeduplicatedCfgList(cfg.types.List):
    def __init__(self, choices=None, **kwargs):
        super(DeduplicatedCfgList, self).__init__(**kwargs)
        self.choices = choices or []

    def __call__(self, *args, **kwargs):
        result = super(DeduplicatedCfgList, self).__call__(*args, **kwargs)
        result_set = set(result)
        if len(result) != len(result_set):
            LOG.warning("Duplicated values: %s found in CLI options, "
                        "auto de-duplicated", result)
            result = list(result_set)
        if self.choices and not (result_set <= set(self.choices)):
            raise Exception('Valid values are %s, but found %s'
                            % (self.choices, result))
        return result


CLI_OPTS = [
    MultiChoicesOpt('polling-namespaces',
                    default=['compute', 'central'],
                    dest='polling_namespaces',
                    help='Polling namespace(s) to be used while '
                         'resource polling')
]


def _prepare_config():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts(CLI_OPTS)
    service.prepare_service(conf=conf)
    return conf


def create_polling_service(worker_id, conf=None):
    if conf is None:
        conf = _prepare_config()
        conf.log_opt_values(LOG, log.ERROR)
    return manager.AgentManager(worker_id, conf, conf.polling_namespaces)



def send_sample():

    service.prepare_service()

    # Set up logging to use the console
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    root_logger = logging.getLogger('')
    root_logger.addHandler(console)
    root_logger.setLevel(logging.ERROR)

    conf = _prepare_config()
    pipeline_manager = sample_pipe.SamplePipelineManager(conf)

    with pipeline_manager.publisher() as p:
        for i in range(0, 20):
            p([sample.Sample(
                name="test_name" + str(i),
                type="test_type",
                unit="test_unit",
                volume=2063,
                user_id="test_user_id",
                project_id="test_project_id",
                resource_id="test_resource_id",
                timestamp="2022-09-15T21:00:51+00:00",
                resource_metadata="test_resource_metadata")])

send_sample()

