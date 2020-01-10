# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test for the Ambrite example."""

# pytype: skip-file

from __future__ import absolute_import

import Ambrite
import collections
import logging
import re
import tempfile
import unittest
from apache_beam.testing.util import open_shards


class AmbriteTest(unittest.TestCase):

    SAMPLE_USERS = (
        u'1,Mabelle,AAKVwRLMqGm\n2,Nowell,NoXkciK6e\n1,1,1\n,,\n')

    def test_split_and_lower(self):
        self.assertEqual(Ambrite.split_and_lower(
            '1,Mabelle,AAKVwRLMqGm'), ['1', 'mabelle', 'aakvwrlmqgm'])
        self.assertEqual(Ambrite.split_and_lower(
            '1,AAKVwRLMqGm'), ['1', 'aakvwrlmqgm'])

    def test_no_num_format(self):
        self.assertEqual(Ambrite.no_num_format(
            ['1', 'Mabelle', 'AAKVwRLMqGm']), ',Mabelle,AAKVwRLMqGm,')
        self.assertEqual(Ambrite.no_num_format(
            ['1', 'AAKVwRLMqGm']), ',AAKVwRLMqGm,')

    def test_format_output(self):
        self.assertEqual(Ambrite.format_output(
            ',Mabelle,AAKVwRLMqGm'), 'Mabelle,AAKVwRLMqGm')
        self.assertEqual(Ambrite.format_output(
            ',,Mabelle,AAKVwRLMqGm'), ',Mabelle')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
