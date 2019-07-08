#!/usr/bin/env python2.7
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import sys

try:
  with open(sys.argv[1], 'r') as f:
    json.load(f)
except Exception as e:
  print "Invalid JSON in '%s': %s" % (sys.argv[1], e)
  sys.exit(1)

