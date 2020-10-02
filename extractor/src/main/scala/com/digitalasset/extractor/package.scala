// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import pprint.PPrinter

package object extractor {
  val pprinter: PPrinter = PPrinter(200, 1000)
  def pprint(x: Any): Unit = pprinter.pprintln(x)
  def pformat(x: Any): String = pprinter(x).toString()
}
