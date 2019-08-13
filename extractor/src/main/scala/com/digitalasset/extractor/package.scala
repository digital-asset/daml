// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import pprint.PPrinter

package object extractor {
  val pprinter: PPrinter = PPrinter(200, 1000)
  def pprint(x: Any): Unit = pprinter.pprintln(x)
  def pformat(x: Any): String = pprinter(x).toString()
}
