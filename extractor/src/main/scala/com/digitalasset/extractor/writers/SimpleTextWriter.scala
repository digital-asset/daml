// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers

import com.digitalasset.daml.lf.iface.Interface
import com.digitalasset.extractor.ledger.types.Event

class SimpleTextWriter(val printer: Any => Unit) extends PrinterFunctionWriter with Writer {
  def handlePackage(id: String, interface: Interface): Unit = {
    printer(s"""Information of Package "$id":""")
    printer(s"#######################################################")
    printer(s"${interface.toString}")
    printer("########################################################")
  }

  def printEvent(event: Event): Unit = printer(event.toString)
}
