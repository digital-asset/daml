// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers

import com.digitalasset.daml.lf.iface.Interface
import com.digitalasset.extractor.ledger.types.Event
import com.digitalasset.extractor.targets.PrettyPrintTarget
import _root_.pprint.PPrinter

class PrettyPrintWriter(val target: PrettyPrintTarget) extends PrinterFunctionWriter with Writer {
  private val pprinter: PPrinter = PPrinter(target.width, target.height)
  private def pprint(x: Any): Unit = pprinter.pprintln(x)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def printer: Any => Unit = pprint

  def handlePackage(id: String, interface: Interface): Unit = {
    printer(s"""Information of Package "${id}":""")
    printer(s"#######################################################")
    printer(s"TypeDecls:")
    printer(interface.typeDecls)
    printer("#######################################################")
  }

  def printEvent(event: Event): Unit = printer(event)
}
