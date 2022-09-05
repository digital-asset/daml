// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.perf.util.reporter

import java.io.File

import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, TransformerFactory}
import org.scalameter.CurveData
import org.scalameter.api.{Persistor, Reporter, _}
import org.scalameter.utils.Tree
import org.w3c.dom.Document

/** Produces a file which's contents mimic JMeter's XML output
  * http://jmeter.apache.org/usermanual/listeners.html#xmlformat2.1
  */
class JMeterReporter[T: Numeric](clazz: Class[_]) extends Reporter[T] {

  private def writeToFile(document: Document, file: File): Unit = {
    val transformer = TransformerFactory.newInstance.newTransformer
    transformer.setOutputProperty(OutputKeys.INDENT, "yes")
    val source = new DOMSource(document)
    val console = new StreamResult(file)
    transformer.transform(source, console)
  }

  def report(results: Tree[CurveData[T]], persistor: Persistor): Boolean = {

    // resultDir is global setting
    val resultDirLocation = org.scalameter.currentContext(reports.resultDir)
    val resultDir = new File(resultDirLocation, "jmeter")
    resultDir.mkdirs()
    val resultFile = new File(resultDir, s"${clazz.getName.stripSuffix("$")}.xml")

    val document = JMeterXmlGenerator
      .appendToDocument(results, resultFile)

    writeToFile(document, resultFile)

    true
  }

  def report(result: CurveData[T], persistor: Persistor): Unit = ()
}
