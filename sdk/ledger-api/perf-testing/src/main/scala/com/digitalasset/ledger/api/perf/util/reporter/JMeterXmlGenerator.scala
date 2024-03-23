// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.perf.util.reporter

import java.io.File
import java.time.Clock

import javax.xml.parsers.DocumentBuilderFactory
import org.scalameter.utils.Tree
import org.scalameter.{CurveData, Parameters}
import org.w3c.dom.{Document, Element}

private[reporter] object JMeterXmlGenerator {

  private val rootElementName = "testResults"

  private def newDocument[T: Numeric](results: Tree[CurveData[T]]): Document = {
    val doc = newDocBuilder.newDocument
    val root = doc.createElementNS(null, rootElementName)
    doc.appendChild(root)
    addMeasurementsToDocument(results, doc, root)
  }

  private def newDocBuilder = {
    DocumentBuilderFactory.newInstance.newDocumentBuilder
  }

  def appendToDocument[T: Numeric](results: Tree[CurveData[T]], file: File): Document = {
    if (file.exists()) {
      val doc = newDocBuilder.parse(file) // Will throw on failure.
      Option(doc.getDocumentElement)
        .filter(_.getNodeName == rootElementName)
        .fold(
          sys.error(
            s"Cannot append to malformed XML. Root element '$rootElementName' was not found in file '${file.getAbsolutePath}'."
          )
        )(addMeasurementsToDocument(results, doc, _))
    } else {
      newDocument(results)
    }

  }

  private def addMeasurementsToDocument[T: Numeric](
      results: Tree[CurveData[T]],
      doc: Document,
      root: Element,
  ) = {
    val reportGenerationTime: String = Clock.systemUTC().millis().toString
    val measurementXmlGenerator = new MeasurementXmlGenerator[T](doc, reportGenerationTime)
    for {
      result <- results
      xmlElement <- measurementXmlGenerator.asXmlElements(result)
    } {
      root.appendChild(xmlElement)
    }
    doc
  }

  private class MeasurementXmlGenerator[T: Numeric](doc: Document, generationTime: String) {

    def asXmlElements(result: CurveData[T]): Iterable[Element] = {
      import AttributeNames._

      for {
        aggregate <- result.measurements
        measurement <- aggregate.data.complete
      } yield {
        val node = doc.createElement("sample")
        val testName = result.context.scope
        val paramsStr = paramSuffix(aggregate.params)
        val resp = FakeResponseData(aggregate.data.success)

        node.setAttribute(label, s"$testName$paramsStr")
        node.setAttribute(timestamp, generationTime)
        node.setAttribute(
          time,
          implicitly[Numeric[T]].toLong(measurement).toString,
        ) // No floating point results observed among samples
        node.setAttribute(responseCode, resp.code)
        node.setAttribute(responseMessage, resp.message)
        node.setAttribute(success, resp.success)
        node.setAttribute(latency, "0")
        node.setAttribute(threadName, "-")

        node
      }
    }

    private def paramSuffix(parameters: Parameters): String = {
      val filtered: Vector[String] = parameters.axisData.view
        .map { case (key, value) =>
          s"${key.fullName}:$value"
        }
        .to(Vector)

      if (filtered.isEmpty) ""
      else
        filtered.sorted // to make sure order remains stable between runs in Jenkins so it can match up new results
          .mkString(".[", ",", "]")

    }
  }

  private object FakeResponseData {
    case class FakeResponseData(code: String, message: String, success: String)

    private val success = FakeResponseData("200", "OK", "true")
    private val failure = FakeResponseData("500", "FAIL", "false")

    def apply(boolean: Boolean): FakeResponseData = if (boolean) success else failure
  }

  private object AttributeNames {
    val label = "lb"
    val timestamp = "ts"
    val time = "t"
    val responseCode = "rc"
    val responseMessage = "rm"
    val success = "s"
    val latency = "lt"
    val threadName = "tn"
  }
}
