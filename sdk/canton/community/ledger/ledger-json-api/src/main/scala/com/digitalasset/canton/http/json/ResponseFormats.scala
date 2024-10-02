// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.stream.{FanOutShape2, SourceShape, UniformFanInShape}
import org.apache.pekko.util.ByteString
import com.digitalasset.canton.fetchcontracts.util.PekkoStreamsUtils
import scalaz.syntax.show.*
import scalaz.{Show, \/}
import spray.json.*

object ResponseFormats {
  def resultJsObject[A: JsonWriter](a: A): JsObject =
    resultJsObject(a.toJson)

  def resultJsObject(a: JsValue): JsObject =
    JsObject(statusField(StatusCodes.OK), ("result", a))

  def resultJsObject[E: Show](
      jsVals: Source[E \/ JsValue, NotUsed],
      warnings: Option[JsValue],
  ): Source[ByteString, NotUsed] = {

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits.*

      val partition: FanOutShape2[E \/ JsValue, E, JsValue] = b add PekkoStreamsUtils.partition
      val concat: UniformFanInShape[ByteString, ByteString] = b add Concat(3)

      // first produce optional warnings and result element
      warnings match {
        case Some(x) =>
          Source.single(ByteString(s"""{"warnings":${x.compactPrint},"result":[""")) ~> concat.in(0)
        case None =>
          Source.single(ByteString("""{"result":[""")) ~> concat.in(0)
      }

      jsVals ~> partition.in

      // second consume all successes
      partition.out1.zipWithIndex.map(a => formatOneElement(a._1, a._2)) ~> concat.in(1)

      // then consume all failures and produce the status and optional errors
      partition.out0.fold(Vector.empty[E])((b, a) => b :+ a).map {
        case Vector() =>
          ByteString("""],"status":200}""")
        case errors =>
          val jsErrors: Vector[JsString] = errors.map(e => JsString(e.shows))
          ByteString(s"""],"errors":${JsArray(jsErrors).compactPrint},"status":501}""")
      } ~> concat.in(2)

      SourceShape(concat.out)
    }

    Source.fromGraph(graph)
  }

  private def formatOneElement(a: JsValue, index: Long): ByteString =
    if (index == 0L) ByteString(a.compactPrint)
    else ByteString("," + a.compactPrint)

  def statusField(status: StatusCode): (String, JsNumber) =
    ("status", JsNumber(status.intValue()))
}
