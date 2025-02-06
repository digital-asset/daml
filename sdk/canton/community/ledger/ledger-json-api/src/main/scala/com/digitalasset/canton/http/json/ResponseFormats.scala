// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Concat, Sink, Source}
import org.apache.pekko.util.ByteString
import scalaz.syntax.show.*
import scalaz.{-\/, Show, \/, \/-}
import spray.json.*

import scala.concurrent.{ExecutionContext, Future}

object ResponseFormats {
  def resultJsObject[A: JsonWriter](a: A): JsObject =
    resultJsObject(a.toJson)

  def resultJsObject(a: JsValue): JsObject =
    JsObject(("status", JsNumber(OK.intValue)), ("result", a))

  def resultJsObject[E: Show](
      jsVals: Source[E \/ JsValue, NotUsed],
      warnings: Option[JsValue],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(Source[ByteString, NotUsed], StatusCode)] =
    jsVals
      .runWith {
        Sink
          // Collapse the stream of `E \/ JsValue` into a single pair of errors and results,
          // only one of which may be non-empty.
          .fold((Vector.empty[E], Vector.empty[JsValue])) {
            case ((errors, results), \/-(r)) if errors.isEmpty => (Vector.empty, results :+ r)
            case ((errors, _), \/-(_)) => (errors, Vector.empty)
            case ((errors, _), -\/(e)) => (errors :+ e, Vector.empty)
          }
      }
      .map { case (errors, results) =>
        // Convert that into a stream containing the appropriate JSON response object
        val (name, vals, statusCode): (String, Iterator[JsValue], StatusCode) =
          if (errors.nonEmpty)
            ("errors", errors.iterator.map(e => JsString(e.shows)), InternalServerError)
          else
            ("result", results.iterator, OK)
        val payload = arrayField(name, vals)
        val status = scalarField("status", JsNumber(statusCode.intValue))
        val comma = single(",")
        val jsonSource: Source[ByteString, NotUsed] = Source.combine(
          single("{"),
          warnings.fold(Source.empty[ByteString])(scalarField("warnings", _) ++ comma),
          payload,
          comma,
          status,
          single("}"),
        )(Concat(_))
        (jsonSource, statusCode)
      }

  private def single(value: String): Source[ByteString, NotUsed] =
    Source.single(ByteString(value))

  private def scalarField(name: String, value: JsValue): Source[ByteString, NotUsed] =
    single(s""""$name":${value.compactPrint}""")

  private def arrayField(name: String, items: Iterator[JsValue]): Source[ByteString, NotUsed] = {
    val csv = Source.fromIterator(() =>
      items.zipWithIndex.map { case (r, i) =>
        val str = r.compactPrint
        ByteString(if (i == 0) str else "," + str)
      }
    )
    single(s""""$name":[""") ++ csv ++ single("]")
  }
}
