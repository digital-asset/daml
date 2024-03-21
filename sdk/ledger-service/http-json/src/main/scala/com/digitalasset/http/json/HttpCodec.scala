// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import com.daml.scalautil.ExceptionOps._
import scalaz.std.stream.unfold
import scalaz.{@@, Tag}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/** Integrating spray json into akka http. */
object HttpCodec {
  sealed trait JsonApi
  val JsonApi = Tag.of[JsonApi]

  implicit val jsonExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: DeserializationException =>
      complete(
        (
          StatusCodes.BadRequest,
          ResponseFormats.errorsJsObject(
            StatusCodes.BadRequest,
            s"JSON parser error: ${e.msg}" +: unfoldCauses(e.cause).map(_.description): _*
          ),
        )
      )
  }

  private[this] def unfoldCauses(t: Throwable): Seq[Throwable] =
    unfold(t)(tnq => Option(tnq) map (tnn => (tnn, tnn.getCause)))

  private[this] val simpleJsValueUnmarshaller =
    Unmarshaller[String, JsValue] { implicit ec: ExecutionContext => value =>
      Future(value.parseJson)
    }

  implicit val jsValueUnmarshaller: FromEntityUnmarshaller[JsValue] =
    (implicitly[FromEntityUnmarshaller[String]] andThen simpleJsValueUnmarshaller
      forContentTypes `application/json`)

  implicit val jsValueMarshaller: ToEntityMarshaller[JsValue] =
    Marshaller.combined((_: JsValue).compactPrint)(Marshaller.stringMarshaller(`application/json`))

  implicit def jsonCodecUnmarshaller[A: RootJsonReader]: FromEntityUnmarshaller[A @@ JsonApi] =
    JsonApi.subst[FromEntityUnmarshaller, A](jsValueUnmarshaller map (_.convertTo[A]))

  implicit def jsonCodecMarshaller[A: RootJsonWriter]: ToEntityMarshaller[A @@ JsonApi] =
    JsonApi.subst[ToEntityMarshaller, A](Marshaller.combined((_: A).toJson))
}
