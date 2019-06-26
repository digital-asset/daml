package com.digitalasset.http.json

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

object HttpCodec {
  private[this] val simpleJsValueUnmarshaller =
    Unmarshaller[String, JsValue] { implicit ec: ExecutionContext => value =>
      Future(value.parseJson)
    }

  implicit val jsValueUnmarshaller: FromEntityUnmarshaller[JsValue] =
    (implicitly[FromEntityUnmarshaller[String]] andThen simpleJsValueUnmarshaller
      forContentTypes `application/json`)

  implicit val jsValueMarshaller: ToEntityMarshaller[JsValue] =
    Marshaller.combined((_: JsValue).compactPrint)(Marshaller.stringMarshaller(`application/json`))
}
