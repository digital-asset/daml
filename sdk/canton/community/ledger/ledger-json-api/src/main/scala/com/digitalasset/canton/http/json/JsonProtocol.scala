// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http
import com.digitalasset.canton.http.*
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.struct.Struct
import io.circe.generic.semiauto.*
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import org.apache.pekko.http.scaladsl.model.StatusCode
import scalaz.syntax.tag.*
import scalaz.{@@, Tag}

import scala.concurrent.duration.*
import scala.util.Try

object JsonProtocol {

  import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.{
    decodeStruct,
    encodeStruct,
  }

  // Helpers for tagged types
  private def taggedEncoder[A: Encoder, T]: Encoder[A @@ T] =
    Tag.subst(implicitly(Encoder[A]))

  private def taggedDecoder[A: Decoder, T]: Decoder[A @@ T] =
    Tag.subst(implicitly(Decoder[A]))

  implicit val PartyEncoder: Encoder[http.Party] = taggedEncoder
  implicit val PartyDecoder: Decoder[http.Party] = taggedDecoder

  implicit val HexStringDecoder: Decoder[Ref.HexString] =
    Decoder.decodeString.emap(s => Ref.HexString.fromString(s).left.map(_.toString))
  implicit val HexStringEncoder: Encoder[Ref.HexString] =
    Encoder.encodeString.contramap(_.toString)

  implicit val StatusCodeEncoder: Encoder[StatusCode] =
    Encoder.instance(sc => Json.fromInt(sc.intValue))
  implicit val StatusCodeDecoder: Decoder[StatusCode] =
    Decoder.decodeInt.emap(i => Try(StatusCode.int2StatusCode(i)).toEither.left.map(_.getMessage))

  implicit val ResourceInfoDetailEncoder: Encoder[http.ResourceInfoDetail] = deriveEncoder
  implicit val ResourceInfoDetailDecoder: Decoder[http.ResourceInfoDetail] = deriveDecoder

  implicit val ErrorInfoDetailEncoder: Encoder[http.ErrorInfoDetail] = deriveEncoder
  implicit val ErrorInfoDetailDecoder: Decoder[http.ErrorInfoDetail] = deriveDecoder

  implicit val RetryInfoDetailDurationEncoder: Encoder[http.RetryInfoDetailDuration] =
    Encoder.encodeLong.contramap[http.RetryInfoDetailDuration](_.unwrap.toNanos)
  implicit val RetryInfoDetailDurationDecoder: Decoder[http.RetryInfoDetailDuration] =
    Decoder.decodeLong.map[http.RetryInfoDetailDuration](l =>
      http.RetryInfoDetailDuration(Duration.fromNanos(l): Duration)
    )

  implicit val RetryInfoDetailEncoder: Encoder[http.RetryInfoDetail] = deriveEncoder
  implicit val RetryInfoDetailDecoder: Decoder[http.RetryInfoDetail] = deriveDecoder

  implicit val RequestInfoDetailEncoder: Encoder[http.RequestInfoDetail] = deriveEncoder
  implicit val RequestInfoDetailDecoder: Decoder[http.RequestInfoDetail] = deriveDecoder

  implicit val ErrorDetailEncoder: Encoder[http.ErrorDetail] = Encoder.instance {
    case resourceInfoDetail: http.ResourceInfoDetail =>
      ResourceInfoDetailEncoder(resourceInfoDetail).deepMerge(
        Json.obj("type" -> Json.fromString(classOf[ResourceInfoDetail].getSimpleName))
      )
    case errorInfoDetail: http.ErrorInfoDetail =>
      ErrorInfoDetailEncoder(errorInfoDetail).deepMerge(
        Json.obj("type" -> Json.fromString(classOf[ErrorInfoDetail].getSimpleName))
      )
    case retryInfoDetail: http.RetryInfoDetail =>
      RetryInfoDetailEncoder(retryInfoDetail).deepMerge(
        Json.obj("type" -> Json.fromString(classOf[RetryInfoDetail].getSimpleName))
      )
    case requestInfoDetail: http.RequestInfoDetail =>
      RequestInfoDetailEncoder(requestInfoDetail).deepMerge(
        Json.obj("type" -> Json.fromString(classOf[RequestInfoDetail].getSimpleName))
      )
  }

  implicit val ErrorDetailDecoder: Decoder[http.ErrorDetail] = Decoder.instance { c =>
    c.get[String]("type").flatMap {
      case t if t == classOf[ResourceInfoDetail].getSimpleName => c.as[http.ResourceInfoDetail]
      case t if t == classOf[ErrorInfoDetail].getSimpleName => c.as[http.ErrorInfoDetail]
      case t if t == classOf[RetryInfoDetail].getSimpleName => c.as[http.RetryInfoDetail]
      case t if t == classOf[RequestInfoDetail].getSimpleName => c.as[http.RequestInfoDetail]
      case other => Left(DecodingFailure(s"Unknown error detail type: $other", c.history))
    }
  }

  implicit val LedgerApiErrorEncoder: Encoder[http.LedgerApiError] = deriveEncoder
  implicit val LedgerApiErrorDecoder: Decoder[http.LedgerApiError] = deriveDecoder

  implicit val ErrorResponseEncoder: Encoder[http.ErrorResponse] = deriveEncoder
  implicit val ErrorResponseDecoder: Decoder[http.ErrorResponse] = deriveDecoder

  implicit val StructEncoder: Encoder[Struct] = encodeStruct

  implicit val StructDecoder: Decoder[Struct] = decodeStruct
}
