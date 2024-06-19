// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.error.*
import com.daml.error.ErrorCategory.GenericErrorCategory
import com.daml.error.utils.DecodedCantonError
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.google.protobuf
import com.google.protobuf.field_mask.FieldMask
import com.google.protobuf.struct.Struct
import com.google.protobuf.util.JsonFormat
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder}
import io.grpc.Status
import org.slf4j.event.Level
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{DecodeResult, Schema, SchemaType}

import java.time.Instant
import java.util.Base64
import scala.concurrent.duration.Duration
import scala.util.Try

object JsSchema {

  final case class JsCantonError(
      code: String,
      cause: String,
      correlationId: Option[String],
      traceId: Option[String],
      context: Map[String, String],
      resources: Seq[(String, String)],
      errorCategory: Int,
      grpcCodeValue: Option[Int],
  )

  object JsCantonError {
    implicit val rw: Codec[JsCantonError] = deriveCodec

    def fromErrorCode(damlError: DamlError): JsCantonError = JsCantonError(
      code = damlError.code.id,
      cause = damlError.cause,
      correlationId = damlError.errorContext.correlationId,
      traceId = damlError.errorContext.traceId,
      context = damlError.context,
      resources = damlError.resources.map { case (k, v) => (k.asString, v) },
      errorCategory = damlError.code.category.asInt,
      grpcCodeValue = None,
    )

    def fromDecodedCantonError(decodedCantonError: DecodedCantonError): JsCantonError =
      JsCantonError(
        code = decodedCantonError.code.id,
        errorCategory = decodedCantonError.code.category.asInt,
        grpcCodeValue = decodedCantonError.code.category.grpcCode.map(_.value()),
        cause = decodedCantonError.cause,
        correlationId = decodedCantonError.correlationId,
        traceId = decodedCantonError.traceId,
        context = decodedCantonError.context,
        resources = decodedCantonError.resources.map { case (k, v) => (k.toString, v) },
      )

    implicit val genericErrorClass: ErrorClass = ErrorClass(
      List(Grouping("generic", "ErrorClass"))
    )

    private final case class GenericErrorCode(
        override val id: String,
        override val category: ErrorCategory,
    ) extends ErrorCode(id, category)

    def toDecodedCantonError(jsCantonError: JsCantonError): DecodedCantonError =
      // TODO (i19398) revisit error handling code
      new DecodedCantonError(
        code = GenericErrorCode(
          id = jsCantonError.code,
          category = GenericErrorCategory(
            grpcCode = jsCantonError.grpcCodeValue.map(Status.fromCodeValue).map(_.getCode),
            logLevel = Level.INFO,
            retryable = None,
            securitySensitive = false,
            asInt = jsCantonError.errorCategory,
            rank = 1,
          ),
        ),
        cause = jsCantonError.cause,
        correlationId = jsCantonError.correlationId,
        traceId = jsCantonError.traceId,
        context = jsCantonError.context,
        resources = jsCantonError.resources.map { case (k, v) => (ErrorResource(k), v) },
      )
  }

  implicit def eitherDecoder[A, B](implicit
      a: Decoder[A],
      b: Decoder[B],
  ): Decoder[Either[A, B]] = {
    val left: Decoder[Either[A, B]] = a.map(Left.apply)
    val right: Decoder[Either[A, B]] = b.map(scala.util.Right.apply)
    // Always try to decode first the happy path
    right or left
  }

  implicit def eitherEncoder[A, B](implicit
      a: Encoder[A],
      b: Encoder[B],
  ): Encoder[Either[A, B]] =
    Encoder.instance(_.fold(a.apply, b.apply))

  object DirectScalaPbRwImplicits {

    implicit val om: Codec[ObjectMeta] = deriveCodec

    implicit val encodeDuration: Encoder[Duration] =
      Encoder.encodeString.contramap[Duration](_.toString)

    implicit val decodeDuration: Decoder[Duration] = Decoder.decodeString.emapTry { str =>
      Try(Duration.create(str))
    }

    implicit val byteStringSchema: Schema[protobuf.ByteString] = Schema(
      SchemaType.SString()
    )

    implicit val encodeByteString: Encoder[protobuf.ByteString] =
      Encoder.encodeString.contramap[protobuf.ByteString] { v =>
        Base64.getEncoder.encodeToString(v.toByteArray)
      }
    implicit val decodeByteString: Decoder[protobuf.ByteString] = Decoder.decodeString.emapTry {
      str =>
        Try(protobuf.ByteString.copyFrom(Base64.getDecoder.decode(str)))
    }

    implicit val field: Codec[scalapb.UnknownFieldSet.Field] = deriveCodec
    implicit val unknownFieldSet: Codec[scalapb.UnknownFieldSet] = deriveCodec

    implicit val fieldMask: Codec[FieldMask] = deriveCodec
    implicit val unknownFieldSetSchema: Schema[scalapb.UnknownFieldSet] =
      Schema.string[scalapb.UnknownFieldSet]

    implicit val encodeTimestamp: Encoder[protobuf.timestamp.Timestamp] =
      Encoder.encodeInstant.contramap[protobuf.timestamp.Timestamp] { timestamp =>
        Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong)
      }

    implicit val decodeTimestamp: Decoder[protobuf.timestamp.Timestamp] =
      Decoder.decodeInstant.map(v => protobuf.timestamp.Timestamp(v.getEpochSecond, v.getNano))

    implicit val timestampCodec: sttp.tapir.Codec[String, protobuf.timestamp.Timestamp, TextPlain] =
      sttp.tapir.Codec.instant.mapDecode { (time: Instant) =>
        DecodeResult.Value(protobuf.timestamp.Timestamp(time.getEpochSecond, time.getNano))
      } { timestamp => Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong) }

    implicit val decodeStruct: Decoder[protobuf.struct.Struct] =
      Decoder.decodeJson.map { json =>
        val builder = protobuf.Struct.newBuilder
        JsonFormat.parser.ignoringUnknownFields.merge(json.toString(), builder)
        protobuf.struct.Struct.fromJavaProto(builder.build)
      }

    implicit val encodeStruct: Encoder[protobuf.struct.Struct] =
      Encoder.encodeJson.contramap { struct =>
        val printer = JsonFormat.printer()
        val jsonString = printer.print(Struct.toJavaProto(struct))
        io.circe.parser.parse(jsonString) match {
          case Right(json) => json
          case Left(error) => throw new IllegalStateException(s"Failed to parse JSON: $error")
        }
      }
  }

}
