// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.*
import com.daml.error.ErrorCategory.GenericErrorCategory
import com.daml.error.utils.DecodedCantonError
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.trace_context.TraceContext
import com.google.protobuf
import com.google.protobuf.field_mask.FieldMask
import com.google.protobuf.struct.Struct
import com.google.protobuf.util.JsonFormat
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder, Json}
import io.grpc.Status
import org.slf4j.event.Level
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{DecodeResult, Schema, SchemaType}

import java.time.Instant
import java.util.Base64
import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.util.Try

/** JSON wrappers that do not belong to a particular service */
object JsSchema {

  final case class JsTransaction(
      update_id: String,
      command_id: String,
      workflow_id: String,
      effective_at: com.google.protobuf.timestamp.Timestamp,
      events: Seq[JsEvent.Event],
      offset: String,
      domain_id: String,
      trace_context: Option[TraceContext],
      record_time: com.google.protobuf.timestamp.Timestamp,
  )

  final case class JsTransactionTree(
      update_id: String,
      command_id: String,
      workflow_id: String,
      effective_at: Option[protobuf.timestamp.Timestamp],
      offset: String,
      events_by_id: Map[String, JsTreeEvent.TreeEvent],
      root_event_ids: Seq[String],
      domain_id: String,
      trace_context: Option[TraceContext],
      record_time: protobuf.timestamp.Timestamp,
  )

  final case class JsStatus(
      code: Int,
      message: String,
      details: Seq[com.google.protobuf.any.Any],
  )

  final case class JsInterfaceView(
      interface_id: String,
      view_status: JsStatus,
      view_value: Option[Json],
  )

  object JsEvent {
    sealed trait Event

    final case class CreatedEvent(
        event_id: String,
        contract_id: String,
        template_id: String,
        contract_key: Option[Json],
        create_argument: Option[Json],
        created_event_blob: protobuf.ByteString,
        interface_views: Seq[JsInterfaceView],
        witness_parties: Seq[String],
        signatories: Seq[String],
        observers: Seq[String],
        created_at: protobuf.timestamp.Timestamp,
        package_name: String,
    ) extends Event

    final case class ArchivedEvent(
        event_id: String,
        contract_id: String,
        template_id: String,
        witness_parties: Seq[String],
        package_name: String,
    ) extends Event
  }

  object JsTreeEvent {
    sealed trait TreeEvent

    final case class ExercisedTreeEvent(
        event_id: String,
        contract_id: String,
        template_id: String,
        interface_id: String,
        choice: String,
        choice_argument: Json,
        acting_parties: Seq[String],
        consuming: Boolean,
        witness_parties: Seq[String],
        child_event_ids: Seq[String],
        exercise_result: Json,
        package_name: String,
    ) extends TreeEvent

    final case class CreatedTreeEvent(
        event_id: String,
        contract_id: String,
        template_id: String,
        contract_key: Option[Json],
        create_arguments: Option[Json],
        created_event_blob: protobuf.ByteString,
        interface_views: Seq[JsInterfaceView],
        witness_parties: Seq[String],
        signatories: Seq[String],
        observers: Seq[String],
        createdAt: Option[com.google.protobuf.timestamp.Timestamp],
        packageName: String,
    ) extends TreeEvent

  }

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

  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
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
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  object DirectScalaPbRwImplicits {

    implicit val om: Codec[ObjectMeta] = deriveCodec
    implicit val traceContext: Codec[TraceContext] = deriveCodec
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
      }(timestamp => Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong))

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

    implicit val encodeIdentifier: Encoder[com.daml.ledger.api.v2.value.Identifier] =
      Encoder.encodeString.contramap { identifier =>
        IdentifierConverter.toJson(identifier)
      }

    implicit val decodeIdentifier: Decoder[com.daml.ledger.api.v2.value.Identifier] =
      Decoder.decodeString.map(IdentifierConverter.fromJson)

    implicit val jsEvent: Codec[JsEvent.Event] = deriveCodec
    implicit val any: Codec[com.google.protobuf.any.Any] = deriveCodec
    implicit val jsStatus: Codec[JsStatus] = deriveCodec
    implicit val jsInterfaceView: Codec[JsInterfaceView] = deriveCodec
    implicit val jsCreatedEvent: Codec[JsEvent.CreatedEvent] = deriveCodec
    implicit val jsArchivedEvent: Codec[JsEvent.ArchivedEvent] = deriveCodec
    implicit val jsTransactionTree: Codec[JsTransactionTree] = deriveCodec
    implicit val jsSubmitAndWaitForTransactionTreeResponse
        : Codec[JsSubmitAndWaitForTransactionTreeResponse] = deriveCodec
    implicit val jsTreeEvent: Codec[JsTreeEvent.TreeEvent] = deriveCodec
    implicit val jsExercisedTreeEvent: Codec[JsTreeEvent.ExercisedTreeEvent] = deriveCodec
    implicit val jsCreatedTreeEvent: Codec[JsTreeEvent.CreatedTreeEvent] = deriveCodec
  }
}
