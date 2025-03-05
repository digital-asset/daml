// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.*
import com.daml.error.ErrorCategory.GenericErrorCategory
import com.daml.error.utils.DecodedCantonError
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.trace_context.TraceContext
import com.daml.ledger.api.v2.{offset_checkpoint, reassignment, transaction_filter}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsEvent.CreatedEvent
import com.google.protobuf
import com.google.protobuf.field_mask.FieldMask
import com.google.protobuf.struct.Struct
import com.google.protobuf.util.JsonFormat
import io.circe.generic.extras.Configuration
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder, Json}
import io.grpc.Status
import org.slf4j.event.Level
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.generic.auto.*
import sttp.tapir.{DecodeResult, Schema, SchemaType}

import java.time.Instant
import java.util.Base64
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

/** JSON wrappers that do not belong to a particular service */
object JsSchema {

  implicit val config: Configuration = Configuration.default.copy(
    useDefaults = true
  )
  final case class JsTransaction(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: com.google.protobuf.timestamp.Timestamp,
      events: Seq[JsEvent.Event],
      offset: Long,
      synchronizerId: String,
      traceContext: Option[TraceContext],
      recordTime: com.google.protobuf.timestamp.Timestamp,
  )

  final case class JsTransactionTree(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Option[protobuf.timestamp.Timestamp],
      offset: Long,
      eventsById: Map[Int, JsTreeEvent.TreeEvent],
      synchronizerId: String,
      traceContext: Option[TraceContext],
      recordTime: protobuf.timestamp.Timestamp,
  )

  final case class JsTopologyTransaction(
      updateId: String,
      events: Seq[JsTopologyEvent.Event],
      offset: Long,
      synchronizerId: String,
      traceContext: Option[TraceContext],
      recordTime: com.google.protobuf.timestamp.Timestamp,
  )

  final case class JsStatus(
      code: Int,
      message: String,
      details: Seq[com.google.protobuf.any.Any],
  )

  final case class JsInterfaceView(
      interfaceId: String,
      viewStatus: JsStatus,
      viewValue: Option[Json],
  )

  object JsServicesCommonCodecs {
    implicit val jsTransactionRW: Codec[JsTransaction] = deriveCodec

    implicit val unassignedEventRW: Codec[reassignment.UnassignedEvent] = deriveCodec

    implicit val identifierFilterSchema
        : Schema[transaction_filter.CumulativeFilter.IdentifierFilter] =
      Schema.oneOfWrapped
    implicit val filtersRW: Codec[transaction_filter.Filters] = deriveCodec
    implicit val cumulativeFilterRW: Codec[transaction_filter.CumulativeFilter] = deriveCodec
    implicit val identifierFilterRW: Codec[transaction_filter.CumulativeFilter.IdentifierFilter] =
      deriveCodec
    implicit val wildcardFilterRW: Codec[transaction_filter.WildcardFilter] =
      deriveCodec
    implicit val templateFilterRW: Codec[transaction_filter.TemplateFilter] =
      deriveCodec
    implicit val interfaceFilterRW: Codec[transaction_filter.InterfaceFilter] =
      deriveCodec
    implicit val transactionFilterRW: Codec[transaction_filter.TransactionFilter] = deriveCodec
    implicit val eventFormatRW: Codec[transaction_filter.EventFormat] = deriveCodec
    implicit val transactionShapeRW: Codec[transaction_filter.TransactionShape] = deriveCodec
    implicit val transactionFormatRW: Codec[transaction_filter.TransactionFormat] = deriveCodec

  }

  object JsEvent {
    sealed trait Event

    final case class CreatedEvent(
        offset: Long,
        nodeId: Int,
        contractId: String,
        templateId: String,
        contractKey: Option[Json],
        createArgument: Option[Json],
        createdEventBlob: protobuf.ByteString,
        interfaceViews: Seq[JsInterfaceView],
        witnessParties: Seq[String],
        signatories: Seq[String],
        observers: Seq[String],
        createdAt: protobuf.timestamp.Timestamp,
        packageName: String,
    ) extends Event

    final case class ArchivedEvent(
        offset: Long,
        nodeId: Int,
        contractId: String,
        templateId: String,
        witnessParties: Seq[String],
        packageName: String,
        implementedInterfaces: Seq[String],
    ) extends Event

    final case class ExercisedEvent(
        offset: Long,
        nodeId: Int,
        contractId: String,
        templateId: com.daml.ledger.api.v2.value.Identifier,
        interfaceId: Option[com.daml.ledger.api.v2.value.Identifier],
        choice: String,
        choiceArgument: Json,
        actingParties: Seq[String],
        consuming: Boolean,
        witnessParties: Seq[String],
        lastDescendantNodeId: Int,
        exerciseResult: Json,
        packageName: String,
        implementedInterfaces: Seq[com.daml.ledger.api.v2.value.Identifier],
    ) extends Event
  }

  object JsTopologyEvent {
    sealed trait Event

    final case class ParticipantAuthorizationChanged(
        partyId: String,
        participantId: String,
        participantPermission: Int,
    ) extends Event

    final case class ParticipantAuthorizationRevoked(
        partyId: String,
        participantId: String,
    ) extends Event
  }

  object JsTreeEvent {
    sealed trait TreeEvent

    final case class ExercisedTreeEvent(
        offset: Long,
        nodeId: Int,
        contractId: String,
        templateId: String,
        interfaceId: Option[String],
        choice: String,
        choiceArgument: Json,
        actingParties: Seq[String],
        consuming: Boolean,
        witnessParties: Seq[String],
        exerciseResult: Json,
        packageName: String,
        lastDescendantNodeId: Int,
    ) extends TreeEvent

    final case class CreatedTreeEvent(value: CreatedEvent) extends TreeEvent

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
      retryInfo: Option[Duration],
      definiteAnswer: Option[Boolean],
  )

  object JsCantonError {
    import DirectScalaPbRwImplicits.*
    implicit val rw: Codec[JsCantonError] = deriveCodec

    def fromErrorCode(damlError: DamlError): JsCantonError = JsCantonError(
      code = damlError.code.id,
      cause = damlError.cause,
      correlationId = damlError.errorContext.correlationId,
      traceId = damlError.errorContext.traceId,
      context = damlError.context,
      resources = damlError.resources.map { case (k, v) => (k.asString, v) },
      errorCategory = damlError.code.category.asInt,
      grpcCodeValue = damlError.code.category.grpcCode.map(_.value()),
      retryInfo = damlError.code.category.retryable.map(_.duration),
      definiteAnswer = damlError match {
        case errorWithDefiniteAnswer: DamlErrorWithDefiniteAnswer =>
          Some(errorWithDefiniteAnswer.definiteAnswer)
        case _ => None
      },
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
        retryInfo = decodedCantonError.code.category.retryable.map(_.duration),
        definiteAnswer = decodedCantonError.definiteAnswerO,
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
            retryable = jsCantonError.retryInfo.map(duration =>
              ErrorCategoryRetry(FiniteDuration(duration.length, duration.unit))
            ),
            redactDetails = false,
            asInt = jsCantonError.errorCategory,
            rank = 1,
          ),
        ),
        cause = jsCantonError.cause,
        correlationId = jsCantonError.correlationId,
        traceId = jsCantonError.traceId,
        context = jsCantonError.context,
        resources = jsCantonError.resources.map { case (k, v) => (ErrorResource(k), v) },
        definiteAnswerO = jsCantonError.definiteAnswer,
      )
  }
  object DirectScalaPbRwImplicits {
    import sttp.tapir.generic.auto.*
    import sttp.tapir.json.circe.*

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
    implicit val jsCreatedEvent: Codec[JsEvent.CreatedEvent] = deriveCodec
    implicit val jsArchivedEvent: Codec[JsEvent.ArchivedEvent] = deriveCodec
    implicit val jsExercisedEvent: Codec[JsEvent.ExercisedEvent] = deriveCodec

    implicit val any: Codec[com.google.protobuf.any.Any] = deriveCodec
    implicit val jsStatus: Codec[JsStatus] = deriveCodec
    implicit val jsInterfaceView: Codec[JsInterfaceView] = deriveCodec

    implicit val jsTransactionTree: Codec[JsTransactionTree] = deriveCodec
    implicit val jsTopologyTransaction: Codec[JsTopologyTransaction] = deriveCodec
    implicit val jsSubmitAndWaitForTransactionTreeResponse
        : Codec[JsSubmitAndWaitForTransactionTreeResponse] = deriveCodec
    implicit val jsTreeEvent: Codec[JsTreeEvent.TreeEvent] = deriveCodec
    implicit val jsExercisedTreeEvent: Codec[JsTreeEvent.ExercisedTreeEvent] = deriveCodec
    implicit val jsCreatedTreeEvent: Codec[JsTreeEvent.CreatedTreeEvent] = deriveCodec
    implicit val jsTopologyEvent: Codec[JsTopologyEvent.Event] = deriveCodec
    implicit val jsParticipantAuthorizationChanged
        : Codec[JsTopologyEvent.ParticipantAuthorizationChanged] = deriveCodec
    implicit val jsParticipantAuthorizationRevoked
        : Codec[JsTopologyEvent.ParticipantAuthorizationRevoked] = deriveCodec

    implicit val offsetCheckpoint: Codec[offset_checkpoint.OffsetCheckpoint] = deriveCodec
    implicit val offsetCheckpointSynchronizerTime: Codec[offset_checkpoint.SynchronizerTime] =
      deriveCodec

    implicit val grpcStatusRW: Codec[
      com.google.rpc.status.Status
    ] = deriveCodec

    // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
    implicit val identifierSchema: Schema[com.daml.ledger.api.v2.value.Identifier] = Schema.string

    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    implicit val jsEventSchema: Schema[JsEvent.Event] =
      Schema.oneOfWrapped

    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    implicit val jsTreeEventSchema: Schema[JsTreeEvent.TreeEvent] =
      Schema.oneOfWrapped

    implicit val valueSchema: Schema[com.google.protobuf.struct.Value] = Schema.any
  }
}
