// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.v30
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionCommon,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.typesafe.scalalogging.Logger

/** Wrapper around [[TraceContext]] to keep serialization out of the [[TraceContext]] itself
  * and thereby reduce its dependencies.
  */
final case class SerializableTraceContext(traceContext: TraceContext)
    extends HasVersionedWrapper[SerializableTraceContext] {

  def unwrap: TraceContext = traceContext

  override protected def companionObj
      : HasVersionedMessageCompanionCommon[SerializableTraceContext] = SerializableTraceContext

  def toProtoV30: v30.TraceContext = {
    val w3cTraceContext = traceContext.asW3CTraceContext
    v30.TraceContext(w3cTraceContext.map(_.parent), w3cTraceContext.flatMap(_.state))
  }

  def toDamlProto: DamlTraceContext = {
    val w3cTraceContext = traceContext.asW3CTraceContext
    DamlTraceContext(w3cTraceContext.map(_.parent), w3cTraceContext.flatMap(_.state))
  }

  def toDamlProtoOpt: Option[DamlTraceContext] =
    Option.when(traceContext != TraceContext.empty)(toDamlProto)
}

object SerializableTraceContext
    extends HasVersionedMessageCompanion[SerializableTraceContext]
    with HasVersionedMessageCompanionDbHelpers[SerializableTraceContext] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.TraceContext)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  /** The name of the class as used for pretty-printing */
  override def name: String = "TraceContext"

  val empty: SerializableTraceContext = SerializableTraceContext(TraceContext.empty)

  /** Construct a TraceContext from provided protobuf structure.
    * Errors will be logged at a WARN level using the provided storageLogger and an empty TraceContext will be returned.
    */
  def fromProtoSafeV30Opt(logger: Logger)(
      traceContextP: Option[v30.TraceContext]
  ): SerializableTraceContext =
    safely(logger)(fromProtoV30Opt)(traceContextP)

  def fromProtoV30Opt(
      traceContextP: Option[v30.TraceContext]
  ): ParsingResult[SerializableTraceContext] =
    for {
      tcP <- ProtoConverter.required("traceContext", traceContextP)
      tc <- fromProtoV30(tcP)
    } yield tc

  def fromProtoV30(tc: v30.TraceContext): ParsingResult[SerializableTraceContext] =
    Right(SerializableTraceContext(W3CTraceContext.toTraceContext(tc.traceparent, tc.tracestate)))

  def fromDamlProtoSafeOpt(logger: Logger)(
      traceContextP: Option[DamlTraceContext]
  ): SerializableTraceContext =
    safely(logger)(fromDamlProtoOpt)(traceContextP)

  def fromDamlProtoOpt(
      traceContextP: Option[DamlTraceContext]
  ): ParsingResult[SerializableTraceContext] =
    for {
      tcP <- ProtoConverter.required("traceContext", traceContextP)
      tc <- fromDamlProto(tcP)
    } yield tc

  def fromDamlProto(tc: DamlTraceContext): ParsingResult[SerializableTraceContext] =
    Right(SerializableTraceContext(W3CTraceContext.toTraceContext(tc.traceparent, tc.tracestate)))

  private def safely[A](
      logger: Logger
  )(fn: A => ParsingResult[SerializableTraceContext])(a: A): SerializableTraceContext =
    fn(a) match {
      case Left(err) =>
        logger.warn(s"Failed to deserialize provided trace context: $err")
        SerializableTraceContext(TraceContext.empty)
      case Right(traceContext) => traceContext
    }
}
