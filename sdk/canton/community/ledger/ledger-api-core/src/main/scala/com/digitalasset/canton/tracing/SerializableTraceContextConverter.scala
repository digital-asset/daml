// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.ledger.api.v2.trace_context.TraceContext as DamlTraceContext
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.typesafe.scalalogging.Logger

object SerializableTraceContextConverter {
  implicit class SerializableTraceContextExtension(
      val serializableTraceContext: SerializableTraceContext
  ) extends AnyVal {
    def toDamlProto: DamlTraceContext = {
      val w3cTraceContext = serializableTraceContext.traceContext.asW3CTraceContext
      DamlTraceContext(w3cTraceContext.map(_.parent), w3cTraceContext.flatMap(_.state))
    }

    def toDamlProtoOpt: Option[DamlTraceContext] =
      Option.when(serializableTraceContext.traceContext != TraceContext.empty)(toDamlProto)
  }

  def fromDamlProtoSafeOpt(logger: Logger)(
      traceContextP: Option[DamlTraceContext]
  ): SerializableTraceContext =
    SerializableTraceContext.safely(logger)(fromDamlProtoOpt)(traceContextP)

  def fromDamlProtoOpt(
      traceContextP: Option[DamlTraceContext]
  ): ParsingResult[SerializableTraceContext] =
    for {
      tcP <- ProtoConverter.required("traceContext", traceContextP)
      tc <- fromDamlProto(tcP)
    } yield tc

  def fromDamlProto(tc: DamlTraceContext): ParsingResult[SerializableTraceContext] =
    Right(SerializableTraceContext(W3CTraceContext.toTraceContext(tc.traceparent, tc.tracestate)))
}
