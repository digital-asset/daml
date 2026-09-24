// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{Attributes, ServerTransportFilter}

final case class GrpcConnectionLogger(loggerFactory: NamedLoggerFactory)
    extends ServerTransportFilter
    with NamedLogging {
  override def transportReady(transportAttrs: Attributes): Attributes = {
    val attributes = if (transportAttrs == null) "<no-attributes>" else transportAttrs.toString
    logger.debug(s"Grpc connection open: $attributes")(TraceContext.empty)
    super.transportReady(transportAttrs)
  }

  override def transportTerminated(transportAttrs: Attributes): Unit = {
    val attributes = if (transportAttrs == null) "<no-attributes>" else transportAttrs.toString
    logger.debug(s"Grpc connection closed: $attributes")(TraceContext.empty)
    super.transportTerminated(transportAttrs)
  }
}
