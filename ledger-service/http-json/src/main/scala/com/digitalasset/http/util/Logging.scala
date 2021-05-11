// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.logging.LoggingContextOf
import com.daml.logging.LoggingContextOf.{label, newLoggingContext, withEnrichedLoggingContext}

import java.util.UUID

object Logging {

  /**  The [[CorrelationID]] is just a tag for [[LoggingContextOf]]
    *  to signal, that we request to include an UUID/ID
    *  so we can correlate related logs within a specific application run.
    *  A preferred instance of a [[LoggingContextOf]] containing a correlation id
    *  can be created via [[correlationIdLogCtx]].
    *  TL:DR, Tag to request an Unique ID for an application run within logs.
    */
  sealed abstract class CorrelationID

  /**   The [[RequestID]] is just a tag for [[LoggingContextOf]]
    *   to signal, that we request to include an UUID/ID
    *   so we can correlate logs of a specific http request (for now).
    *   A preferred [[LoggingContextOf]] can be created via [[extendWithRequestIdLogCtx]]
    *   which is based on a prior call to [[correlationIdLogCtx]].
    *   TL:DR, Tag to request an Unique ID for a http request within logs.
    */
  sealed abstract class RequestID

  def correlationIdLogCtx[Z](fn: LoggingContextOf[CorrelationID] => Z): Z =
    newLoggingContext(label[CorrelationID], Map("correlation_id" -> UUID.randomUUID().toString))(fn)

  def correlationIdLogCtx(): LoggingContextOf[CorrelationID] = correlationIdLogCtx(identity(_))

  def extendWithRequestIdLogCtx[Z](
      fn: LoggingContextOf[CorrelationID with RequestID] => Z
  )(implicit lc: LoggingContextOf[CorrelationID]): Z =
    withEnrichedLoggingContext(label[RequestID], Map("request_id" -> UUID.randomUUID().toString))
      .run(fn)

}
