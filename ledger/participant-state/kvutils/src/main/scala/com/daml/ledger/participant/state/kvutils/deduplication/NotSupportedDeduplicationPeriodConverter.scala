// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import com.daml.ledger.api.domain.ApplicationId
import com.daml.lf.data.Ref.{LedgerString, Party}
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

object NotSupportedDeduplicationPeriodConverter extends DeduplicationPeriodConverter {

  override def convertOffsetToDuration(
      offset: LedgerString,
      applicationId: ApplicationId,
      actAs: Set[Party],
      submittedAt: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DeduplicationConversionFailure, Duration]] = throw new IllegalArgumentException(
    "Offsets are not supported as deduplication periods"
  )

}
