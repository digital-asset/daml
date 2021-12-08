// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.deduplication

import java.time.{Duration, Instant}

import akka.stream.Materializer
import com.daml.ledger.api.domain.ApplicationId
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

trait DeduplicationPeriodConverter {

  def convertOffsetToDuration(
      offset: Ref.HexString,
      applicationId: ApplicationId,
      actAs: Set[Ref.Party],
      submittedAt: Instant,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DeduplicationConversionFailure, Duration]]

}
