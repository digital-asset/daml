// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import ConflictCheckingLedgerBridge._
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.metrics.Timed
import com.daml.platform.ApiOffset

import scala.concurrent.{ExecutionContext, Future}

/** Tags the prepared submission with the current ledger end as available on the Ledger API. */
private[validate] class TagWithLedgerEndImpl(
    indexService: IndexService,
    bridgeMetrics: BridgeMetrics,
)(implicit executionContext: ExecutionContext)
    extends TagWithLedgerEnd {
  override def apply(
      preparedSubmission: Validation[PreparedSubmission]
  ): AsyncValidation[(Offset, PreparedSubmission)] = preparedSubmission match {
    case Left(rejection) => Future.successful(Left(rejection))
    case Right(preparedSubmission) =>
      Timed.future(
        bridgeMetrics.Stages.TagWithLedgerEnd.timer,
        indexService
          .currentLedgerEnd()(preparedSubmission.submission.loggingContext)
          .map(ledgerEnd =>
            Right(ApiOffset.assertFromString(ledgerEnd.value) -> preparedSubmission)
          ),
      )
  }
}
