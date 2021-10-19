// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.export.{SubmissionInfo, WriteSet}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait CommitStrategySupport[LogResult] {
  def stateKeySerializationStrategy: StateKeySerializationStrategy

  def commit(
      submissionInfo: SubmissionInfo
  )(implicit materializer: Materializer, loggingContext: LoggingContext): Future[WriteSet]

  def newReadServiceFactory(): ReplayingReadServiceFactory

  def writeSetComparison: WriteSetComparison
}
