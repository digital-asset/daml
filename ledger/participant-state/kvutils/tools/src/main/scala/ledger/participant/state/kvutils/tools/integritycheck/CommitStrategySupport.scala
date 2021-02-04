// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.export.{SubmissionInfo, WriteSet}
import com.daml.ledger.participant.state.v1.ReadService
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.concurrent.Future

/** A ReadService that streams back previously recorded state updates */
trait ReplayingReadService extends ReadService {
  def updateCount(): Long
}

/** Records state updates and creates corresponding ReplayingReadService instances */
trait ReplayingReadServiceFactory {
  def appendBlock(writeSet: WriteSet): Unit

  def createReadService(implicit materializer: Materializer): ReplayingReadService
}

trait CommitStrategySupport[LogResult] {
  def stateKeySerializationStrategy: StateKeySerializationStrategy

  def commit(
      submissionInfo: SubmissionInfo
  )(implicit materializer: Materializer): Future[WriteSet]

  def newReadServiceFactory(): ReplayingReadServiceFactory
}
