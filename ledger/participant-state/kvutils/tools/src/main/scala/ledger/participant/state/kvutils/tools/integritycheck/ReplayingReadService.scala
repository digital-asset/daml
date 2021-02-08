// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.`export`.WriteSet
import com.daml.ledger.participant.state.v1.ReadService

/** A ReadService that streams back previously recorded state updates */
trait ReplayingReadService extends ReadService {
  def updateCount(): Long
}

/** Records state updates and creates corresponding ReplayingReadService instances */
trait ReplayingReadServiceFactory {
  def appendBlock(writeSet: WriteSet): Unit

  def createReadService(implicit materializer: Materializer): ReplayingReadService
}
