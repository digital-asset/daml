// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.{DamlKvutils, DamlStateMap}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time
import org.scalatest.{Inside, Matchers, WordSpec}
import org.slf4j.{Logger, LoggerFactory}

class TransactionCommitterSpec extends WordSpec with Matchers with Inside {
  "TransactionCommitter" should {
    "not exhaust the stack when performing lots of reads" in {
      implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)

      val inputState = Map[DamlStateKey, Option[DamlStateValue]](
        DamlStateKey.getDefaultInstance -> Some(DamlStateValue.getDefaultInstance))

      for (_ <- 1 to 100000)
        TransactionCommitter.getContractState(
          new CommitContext {
            override def inputs: DamlStateMap = inputState

            override def getEntryId: DamlKvutils.DamlLogEntryId = ???

            override def getMaximumRecordTime: Time.Timestamp = ???

            override def getRecordTime: Time.Timestamp = ???

            override def getParticipantId: ParticipantId = ???
          },
          DamlStateKey.getDefaultInstance
        )

      succeed
    }
  }
}
