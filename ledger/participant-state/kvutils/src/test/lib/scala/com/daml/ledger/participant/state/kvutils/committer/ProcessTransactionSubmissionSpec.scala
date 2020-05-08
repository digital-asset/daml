// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.committing.Common.Commit
import com.daml.ledger.participant.state.kvutils.committing.Common.Commit.sequence2
import com.daml.ledger.participant.state.kvutils.committing.{Common, ProcessTransactionSubmission}
import com.daml.lf.data.InsertOrdMap
import org.scalatest.{Inside, Matchers, WordSpec}
import org.slf4j.{Logger, LoggerFactory}

class ProcessTransactionSubmissionSpec extends WordSpec with Matchers with Inside {
  "ProcessTransactionSubmission" should {
    "exhaust the stack when read chains are very long" in {
      def readChain(n: Int): Commit[Unit] = {
        val oneRead =
          ProcessTransactionSubmission.getContractState(DamlStateKey.getDefaultInstance).map { _ =>
            ()
          }
        (1 until n)
          .foldLeft(oneRead) { (chain, _) =>
            chain.flatMap { _ =>
              oneRead
            }
          }
      }

      implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)

      val inputState = Map[DamlStateKey, Option[DamlStateValue]](
        DamlStateKey.getDefaultInstance -> Some(DamlStateValue.getDefaultInstance))

      an[StackOverflowError] should be thrownBy sequence2(readChain(3639))
        .run(Common.CommitContext(inputState, InsertOrdMap.empty))
    }
  }
}
