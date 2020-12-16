// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.Commands
import com.daml.ledger.participant.state.index.v2.{ContractStore, IndexPackagesService}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, ResultDone}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{SubmittedTransaction, Transaction}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class StoreBackedCommandExecutorSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val emptyTransactionMetadata = Transaction.Metadata(
    submissionSeed = None,
    submissionTime = Time.Timestamp.now(),
    usedPackages = Set.empty,
    dependsOnTime = false,
    nodeSeeds = ImmArray.empty,
  )

  "execute" should {
    "add interpretation time to result" in {
      val mockEngine = mock[Engine]
      when(
        mockEngine.submit(
          any[Set[Ref.Party]],
          any[com.daml.lf.command.Commands],
          any[ParticipantId],
          any[Hash]))
        .thenReturn(
          ResultDone[(SubmittedTransaction, Transaction.Metadata)](
            (TransactionBuilder.EmptySubmitted, emptyTransactionMetadata)
          )
        )
      val instance = new StoreBackedCommandExecutor(
        mockEngine,
        Ref.ParticipantId.assertFromString("anId"),
        mock[IndexPackagesService],
        mock[ContractStore],
        new Metrics(new MetricRegistry))
      val mockDomainCommands = mock[Commands]
      val mockLfCommands = mock[com.daml.lf.command.Commands]
      when(mockLfCommands.ledgerEffectiveTime).thenReturn(Time.Timestamp.now())
      when(mockDomainCommands.workflowId).thenReturn(None)
      when(mockDomainCommands.commands).thenReturn(mockLfCommands)
      when(mockDomainCommands.actAs).thenReturn(Set.empty[Ref.Party])
      when(mockDomainCommands.readAs).thenReturn(Set.empty[Ref.Party])

      LoggingContext.newLoggingContext { implicit context =>
        instance.execute(mockDomainCommands, Hash.hashPrivateKey("a key")).map { actual =>
          actual.right.foreach { actualResult =>
            actualResult.interpretationTimeNanos should be > 0L
          }
          succeed
        }
      }
    }
  }
}
