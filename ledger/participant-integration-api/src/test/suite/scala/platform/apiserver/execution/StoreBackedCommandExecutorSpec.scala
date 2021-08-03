// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import java.time.{Duration, Instant}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.{ApplicationId, CommandId, Commands, LedgerId, SubmissionId}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
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
import com.daml.lf.command.{Commands => LfCommands}

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
          any[Set[Ref.Party]],
          any[com.daml.lf.command.Commands],
          any[ParticipantId],
          any[Hash],
        )
      )
        .thenReturn(
          ResultDone[(SubmittedTransaction, Transaction.Metadata)](
            (TransactionBuilder.EmptySubmitted, emptyTransactionMetadata)
          )
        )

      val commands = Commands(
        ledgerId = LedgerId("ledgerId"),
        workflowId = None,
        applicationId = ApplicationId(Ref.ApplicationId.assertFromString("applicationId")),
        commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
        submissionId = SubmissionId(Ref.SubmissionId.assertFromString("submissionId")),
        actAs = Set.empty,
        readAs = Set.empty,
        submittedAt = Instant.EPOCH,
        deduplicationDuration = Duration.ZERO,
        commands = LfCommands(
          commands = ImmArray.empty,
          ledgerEffectiveTime = Time.Timestamp.Epoch,
          commandsReference = "",
        ),
      )
      val submissionSeed = Hash.hashPrivateKey("a key")
      val configuration = Configuration(
        generation = 1,
        timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ZERO,
          minSkew = Duration.ZERO,
          maxSkew = Duration.ZERO,
        ).get,
        maxDeduplicationTime = Duration.ZERO,
      )

      val instance = new StoreBackedCommandExecutor(
        mockEngine,
        Ref.ParticipantId.assertFromString("anId"),
        mock[IndexPackagesService],
        mock[ContractStore],
        new Metrics(new MetricRegistry),
      )

      LoggingContext.newLoggingContext { implicit context =>
        instance.execute(commands, submissionSeed, configuration).map { actual =>
          actual.foreach { actualResult =>
            actualResult.interpretationTimeNanos should be > 0L
          }
          succeed
        }
      }
    }
  }
}
