// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import java.time.{Duration, Instant}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.domain.{ApplicationId, CommandId, Commands, LedgerId, SubmissionId}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.lf.command.{Commands => LfCommands}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.ErrorCause
import com.daml.platform.store.ErrorCause.LedgerTime
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class LedgerTimeAwareCommandExecutorSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

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

  val cid = TransactionBuilder.newCid
  val transaction = TransactionBuilder.justSubmitted(
    TransactionBuilder().fetch(
      TransactionBuilder().create(
        cid,
        Ref.Identifier(
          Ref.PackageId.assertFromString("abc"),
          Ref.QualifiedName.assertFromString("Main:Template"),
        ),
        Value.ValueUnit,
        Set.empty,
        Set.empty,
      )
    )
  )

  private def runExecutionTest(
      dependsOnLedgerTime: Boolean,
      contractStoreResults: List[Try[Option[Instant]]],
      finalExecutionResult: Either[ErrorCause, Time.Timestamp],
  ) = {

    def commandExecutionResult(let: Time.Timestamp) = CommandExecutionResult(
      SubmitterInfo(
        Nil,
        Ref.ApplicationId.assertFromString("foobar"),
        Ref.CommandId.assertFromString("foobar"),
        DeduplicationDuration(Duration.ofMinutes(1)),
        Ref.SubmissionId.assertFromString("foobar"),
        configuration,
      ),
      TransactionMeta(let, None, Time.Timestamp.Epoch, submissionSeed, None, None, None),
      transaction,
      dependsOnLedgerTime,
      5L,
    )

    val mockExecutor = mock[CommandExecutor]
    when(
      mockExecutor.execute(any[Commands], any[Hash], any[Configuration])(
        any[ExecutionContext],
        any[LoggingContext],
      )
    )
      .thenAnswer((c: Commands) =>
        Future.successful(Right(commandExecutionResult(c.commands.ledgerEffectiveTime)))
      )

    val mockContractStore = mock[ContractStore]
    contractStoreResults.tail.foldLeft(
      when(mockContractStore.lookupMaximumLedgerTime(any[Set[ContractId]])(any[LoggingContext]))
        .thenReturn(Future.fromTry(contractStoreResults.head))
    ) { case (mock, result) =>
      mock.andThen(Future.fromTry(result))
    }

    val commands = Commands(
      ledgerId = LedgerId("ledgerId"),
      workflowId = None,
      applicationId = ApplicationId(Ref.ApplicationId.assertFromString("applicationId")),
      commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
      submissionId = SubmissionId(Ref.SubmissionId.assertFromString("submissionId")),
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = Instant.EPOCH,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      commands = LfCommands(
        commands = ImmArray.Empty,
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        commandsReference = "",
      ),
    )

    val instance = new LedgerTimeAwareCommandExecutor(
      mockExecutor,
      mockContractStore,
      3,
      new Metrics(new MetricRegistry),
    )
    LoggingContext.newLoggingContext { implicit context =>
      instance.execute(commands, submissionSeed, configuration).map { actual =>
        val expectedResult = finalExecutionResult.map(let =>
          CommandExecutionResult(
            SubmitterInfo(
              Nil,
              Ref.ApplicationId.assertFromString("foobar"),
              Ref.CommandId.assertFromString("foobar"),
              DeduplicationDuration(Duration.ofMinutes(1)),
              Ref.SubmissionId.assertFromString("foobar"),
              configuration,
            ),
            TransactionMeta(let, None, Time.Timestamp.Epoch, submissionSeed, None, None, None),
            transaction,
            dependsOnLedgerTime,
            5L,
          )
        )

        verify(mockExecutor, times(contractStoreResults.size)).execute(
          any[Commands],
          any[Hash],
          any[Configuration],
        )(any[ExecutionContext], any[LoggingContext])
        verify(mockContractStore, times(contractStoreResults.size))
          .lookupMaximumLedgerTime(Set(cid))

        actual shouldEqual expectedResult
      }
    }
  }

  val missingCid = Failure(MissingContracts(Set(cid)))
  val foundEpoch = Success(Some(Instant.EPOCH))
  val epochPlus5: Time.Timestamp = Time.Timestamp.Epoch.add(Duration.ofSeconds(5))
  val foundEpochPlus5 = Success(Some(epochPlus5.toInstant))
  val noLetFound = Success(None)

  "LedgerTimeAwareCommandExecutor" when {
    "the model doesn't use getTime" should {
      "not retry if ledger effective time is found in the contract store" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          contractStoreResults = List(foundEpoch),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "not retry if the contract does not have ledger effective time" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          contractStoreResults = List(noLetFound),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "retry if the contract cannot be found in the contract store and fail at max retries" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          contractStoreResults = List(missingCid, missingCid, missingCid, missingCid),
          finalExecutionResult = Left(LedgerTime(3)),
        )
      }

      "succeed if the contract can be found on a retry" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          contractStoreResults = List(missingCid, missingCid, missingCid, foundEpoch),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "advance the output time if the contract's LET is in the future" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          contractStoreResults = List(foundEpochPlus5),
          finalExecutionResult = Right(epochPlus5),
        )
      }
    }

    "the model uses getTime" should {
      "retry if the contract's LET is in the future" in {
        runExecutionTest(
          dependsOnLedgerTime = true,
          contractStoreResults = List(
            // the first lookup of +5s will cause the interpretation to be restarted,
            // in case the usage of getTime with a different LET would result in a different transaction
            foundEpochPlus5,
            // The second lookup finds the same ledger time again
            foundEpochPlus5,
          ),
          finalExecutionResult = Right(epochPlus5),
        )
      }

      "retry if the contract's LET is in the future and then retry if the contract is missing" in {
        runExecutionTest(
          dependsOnLedgerTime = true,
          contractStoreResults = List(
            // the first lookup of +5s will cause the interpretation to be restarted,
            // in case the usage of getTime with a different LET would result in a different transaction
            foundEpochPlus5,
            // during the second interpretation the contract was actually archived
            // and could not be found during the maximum ledger time lookup.
            // this causes yet another restart of the interpretation.
            missingCid,
            // The third lookup finds the same ledger time again
            foundEpochPlus5,
          ),
          finalExecutionResult = Right(epochPlus5),
        )
      }
    }

  }
}
