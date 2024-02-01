// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.command.ApiCommands as LfCommands
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.ledger.api.domain.{CommandId, Commands, LedgerId}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.MaximumLedgerTime
import com.digitalasset.canton.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.LedgerTime
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.Future

class LedgerTimeAwareCommandExecutorSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {

  private val loggingContext =
    LoggingContextWithTrace.ForTesting

  private val submissionSeed = Hash.hashPrivateKey("a key")
  private val configuration = Configuration(
    generation = 1,
    timeModel = LedgerTimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = Duration.ZERO,
      maxSkew = Duration.ZERO,
    ).get,
    maxDeduplicationDuration = Duration.ZERO,
  )

  private val cid = TransactionBuilder.newCid

  private val transaction = TransactionBuilder.justSubmitted(
    TestNodeBuilder.fetch(
      TestNodeBuilder.create(
        id = cid,
        templateId = Ref.Identifier(
          Ref.PackageId.assertFromString("abc"),
          Ref.QualifiedName.assertFromString("Main:Template"),
        ),
        argument = Value.ValueUnit,
        signatories = Set.empty,
        observers = Set.empty,
      ),
      byKey = false,
    )
  )

  private val processedDisclosedContracts = ImmArray(
    ProcessedDisclosedContract(
      templateId = Identifier.assertFromString("some:pkg:identifier"),
      packageName = None,
      contractId = cid,
      argument = Value.ValueNil,
      createdAt = Time.Timestamp.Epoch,
      driverMetadata = Bytes.Empty,
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      agreementText = "",
      version = TransactionVersion.StableVersions.max,
    )
  )

  private def runExecutionTest(
      dependsOnLedgerTime: Boolean,
      resolveMaximumLedgerTimeResults: List[MaximumLedgerTime],
      finalExecutionResult: Either[ErrorCause, Time.Timestamp],
  ): Future[Assertion] = {

    def commandExecutionResult(let: Time.Timestamp) = CommandExecutionResult(
      SubmitterInfo(
        Nil,
        Nil,
        Ref.ApplicationId.assertFromString("foobar"),
        Ref.CommandId.assertFromString("foobar"),
        DeduplicationDuration(Duration.ofMinutes(1)),
        None,
        configuration,
      ),
      TransactionMeta(let, None, Time.Timestamp.Epoch, submissionSeed, None, None, None, None),
      transaction,
      dependsOnLedgerTime,
      5L,
      Map.empty,
      processedDisclosedContracts,
    )

    val mockExecutor = mock[CommandExecutor]
    when(
      mockExecutor.execute(any[Commands], any[Hash], any[Configuration])(
        any[LoggingContextWithTrace]
      )
    )
      .thenAnswer((c: Commands) =>
        Future.successful(Right(commandExecutionResult(c.commands.ledgerEffectiveTime)))
      )

    val mockResolveMaximumLedgerTime = mock[ResolveMaximumLedgerTime]
    resolveMaximumLedgerTimeResults.tail.foldLeft(
      when(
        mockResolveMaximumLedgerTime(
          eqTo(processedDisclosedContracts),
          any[Set[ContractId]],
        )(
          any[LoggingContextWithTrace]
        )
      )
        .thenReturn(Future.successful(resolveMaximumLedgerTimeResults.head))
    ) { case (mock, result) =>
      mock.andThen(Future.successful(result))
    }

    val commands = Commands(
      ledgerId = Some(LedgerId("ledgerId")),
      workflowId = None,
      applicationId = Ref.ApplicationId.assertFromString("applicationId"),
      commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
      submissionId = None,
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = Time.Timestamp.Epoch,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      commands = LfCommands(
        commands = ImmArray.Empty,
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        commandsReference = "",
      ),
      ImmArray.empty,
    )

    val instance = new LedgerTimeAwareCommandExecutor(
      mockExecutor,
      mockResolveMaximumLedgerTime,
      3,
      Metrics.ForTesting,
      loggerFactory,
    )

    instance.execute(commands, submissionSeed, configuration)(loggingContext).map { actual =>
      val expectedResult = finalExecutionResult.map(let =>
        CommandExecutionResult(
          SubmitterInfo(
            Nil,
            Nil,
            Ref.ApplicationId.assertFromString("foobar"),
            Ref.CommandId.assertFromString("foobar"),
            DeduplicationDuration(Duration.ofMinutes(1)),
            None,
            configuration,
          ),
          TransactionMeta(let, None, Time.Timestamp.Epoch, submissionSeed, None, None, None, None),
          transaction,
          dependsOnLedgerTime,
          5L,
          Map.empty,
          processedDisclosedContracts,
        )
      )

      verify(mockExecutor, times(resolveMaximumLedgerTimeResults.size)).execute(
        any[Commands],
        any[Hash],
        any[Configuration],
      )(any[LoggingContextWithTrace])

      actual shouldEqual expectedResult
    }
  }

  private val missingCid: MaximumLedgerTime = MaximumLedgerTime.Archived(Set(cid))
  private val foundEpoch: MaximumLedgerTime = MaximumLedgerTime.Max(Time.Timestamp.Epoch)
  private val epochPlus5: Time.Timestamp = Time.Timestamp.Epoch.add(Duration.ofSeconds(5))
  private val foundEpochPlus5: MaximumLedgerTime = MaximumLedgerTime.Max(epochPlus5)
  private val noLetFound: MaximumLedgerTime = MaximumLedgerTime.NotAvailable

  "LedgerTimeAwareCommandExecutor" when {
    "the model doesn't use getTime" should {
      "not retry if ledger effective time is resolved" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          resolveMaximumLedgerTimeResults = List(foundEpoch),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "not retry if the maximum ledger time is not available" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          resolveMaximumLedgerTimeResults = List(noLetFound),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "retry if the contract cannot be found in the contract store and fail at max retries" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          resolveMaximumLedgerTimeResults = List(missingCid, missingCid, missingCid, missingCid),
          finalExecutionResult = Left(LedgerTime(3)),
        )
      }

      "succeed if the contract can be found on a retry" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          resolveMaximumLedgerTimeResults = List(missingCid, missingCid, missingCid, foundEpoch),
          finalExecutionResult = Right(Time.Timestamp.Epoch),
        )
      }

      "advance the output time if the contract's LET is in the future" in {
        runExecutionTest(
          dependsOnLedgerTime = false,
          resolveMaximumLedgerTimeResults = List(foundEpochPlus5),
          finalExecutionResult = Right(epochPlus5),
        )
      }
    }

    "the model uses getTime" should {
      "retry if the contract's LET is in the future" in {
        runExecutionTest(
          dependsOnLedgerTime = true,
          resolveMaximumLedgerTimeResults = List(
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
          resolveMaximumLedgerTimeResults = List(
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
