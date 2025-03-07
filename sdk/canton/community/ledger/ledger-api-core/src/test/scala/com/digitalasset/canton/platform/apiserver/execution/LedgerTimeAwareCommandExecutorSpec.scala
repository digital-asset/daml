// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.{DeduplicationPeriod, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.{CommandId, Commands}
import com.digitalasset.canton.ledger.participant.state.index.MaximumLedgerTime
import com.digitalasset.canton.ledger.participant.state.{
  RoutingSynchronizerState,
  SubmitterInfo,
  SynchronizerRank,
  TransactionMeta,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.LedgerTime
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.digitalasset.daml.lf.command.ApiCommands as LfCommands
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageName, PackageVersion}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.ExecutionContext

class LedgerTimeAwareCommandExecutorSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with FailOnShutdown
    with BaseTest {

  private val loggingContext =
    LoggingContextWithTrace.ForTesting

  private val submissionSeed = Hash.hashPrivateKey("a key")

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
      packageName = PackageName.assertFromString("pkg-name"),
      packageVersion = Some(PackageVersion.assertFromString("1.0.0")),
      contractId = cid,
      argument = Value.ValueNil,
      createdAt = Time.Timestamp.Epoch,
      driverMetadata = Bytes.Empty,
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      // TODO(#19494): Change to minVersion once 2.2 is released and 2.1 is removed
      version = LanguageVersion.v2_dev,
    )
  )
  private val synchronizerRank = SynchronizerRank.single(SynchronizerId.tryFromString("some::sync"))
  private val routingSynchronizerState = mock[RoutingSynchronizerState]
  private def runExecutionTest(
      dependsOnLedgerTime: Boolean,
      resolveMaximumLedgerTimeResults: List[MaximumLedgerTime],
      finalExecutionResult: Either[ErrorCause, Time.Timestamp],
      usedForExternalSigning: Boolean = false,
  ): FutureUnlessShutdown[Assertion] = {

    def commandExecutionResult(let: Time.Timestamp) = CommandExecutionResult(
      commandInterpretationResult = CommandInterpretationResult(
        SubmitterInfo(
          Nil,
          Nil,
          Ref.ApplicationId.assertFromString("foobar"),
          Ref.CommandId.assertFromString("foobar"),
          DeduplicationDuration(Duration.ofMinutes(1)),
          None,
          None,
        ),
        TransactionMeta(
          let,
          None,
          Time.Timestamp.Epoch,
          submissionSeed,
          None,
          None,
          None,
        ),
        transaction,
        dependsOnLedgerTime,
        5L,
        Map.empty,
        processedDisclosedContracts,
        None,
      ),
      synchronizerRank = SynchronizerRank.single(SynchronizerId.tryFromString("some::sync")),
      routingSynchronizerState = routingSynchronizerState,
    )

    val mockExecutor = mock[CommandExecutor]
    when(
      mockExecutor.execute(any[Commands], any[Hash], anyBoolean)(
        any[LoggingContextWithTrace]
      )
    )
      .thenAnswer((c: Commands) =>
        EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult](
          FutureUnlessShutdown.pure(Right(commandExecutionResult(c.commands.ledgerEffectiveTime)))
        )
      )

    val mockResolveMaximumLedgerTime = mock[ResolveMaximumLedgerTime]
    resolveMaximumLedgerTimeResults.tail.foldLeft(
      when(
        mockResolveMaximumLedgerTime(
          eqTo(processedDisclosedContracts),
          any[Set[ContractId]],
        )(
          any[LoggingContextWithTrace],
          any[ExecutionContext],
        )
      )
        .thenReturn(FutureUnlessShutdown.pure(resolveMaximumLedgerTimeResults.head))
    ) { case (mock, result) =>
      mock.andThen(FutureUnlessShutdown.pure(result))
    }

    val commands = Commands(
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
      disclosedContracts = ImmArray.empty,
      synchronizerId = None,
      prefetchKeys = Seq.empty,
    )

    val instance = new LedgerTimeAwareCommandExecutor(
      mockExecutor,
      mockResolveMaximumLedgerTime,
      3,
      LedgerApiServerMetrics.ForTesting,
      loggerFactory,
    )

    instance
      .execute(
        commands,
        submissionSeed,
        usedForExternallySigningTransaction = usedForExternalSigning,
      )(loggingContext)
      .value
      .map { actual =>
        val expectedResult = finalExecutionResult.map(let =>
          CommandExecutionResult(
            CommandInterpretationResult(
              SubmitterInfo(
                Nil,
                Nil,
                Ref.ApplicationId.assertFromString("foobar"),
                Ref.CommandId.assertFromString("foobar"),
                DeduplicationDuration(Duration.ofMinutes(1)),
                None,
                None,
              ),
              TransactionMeta(
                let,
                None,
                Time.Timestamp.Epoch,
                submissionSeed,
                None,
                None,
                None,
              ),
              transaction,
              dependsOnLedgerTime,
              5L,
              Map.empty,
              processedDisclosedContracts,
              None,
            ),
            synchronizerRank = synchronizerRank,
            routingSynchronizerState = routingSynchronizerState,
          )
        )

        verify(mockExecutor, times(resolveMaximumLedgerTimeResults.size)).execute(
          any[Commands],
          any[Hash],
          eqTo(usedForExternalSigning),
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
