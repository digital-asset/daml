// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.domain.{CommandId, Commands}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2.IndexPartyManagementService
import com.daml.ledger.participant.state.v2.{SubmissionResult, SubmitterInfo, TransactionMeta}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf
import com.daml.lf.command.{
  ContractMetadata,
  DisclosedContract,
  EngineEnrichedContractMetadata,
  ProcessedDisclosedContract,
  ApiCommands => LfCommands,
}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.{LookupError, Reference}
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.SeedService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.execution.{CommandExecutionResult, CommandExecutor}
import com.daml.platform.services.time.TimeProviderType
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}
import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ApiSubmissionServiceSpec
    extends AnyFlatSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ScalaFutures
    with IntegrationPatience
    with ArgumentMatchersSugar {

  import TransactionBuilder.Implicits._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  private val builder = TransactionBuilder()
  private val knownParties = (1 to 100).map(idx => s"party-$idx").toArray
  private val missingParties = (101 to 200).map(idx => s"party-$idx").toArray
  private val allInformeesInTransaction = knownParties ++ missingParties
  for {
    i <- 0 until 100
  } {
    // Ensure 100 % overlap by having 4 informees per each of the 100 nodes
    val informeesOfNode = allInformeesInTransaction.slice(i * 4, (i + 1) * 4)
    val (signatories, observers) = informeesOfNode.splitAt(2)
    builder.add(
      builder.create(
        Value.ContractId.V1(Hash.hashPrivateKey(i.toString)).coid,
        "test:test",
        Value.ValueNil,
        signatories.toSeq,
        observers.toSeq,
        Option.empty,
      )
    )
  }
  private val transaction = builder.buildSubmitted()

  behavior of "submit"

  it should "finish successfully in the happy flow" in new TestContext {
    apiSubmissionService()
      .submit(SubmitRequest(commands))
      .futureValue
  }

  behavior of "submit"

  it should "return proper gRPC status codes for DamlLf errors" in new TestContext {
    val tmplId = toIdentifier("M:T")

    val errorsToExpectedStatuses: Seq[(ErrorCause, Status)] = List(
      ErrorCause.DamlLf(
        LfError.Interpretation(
          LfError.Interpretation.DamlException(
            LfInterpretationError.ContractNotFound("00" + "00" * 32)
          ),
          None,
        )
      ) -> Status.NOT_FOUND,
      ErrorCause.DamlLf(
        LfError.Interpretation(
          LfError.Interpretation.DamlException(
            LfInterpretationError.DuplicateContractKey(
              GlobalKey.assertBuild(tmplId, Value.ValueUnit)
            )
          ),
          None,
        )
      ) -> Status.ALREADY_EXISTS,
      ErrorCause.DamlLf(
        LfError.Validation(
          LfError.Validation.ReplayMismatch(ReplayMismatch(null, null))
        )
      ) -> Status.INTERNAL,
      ErrorCause.DamlLf(
        LfError.Preprocessing(
          LfError.Preprocessing.Lookup(
            LookupError.NotFound(
              Reference.Package(defaultPackageId),
              Reference.Package(defaultPackageId),
            )
          )
        )
      ) -> Status.INVALID_ARGUMENT,
      ErrorCause.DamlLf(
        LfError.Interpretation(
          LfError.Interpretation.DamlException(
            LfInterpretationError.FailedAuthorization(
              NodeId(1),
              lf.ledger.FailedAuthorization.NoSignatories(tmplId, None),
            )
          ),
          None,
        )
      ) -> Status.INVALID_ARGUMENT,
      ErrorCause.LedgerTime(0) -> Status.ABORTED,
    )

    // when
    private val results = errorsToExpectedStatuses
      .map { case (error, expectedStatus) =>
        when(
          commandExecutor.execute(
            eqTo(commands),
            any[Hash],
            any[Configuration],
          )(any[LoggingContext])
        ).thenReturn(Future.successful(Left(error)))

        apiSubmissionService()
          .submit(SubmitRequest(commands))
          .transform(result => Success(expectedStatus -> result))
          .futureValue
      }

    // then
    results.foreach { case (expectedStatus: Status, result: Try[Unit]) =>
      inside(result) { case Failure(exception) =>
        exception.getMessage should startWith(expectedStatus.getCode.toString)
      }
    }
  }

  it should "rate-limit when configured to do so" in new TestContext {
    val grpcError = RpcStatus.of(Status.Code.ABORTED.value(), s"Quota Exceeded", Seq.empty)

    apiSubmissionService(checkOverloaded = _ => Some(SubmissionResult.SynchronousError(grpcError)))
      .submit(SubmitRequest(commands))
      .transform {
        case Failure(e: StatusRuntimeException)
            if e.getStatus.getCode.value == grpcError.code && e.getStatus.getDescription == grpcError.message =>
          Success(succeed)
        case result =>
          fail(s"Expected submission to be aborted, but got $result")
      }
      .futureValue
  }

  private trait TestContext {
    val writeService = mock[state.WriteService]
    val partyManagementService = mock[IndexPartyManagementService]
    val timeProvider = TimeProvider.Constant(Instant.now)
    val timeProviderType = TimeProviderType.Static
    val ledgerConfigurationSubscription = mock[LedgerConfigurationSubscription]
    val seedService = SeedService.WeakRandom
    val commandExecutor = mock[CommandExecutor]
    val metrics = Metrics.ForTesting

    val disclosedContract = DisclosedContract(
      templateId = Identifier.assertFromString("some:pkg:identifier"),
      contractId = TransactionBuilder.newCid,
      argument = Value.ValueNil,
      metadata = ContractMetadata(
        createdAt = Timestamp.Epoch,
        keyHash = None,
        driverMetadata = ImmArray.empty,
      ),
    )
    val engineEnrichedDisclosedContract = ProcessedDisclosedContract(
      templateId = Identifier.assertFromString("some:pkg:identifier"),
      contractId = TransactionBuilder.newCid,
      argument = Value.ValueNil,
      metadata = EngineEnrichedContractMetadata(
        createdAt = Timestamp.Epoch,
        driverMetadata = ImmArray.empty,
        signatories = Set.empty,
        stakeholders = Set.empty,
        maybeKeyWithMaintainers = None,
        agreementText = "",
      ),
    )
    val commands = Commands(
      ledgerId = None,
      workflowId = None,
      applicationId = Ref.ApplicationId.assertFromString("app"),
      commandId = CommandId(Ref.CommandId.assertFromString("cmd")),
      submissionId = None,
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = Timestamp.Epoch,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      commands = LfCommands(
        commands = ImmArray.empty,
        ledgerEffectiveTime = Timestamp.Epoch,
        commandsReference = "",
      ),
      disclosedContracts = ImmArray(
        disclosedContract
      ),
    )

    val ledgerConfiguration = Configuration.reasonableInitialConfiguration.copy(generation = 7L)

    val submitterInfo = SubmitterInfo(
      actAs = Nil,
      readAs = Nil,
      applicationId = Ref.ApplicationId.assertFromString("foobar"),
      commandId = Ref.CommandId.assertFromString("foobar"),
      deduplicationPeriod = DeduplicationDuration(Duration.ofMinutes(1)),
      submissionId = None,
      ledgerConfiguration = ledgerConfiguration,
    )
    val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = Timestamp.Epoch,
      workflowId = None,
      submissionTime = Time.Timestamp.Epoch,
      submissionSeed = Hash.hashPrivateKey("SomeHash"),
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )
    val estimatedInterpretationCost = 5L
    val explicitlyDisclosedContracts =
      ImmArray(Versioned(TransactionVersion.VDev, engineEnrichedDisclosedContract))
    val commandExecutionResult = CommandExecutionResult(
      submitterInfo = submitterInfo,
      transactionMeta = transactionMeta,
      transaction = transaction,
      dependsOnLedgerTime = false,
      interpretationTimeNanos = estimatedInterpretationCost,
      globalKeyMapping = Map.empty,
      usedDisclosedContracts = explicitlyDisclosedContracts,
    )

    when(ledgerConfigurationSubscription.latestConfiguration())
      .thenReturn(Some(ledgerConfiguration))
    when(
      commandExecutor.execute(eqTo(commands), any[Hash], eqTo(ledgerConfiguration))(
        any[LoggingContext]
      )
    )
      .thenReturn(Future.successful(Right(commandExecutionResult)))
    when(
      writeService.submitTransaction(
        submitterInfo,
        transactionMeta,
        transaction,
        estimatedInterpretationCost,
        Map.empty,
        explicitlyDisclosedContracts,
      )
    ).thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))

    def apiSubmissionService(
        checkOverloaded: TelemetryContext => Option[state.SubmissionResult] = _ => None
    ) = new ApiSubmissionService(
      writeService = writeService,
      timeProviderType = timeProviderType,
      timeProvider = timeProvider,
      ledgerConfigurationSubscription = ledgerConfigurationSubscription,
      seedService = seedService,
      commandExecutor = commandExecutor,
      checkOverloaded = checkOverloaded,
      metrics = metrics,
    )
  }
}
