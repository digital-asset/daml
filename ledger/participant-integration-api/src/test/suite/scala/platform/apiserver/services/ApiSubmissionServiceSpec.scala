// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.Materializer
import com.codahale.metrics.{Meter, MetricRegistry}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.DomainMocks
import com.daml.ledger.api.domain.LedgerOffset.Absolute
import com.daml.ledger.api.domain.{CommandId, Commands, LedgerId, PartyDetails}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationNew,
  IndexConfigManagementService,
  IndexPartyManagementService,
  IndexSubmissionService,
}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf
import com.daml.lf.command.{Commands => LfCommands}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.LookupError
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{GlobalKey, NodeId, ReplayNodeMismatch}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.SeedService
import com.daml.platform.apiserver.execution.CommandExecutor
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.store.ErrorCause
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import io.grpc.Status
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, Mockito, MockitoSugar}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, OneInstancePerTest, Succeeded}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ApiSubmissionServiceSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with OneInstancePerTest
    with BeforeAndAfter
    with AkkaBeforeAndAfterAll
    with TestResourceContext {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  private val builder = TransactionBuilder()
  private val knownParties = (1 to 100).map(idx => s"party-$idx").toArray
  private val knownPartiesSet = knownParties.toSet
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
        s"#contractId$i",
        "template:test:test",
        Value.ValueNil,
        signatories.toSeq,
        observers.toSeq,
        Option.empty,
      )
    )
  }
  private val transaction = builder.buildSubmitted()

  private val partyManagementService = mock[IndexPartyManagementService]
  private val writeService = mock[WriteService]
  private val defaultSubmissionService =
    submissionService(writeService, partyManagementService, implicitPartyAllocation = true)

  before {
    when(
      writeService.allocateParty(any[Option[Ref.Party]], any[Option[Ref.Party]], any[SubmissionId])(
        any[TelemetryContext]
      )
    )
      .thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))
  }

  behavior of "allocateMissingInformees"

  it should "allocate missing informees" in {
    val argCaptor = ArgCaptor[Seq[Ref.Party]]

    when(partyManagementService.getParties(argCaptor.capture)(any[LoggingContext])).thenAnswer(
      Future.successful(
        argCaptor.value
          .filter(knownPartiesSet)
          .map(PartyDetails(_, Option.empty, isLocal = true))
          .toList
      )
    )

    for {
      service <- defaultSubmissionService
      results <- service.allocateMissingInformees(transaction)
    } yield {
      results.find(_ != SubmissionResult.Acknowledged) shouldBe None
      results.size shouldBe 100
      Mockito.mockingDetails(partyManagementService).getInvocations.size() shouldBe 1
      Mockito.mockingDetails(writeService).getInvocations.size() shouldBe 100
      missingParties.foreach(party =>
        verify(writeService).allocateParty(
          eqTo(Some(Ref.Party.assertFromString(party))),
          eqTo(Some(Ref.Party.assertFromString(party))),
          any[SubmissionId],
        )(any[TelemetryContext])
      )
      verifyNoMoreInteractions(writeService)
      succeed
    }
  }

  it should "not allocate if all parties are already known" in {
    val argCaptor = ArgCaptor[Seq[Ref.Party]]
    when(partyManagementService.getParties(argCaptor.capture)(any[LoggingContext])).thenAnswer(
      Future.successful(argCaptor.value.map(PartyDetails(_, Option.empty, isLocal = true)).toList)
    )

    for {
      service <- defaultSubmissionService
      result <- service.allocateMissingInformees(transaction)
    } yield {
      verifyZeroInteractions(writeService)
      result shouldBe Seq.empty[SubmissionResult]
    }
  }

  it should "not allocate missing informees if implicit party allocation is disabled" in {
    for {
      service <- submissionService(null, null, implicitPartyAllocation = false)
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq.empty[SubmissionResult]
    }
  }

  it should "forward SubmissionResult if it failed" in {
    val party = "party-1"
    val typedParty = Ref.Party.assertFromString(party)
    val submissionFailure = SubmissionResult.InternalError(s"failed to allocate $party")
    when(
      writeService.allocateParty(eqTo(Some(typedParty)), eqTo(Some(party)), any[SubmissionId])(
        any[TelemetryContext]
      )
    ).thenReturn(CompletableFuture.completedFuture(submissionFailure))
    when(partyManagementService.getParties(eqTo(Seq(typedParty)))(any[LoggingContext])).thenAnswer(
      Future(List.empty[PartyDetails])
    )
    val builder = TransactionBuilder()
    builder.add(
      builder.create(
        s"#contractId1",
        "template:test:test",
        Value.ValueNil,
        Seq(party),
        Seq.empty,
        Option.empty,
      )
    )
    val transaction = builder.buildSubmitted()

    for {
      service <- defaultSubmissionService
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq(submissionFailure)
    }
  }

  behavior of "submit"

  it should "return proper gRPC status codes for DamlLf errors" in {
    val tmplId = Ref.Identifier.assertFromString("pkgId:M:T")

    val errorsToStatuses = List(
      ErrorCause.DamlLf(
        LfError.Interpretation(
          LfError.Interpretation.DamlException(
            LfInterpretationError.ContractNotFound(Value.ContractId.assertFromString("#cid"))
          ),
          None,
        )
      ) -> Status.ABORTED,
      ErrorCause.DamlLf(
        LfError.Interpretation(
          LfError.Interpretation.DamlException(
            LfInterpretationError.DuplicateContractKey(
              GlobalKey.assertBuild(tmplId, Value.ValueUnit)
            )
          ),
          None,
        )
      ) -> Status.ABORTED,
      ErrorCause.DamlLf(
        LfError.Validation(
          LfError.Validation.ReplayMismatch(ReplayNodeMismatch(null, null, null, null))
        )
      ) -> Status.ABORTED,
      ErrorCause.DamlLf(
        LfError.Preprocessing(
          LfError.Preprocessing.Lookup(
            LookupError.Package(Ref.PackageId.assertFromString("-pkgId"))
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
    val commandId = new AtomicInteger()
    val mockCommandExecutor = mock[CommandExecutor]

    Future
      .sequence(errorsToStatuses.map { case (error, code) =>
        val submitRequest = SubmitRequest(
          Commands(
            ledgerId = LedgerId("ledger-id"),
            workflowId = None,
            applicationId = DomainMocks.applicationId,
            commandId = CommandId(
              Ref.LedgerString.assertFromString(s"commandId-${commandId.incrementAndGet()}")
            ),
            actAs = Set.empty,
            readAs = Set.empty,
            submittedAt = Instant.MIN,
            deduplicateUntil = Instant.MIN,
            commands = LfCommands(ImmArray.empty, Timestamp.MinValue, ""),
          )
        )
        when(
          mockCommandExecutor.execute(eqTo(submitRequest.commands), any[Hash])(
            any[ExecutionContext],
            any[LoggingContext],
          )
        ).thenReturn(Future.successful(Left(error)))

        for {
          service <- submissionService(
            writeService,
            partyManagementService,
            implicitPartyAllocation = true,
            commandExecutor = mockCommandExecutor,
          )
          assertions <- service.submit(submitRequest).transform {
            case Success(_) => fail()
            case Failure(e) => Success(e.getMessage should startWith(code.getCode.toString))
          }
        } yield assertions
      })
      .map(_.forall(_ == Succeeded))
      .map(assert(_))
  }

  private def submissionService(
      writeService: WriteService,
      partyManagementService: IndexPartyManagementService,
      implicitPartyAllocation: Boolean,
      commandExecutor: CommandExecutor = null,
  )(implicit materializer: Materializer) = {
    val mockMetricRegistry = mock[MetricRegistry]
    val mockIndexSubmissionService = mock[IndexSubmissionService]
    val mockConfigManagementService = mock[IndexConfigManagementService]
    val configuration = Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ZERO)
    when(mockMetricRegistry.meter(any[String])).thenReturn(new Meter())
    when(
      mockIndexSubmissionService.deduplicateCommand(
        any[CommandId],
        anyList[Ref.Party],
        any[Instant],
        any[Instant],
      )(any[LoggingContext])
    ).thenReturn(Future.successful(CommandDeduplicationNew))
    when(
      mockIndexSubmissionService.stopDeduplicatingCommand(any[CommandId], anyList[Ref.Party])(
        any[LoggingContext]
      )
    ).thenReturn(Future.unit)
    when(mockConfigManagementService.lookupConfiguration())
      .thenReturn(
        Future.successful(
          Some((Absolute(Ref.LedgerString.assertFromString("offset")), configuration))
        )
      )

    val configProviderResource = LedgerConfigProvider
      .owner(
        mockConfigManagementService,
        optWriteService = Some(writeService),
        timeProvider = mock[TimeProvider],
        config = LedgerConfiguration(
          initialConfiguration = configuration,
          initialConfigurationSubmitDelay = Duration.ZERO,
          configurationLoadTimeout = Duration.ZERO,
        ),
      )
      .acquire()

    for {
      configProvider <- configProviderResource.asFuture
      _ <- configProviderResource.release()
    } yield {
      new ApiSubmissionService(
        writeService = writeService,
        submissionService = mockIndexSubmissionService,
        partyManagementService = partyManagementService,
        timeProvider = null,
        timeProviderType = null,
        ledgerConfigProvider = configProvider,
        seedService = SeedService.WeakRandom,
        commandExecutor = commandExecutor,
        configuration = ApiSubmissionService.Configuration(implicitPartyAllocation),
        metrics = new Metrics(mockMetricRegistry),
      )
    }
  }
}
