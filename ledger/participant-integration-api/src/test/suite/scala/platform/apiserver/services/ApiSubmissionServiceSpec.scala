// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.DomainMocks
import com.daml.ledger.api.domain.{CommandId, Commands, LedgerId, PartyDetails, SubmissionId}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.{
  CommandDeduplicationNew,
  IndexPartyManagementService,
  IndexSubmissionService,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf
import com.daml.lf.command.{Commands => LfCommands}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.{LookupError, Reference}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{GlobalKey, NodeId, ReplayNodeMismatch}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.SeedService
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.execution.CommandExecutor
import com.daml.platform.apiserver.services.ApiSubmissionServiceSpec._
import com.daml.platform.store.ErrorCause
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ApiSubmissionServiceSpec
    extends AsyncFlatSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar
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

  behavior of "allocateMissingInformees"

  it should "allocate missing informees" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]
    when(partyManagementService.getParties(any[Seq[Ref.Party]])(any[LoggingContext]))
      .thenAnswer[Seq[Ref.Party]] { parties =>
        Future.successful(
          parties.view
            .filter(knownPartiesSet)
            .map(PartyDetails(_, Option.empty, isLocal = true))
            .toList
        )
      }
    when(
      writeService.allocateParty(
        any[Option[Ref.Party]],
        any[Option[Ref.Party]],
        any[Ref.SubmissionId],
      )(any[TelemetryContext])
    ).thenReturn(completedFuture(state.SubmissionResult.Acknowledged))

    val service =
      newSubmissionService(writeService, partyManagementService, implicitPartyAllocation = true)

    for {
      results <- service.allocateMissingInformees(transaction)
    } yield {
      results should have size 100
      all(results) should be(state.SubmissionResult.Acknowledged)
      missingParties.foreach { party =>
        verify(writeService).allocateParty(
          eqTo(Some(Ref.Party.assertFromString(party))),
          eqTo(Some(party)),
          any[Ref.SubmissionId],
        )(any[TelemetryContext])
      }
      verifyNoMoreInteractions(writeService)
      succeed
    }
  }

  it should "not allocate if all parties are already known" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]
    when(partyManagementService.getParties(any[Seq[Ref.Party]])(any[LoggingContext]))
      .thenAnswer[Seq[Ref.Party]] { parties =>
        Future.successful(parties.view.map(PartyDetails(_, Option.empty, isLocal = true)).toList)
      }
    when(
      writeService.allocateParty(
        any[Option[Ref.Party]],
        any[Option[Ref.Party]],
        any[Ref.SubmissionId],
      )(any[TelemetryContext])
    ).thenReturn(completedFuture(state.SubmissionResult.Acknowledged))

    val service =
      newSubmissionService(writeService, partyManagementService, implicitPartyAllocation = true)

    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq.empty[state.SubmissionResult]
      verify(writeService, never).allocateParty(
        any[Option[Ref.Party]],
        any[Option[String]],
        any[Ref.SubmissionId],
      )(any[TelemetryContext])
      succeed
    }
  }

  it should "not allocate missing informees if implicit party allocation is disabled" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]
    val service = newSubmissionService(
      writeService,
      partyManagementService,
      implicitPartyAllocation = false,
    )

    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq.empty[state.SubmissionResult]
      verify(writeService, never).allocateParty(
        any[Option[Ref.Party]],
        any[Option[String]],
        any[Ref.SubmissionId],
      )(any[TelemetryContext])
      succeed
    }
  }

  it should "forward SubmissionResult if it failed" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]

    val party = "party-1"
    val typedParty = Ref.Party.assertFromString(party)
    val submissionFailure = state.SubmissionResult.SynchronousError(
      RpcStatus.of(Status.Code.INTERNAL.value(), s"Failed to allocate $party.", Seq.empty)
    )
    when(
      writeService.allocateParty(
        eqTo(Some(typedParty)),
        eqTo(Some(party)),
        any[Ref.SubmissionId],
      )(any[TelemetryContext])
    ).thenReturn(completedFuture(submissionFailure))
    when(partyManagementService.getParties(Seq(typedParty)))
      .thenReturn(Future(List.empty[PartyDetails]))
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

    val service = newSubmissionService(
      writeService,
      partyManagementService,
      implicitPartyAllocation = true,
    )

    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq(submissionFailure)
    }
  }

  behavior of "submit"

  it should "return proper gRPC status codes for DamlLf errors" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]

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
            LookupError(
              Reference.Package(Ref.PackageId.assertFromString("-pkgId")),
              Reference.Package(Ref.PackageId.assertFromString("-pkgId")),
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
    val mockCommandExecutor = mock[CommandExecutor]

    val service = newSubmissionService(
      writeService,
      partyManagementService,
      implicitPartyAllocation = true,
      commandExecutor = mockCommandExecutor,
    )

    Future
      .sequence(errorsToStatuses.map { case (error, code) =>
        val submitRequest = newSubmitRequest()
        when(
          mockCommandExecutor.execute(
            eqTo(submitRequest.commands),
            any[Hash],
            any[Configuration],
          )(any[ExecutionContext], any[LoggingContext])
        ).thenReturn(Future.successful(Left(error)))

        service.submit(submitRequest).transform(result => Success(code -> result))
      })
      .map { results =>
        results.foreach { case (code, result) =>
          inside(result) { case Failure(exception) =>
            exception.getMessage should startWith(code.getCode.toString)
          }
        }
        succeed
      }
  }

  behavior of "command deduplication"

  it should "use deduplication if enabled" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]
    val indexSubmissionService = mock[IndexSubmissionService]
    val mockCommandExecutor = mock[CommandExecutor]

    when(
      mockCommandExecutor.execute(
        any[Commands],
        any[Hash],
        any[Configuration],
      )(any[ExecutionContext], any[LoggingContext])
    ).thenReturn(
      Future.failed(
        new RuntimeException
      ) // we don't care about the result, deduplication already happened
    )

    val service =
      newSubmissionService(
        writeService,
        partyManagementService,
        implicitPartyAllocation = true,
        deduplicationEnabled = true,
        mockIndexSubmissionService = indexSubmissionService,
        commandExecutor = mockCommandExecutor,
      )

    val submitRequest = newSubmitRequest()
    service
      .submit(submitRequest)
      .transform(_ => {
        verify(indexSubmissionService).deduplicateCommand(
          any[CommandId],
          any[List[Ref.Party]],
          any[Instant],
          any[Instant],
        )(any[LoggingContext])
        Success(succeed)
      })
  }

  it should "not use deduplication when disabled" in {
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[state.WriteService]
    val indexSubmissionService = mock[IndexSubmissionService]
    val mockCommandExecutor = mock[CommandExecutor]

    when(
      mockCommandExecutor.execute(
        any[Commands],
        any[Hash],
        any[Configuration],
      )(any[ExecutionContext], any[LoggingContext])
    ).thenReturn(
      Future.failed(
        new RuntimeException
      ) // we don't care about the result, deduplication already happened
    )

    val service =
      newSubmissionService(
        writeService,
        partyManagementService,
        implicitPartyAllocation = true,
        deduplicationEnabled = false,
        mockIndexSubmissionService = indexSubmissionService,
        commandExecutor = mockCommandExecutor,
      )

    val submitRequest = newSubmitRequest()
    service
      .submit(submitRequest)
      .transform(_ => {
        verify(indexSubmissionService, never).deduplicateCommand(
          any[CommandId],
          any[List[Ref.Party]],
          any[Instant],
          any[Instant],
        )(any[LoggingContext])
        Success(succeed)
      })
  }
}

object ApiSubmissionServiceSpec {

  import ArgumentMatchersSugar._
  import MockitoSugar._

  val commandId = new AtomicInteger()

  private def newSubmitRequest() = {
    SubmitRequest(
      Commands(
        ledgerId = LedgerId("ledger-id"),
        workflowId = None,
        applicationId = DomainMocks.applicationId,
        commandId = CommandId(
          Ref.CommandId.assertFromString(s"commandId-${commandId.incrementAndGet()}")
        ),
        submissionId = SubmissionId(Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)),
        actAs = Set.empty,
        readAs = Set.empty,
        submittedAt = Instant.MIN,
        deduplicationDuration = Duration.ZERO,
        commands = LfCommands(ImmArray.empty, Timestamp.MinValue, ""),
      )
    )
  }

  private def newSubmissionService(
      writeService: state.WriteService,
      partyManagementService: IndexPartyManagementService,
      implicitPartyAllocation: Boolean,
      commandExecutor: CommandExecutor = null,
      deduplicationEnabled: Boolean = true,
      mockIndexSubmissionService: IndexSubmissionService = mock[IndexSubmissionService],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ApiSubmissionService = {
    val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
      override def latestConfiguration(): Option[Configuration] =
        Some(Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ZERO))
    }

    when(writeService.isApiDeduplicationEnabled).thenReturn(deduplicationEnabled)
    when(
      mockIndexSubmissionService.deduplicateCommand(
        any[CommandId],
        anyList[Ref.Party],
        any[Instant],
        any[Instant],
      )(any[LoggingContext])
    ).thenReturn(Future.successful(CommandDeduplicationNew))
    when(
      mockIndexSubmissionService.stopDeduplicatingCommand(
        any[CommandId],
        anyList[Ref.Party],
      )(any[LoggingContext])
    ).thenReturn(Future.unit)

    new ApiSubmissionService(
      writeService = writeService,
      submissionService = mockIndexSubmissionService,
      partyManagementService = partyManagementService,
      timeProvider = null,
      timeProviderType = null,
      ledgerConfigurationSubscription = ledgerConfigurationSubscription,
      seedService = SeedService.WeakRandom,
      commandExecutor = commandExecutor,
      configuration = ApiSubmissionService
        .Configuration(implicitPartyAllocation),
      metrics = new Metrics(new MetricRegistry),
    )
  }
}
