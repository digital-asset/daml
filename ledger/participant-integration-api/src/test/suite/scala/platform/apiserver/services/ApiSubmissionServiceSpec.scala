// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.codahale.metrics.MetricRegistry
import com.daml.error.definitions.ErrorCause
import com.daml.ledger.api.domain.{CommandId, Commands, LedgerId, PartyDetails}
import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.{DeduplicationPeriod, DomainMocks}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.IndexPartyManagementService
import com.daml.ledger.participant.state.v2.{SubmissionResult, WriteService}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf
import com.daml.lf.command.{ApiCommands => LfCommands}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.lf.language.{LookupError, Reference}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{GlobalKey, NodeId, ReplayMismatch}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.daml.platform.apiserver.execution.CommandExecutor
import com.daml.platform.apiserver.services.ApiSubmissionServiceSpec._
import com.daml.platform.apiserver.SeedService
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside
import java.time.Duration
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ApiSubmissionServiceSpec
    extends AsyncFlatSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar {

  import TransactionBuilder.Implicits._

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
      )(any[LoggingContext], any[TelemetryContext])
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
        )(any[LoggingContext], any[TelemetryContext])
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
      )(any[LoggingContext], any[TelemetryContext])
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
      )(any[LoggingContext], any[TelemetryContext])
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
      )(any[LoggingContext], any[TelemetryContext])
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
      )(any[LoggingContext], any[TelemetryContext])
    ).thenReturn(completedFuture(submissionFailure))
    when(partyManagementService.getParties(Seq(typedParty)))
      .thenReturn(Future(List.empty[PartyDetails]))
    val builder = TransactionBuilder()
    builder.add(
      builder.create(
        "00" + "00" * 32 + "01",
        "test:test",
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
    // given
    val partyManagementService = mock[IndexPartyManagementService]
    val writeService = mock[WriteService]
    val mockCommandExecutor = mock[CommandExecutor]
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
            LookupError(
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

    val service = newSubmissionService(
      writeService,
      partyManagementService,
      implicitPartyAllocation = true,
      commandExecutor = mockCommandExecutor,
    )

    // when
    val results: Seq[Future[(Status, Try[Unit])]] = errorsToExpectedStatuses
      .map { case (error, expectedStatus) =>
        val submitRequest = newSubmitRequest()
        when(
          mockCommandExecutor.execute(
            eqTo(submitRequest.commands),
            any[Hash],
            any[Configuration],
          )(any[LoggingContext])
        ).thenReturn(Future.successful(Left(error)))
        service
          .submit(submitRequest)
          .transform(result => Success(expectedStatus -> result))
      }
    val sequencedResults: Future[Seq[(Status, Try[Unit])]] = Future.sequence(results)

    // then
    sequencedResults.map { results: Seq[(Status, Try[Unit])] =>
      results.foreach { case (expectedStatus: Status, result: Try[Unit]) =>
        inside(result) { case Failure(exception) =>
          exception.getMessage should startWith(expectedStatus.getCode.toString)
        }
      }
      succeed
    }
  }

  it should "rate-limit when configured to do so" in {
    val grpcError = RpcStatus.of(Status.Code.ABORTED.value(), s"Quota Exceeded", Seq.empty)

    val service =
      newSubmissionService(
        mock[state.WriteService],
        mock[IndexPartyManagementService],
        implicitPartyAllocation = true,
        commandExecutor = mock[CommandExecutor],
        checkOverloaded = _ => Some(SubmissionResult.SynchronousError(grpcError)),
      )

    val submitRequest = newSubmitRequest()
    service
      .submit(submitRequest)
      .transform {
        case Failure(e: StatusRuntimeException)
            if e.getStatus.getCode.value == grpcError.code && e.getStatus.getDescription == grpcError.message =>
          Success(succeed)
        case result =>
          Try(fail(s"Expected submission to be aborted, but got ${result}"))
      }
  }
}

object ApiSubmissionServiceSpec {
  val commandId = new AtomicInteger()

  private def newSubmitRequest() = {
    SubmitRequest(
      Commands(
        ledgerId = Some(LedgerId("ledger-id")),
        workflowId = None,
        applicationId = DomainMocks.applicationId,
        commandId = CommandId(
          Ref.CommandId.assertFromString(s"commandId-${commandId.incrementAndGet()}")
        ),
        submissionId = None,
        actAs = Set.empty,
        readAs = Set.empty,
        submittedAt = Timestamp.Epoch,
        deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
        commands = LfCommands(ImmArray.Empty, Timestamp.MinValue, ""),
      )
    )
  }

  private def newSubmissionService(
      writeService: state.WriteService,
      partyManagementService: IndexPartyManagementService,
      implicitPartyAllocation: Boolean,
      commandExecutor: CommandExecutor = null,
      checkOverloaded: TelemetryContext => Option[SubmissionResult] = _ => None,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ApiSubmissionService = {
    val ledgerConfigurationSubscription = new LedgerConfigurationSubscription {
      override def latestConfiguration(): Option[Configuration] =
        Some(Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ZERO))
    }

    new ApiSubmissionService(
      writeService = writeService,
      partyManagementService = partyManagementService,
      timeProvider = null,
      timeProviderType = null,
      ledgerConfigurationSubscription = ledgerConfigurationSubscription,
      seedService = SeedService.WeakRandom,
      commandExecutor = commandExecutor,
      configuration = ApiSubmissionService.Configuration(implicitPartyAllocation),
      metrics = new Metrics(new MetricRegistry),
      checkOverloaded = checkOverloaded,
    )
  }
}
