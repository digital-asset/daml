// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.ErrorDetails.RetryInfoDetail
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyDetails as ProtoPartyDetails,
}
import com.daml.lf.data.Ref
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.canton.ledger.api.domain.LedgerOffset.Absolute
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  IndexerPartyDetails,
  PartyEntry,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.blindAndConvertToProto
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementServiceSpec.*
import com.digitalasset.canton.platform.localstore.api.{PartyRecord, PartyRecordStore}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, DiscardOps}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.apache.pekko.stream.scaladsl.Source
import org.mockito.{ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class ApiPartyManagementServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ScalaFutures
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach {

  var testTelemetrySetup: TestTelemetrySetup = _
  val partiesPageSize = 100

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  private implicit val ec: ExecutionContext = directExecutionContext

  "ApiPartyManagementService" should {
    def blind(
        idpId: IdentityProviderId,
        partyDetails: IndexerPartyDetails,
        partyRecord: Option[PartyRecord],
    ): ProtoPartyDetails =
      blindAndConvertToProto(idpId)((partyDetails, partyRecord))

    "translate basic input to the output" in {
      blind(IdentityProviderId.Default, partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
    }

    "blind identity_provider_id for non default IDP" in {
      blind(IdentityProviderId("idp_1"), partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "blind identity_provider_id if record is for non default IDP" in {
      blind(
        IdentityProviderId.Default,
        partyDetails,
        Some(partyRecord.copy(identityProviderId = IdentityProviderId("idp_1"))),
      ) shouldBe protoPartyDetails.copy(identityProviderId = "")
    }

    "not blind `isLocal` if local record does not exist" in {
      blind(IdentityProviderId.Default, partyDetails, None) shouldBe protoPartyDetails
    }

    "blind `isLocal` if local record does not exist for non default IDP" in {
      blind(IdentityProviderId("idp_1"), partyDetails, None) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "propagate trace context" in {
      val (
        mockIndexTransactionsService,
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockPartyRecordStore,
      ) = mockedServices()

      when(
        mockIndexPartyManagementService.partyEntries(any[Option[Absolute]])(
          any[LoggingContextWithTrace]
        )
      )
        .thenReturn(
          Source.single(
            PartyEntry.AllocationAccepted(
              Some("aSubmission"),
              IndexerPartyDetails(aParty, None, isLocal = true),
            )
          )
        )

      val apiService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockIdentityProviderExists,
        partiesPageSize,
        mockPartyRecordStore,
        mockIndexTransactionsService,
        TestWritePartyService(testTelemetrySetup.tracer),
        Duration.Zero,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
        new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
        loggerFactory = loggerFactory,
      )

      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .allocateParty(AllocatePartyRequest("aParty"))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .futureValue

      testTelemetrySetup.reportedSpanAttributes should contain(anApplicationIdSpanAttribute)
    }

    "close while allocating party" in {
      val (
        mockIndexTransactionsService,
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockPartyRecordStore,
      ) = mockedServices()

      val promise = Promise[Unit]()

      when(
        mockIndexPartyManagementService.partyEntries(any[Option[Absolute]])(
          any[LoggingContextWithTrace]
        )
      )
        .thenReturn({
          promise.success(())
          Source.never
        })

      val apiPartyManagementService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockIdentityProviderExists,
        partiesPageSize,
        mockPartyRecordStore,
        mockIndexTransactionsService,
        TestWritePartyService(testTelemetrySetup.tracer),
        Duration.Zero,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
        NoOpTelemetry,
        loggerFactory = loggerFactory,
      )

      promise.future.map(_ => apiPartyManagementService.close()).discard

      apiPartyManagementService
        .allocateParty(AllocatePartyRequest("aParty"))
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "SERVER_IS_SHUTTING_DOWN(1,0): Server is shutting down",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "SERVER_IS_SHUTTING_DOWN",
                  Map(
                    "parties" -> "['aParty']",
                    "submissionId" -> "'aSubmission'",
                    "category" -> "1",
                    "definite_answer" -> "false",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(1.second),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }
  }

  private def mockedServices(): (
      IndexTransactionsService,
      IdentityProviderExists,
      IndexPartyManagementService,
      PartyRecordStore,
  ) = {
    val mockIndexTransactionsService = mock[IndexTransactionsService]
    when(mockIndexTransactionsService.currentLedgerEnd())
      .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))

    val mockIdentityProviderExists = mock[IdentityProviderExists]
    when(
      mockIdentityProviderExists.apply(ArgumentMatchers.eq(IdentityProviderId.Default))(
        any[LoggingContextWithTrace]
      )
    )
      .thenReturn(Future.successful(true))

    val mockIndexPartyManagementService = mock[IndexPartyManagementService]

    val mockPartyRecordStore = mock[PartyRecordStore]
    when(
      mockPartyRecordStore.createPartyRecord(any[PartyRecord])(any[LoggingContextWithTrace])
    ).thenReturn(
      Future.successful(
        Right(PartyRecord(aParty, ObjectMeta.empty, IdentityProviderId.Default))
      )
    )
    when(
      mockPartyRecordStore.getPartyRecordO(any[Ref.Party])(any[LoggingContextWithTrace])
    ).thenReturn(Future.successful(Right(None)))

    (
      mockIndexTransactionsService,
      mockIdentityProviderExists,
      mockIndexPartyManagementService,
      mockPartyRecordStore,
    )
  }
}

object ApiPartyManagementServiceSpec {

  val partyDetails: IndexerPartyDetails = IndexerPartyDetails(
    party = Ref.Party.assertFromString("Bob"),
    displayName = Some("Bob Martin"),
    isLocal = true,
  )
  val partyRecord: PartyRecord = PartyRecord(
    party = Ref.Party.assertFromString("Bob"),
    ObjectMeta.empty,
    IdentityProviderId.Default,
  )
  val protoPartyDetails: ProtoPartyDetails = ProtoPartyDetails(
    party = "Bob",
    displayName = "Bob Martin",
    localMetadata = Some(new com.daml.ledger.api.v1.admin.object_meta.ObjectMeta()),
    isLocal = true,
    identityProviderId = "",
  )

  val aParty = Ref.Party.assertFromString("aParty")

  private final case class TestWritePartyService(tracer: Tracer) extends state.WritePartyService {
    override def allocateParty(
        hint: Option[Ref.Party],
        displayName: Option[String],
        submissionId: Ref.SubmissionId,
    )(implicit
        traceContext: TraceContext
    ): CompletionStage[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
