// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  PartyDetails as ProtoPartyDetails,
}
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.base.error.utils.ErrorDetails.RetryInfoDetail
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta}
import com.digitalasset.canton.ledger.localstore.api.{
  PartyRecord,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexPartyManagementService,
  IndexerPartyDetails,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.blindAndConvertToProto
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementServiceSpec.*
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.tracking.{InFlight, StreamTracker}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ExternalPartyOnboardingDetails,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.{ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
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
  val partiesPageSize = PositiveInt.tryCreate(100)

  val aSubmissionId = Ref.SubmissionId.assertFromString("aSubmissionId")

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

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
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockPartyRecordStore,
      ) = mockedServices()
      val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)

      val apiService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockIdentityProviderExists,
        partiesPageSize,
        mockPartyRecordStore,
        TestPartySyncService(testTelemetrySetup.tracer),
        oneHour,
        ApiPartyManagementService.CreateSubmissionId.fixedForTests(aSubmissionId),
        new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
        partyAllocationTracker,
        participantId = participantId,
        loggerFactory = loggerFactory,
      )

      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()

      // Kick the interaction off
      val future = apiService
        .allocateParty(AllocatePartyRequest("aParty", None, "", "", ""))
        .thereafter { _ =>
          scope.close()
          span.end()
        }

      // Allow the tracker to complete
      partyAllocationTracker.onStreamItem(
        PartyAllocation.Completed(
          PartyAllocation.TrackerKey.forTests(aSubmissionId),
          IndexerPartyDetails(aParty, isLocal = true),
        )
      )

      // Wait for tracker to complete
      future.futureValue

      testTelemetrySetup.reportedSpanAttributes should contain(anUserIdSpanAttribute)
    }

    "close while allocating party" in {
      val (
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockPartyRecordStore,
      ) = mockedServices()
      val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)

      val apiPartyManagementService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockIdentityProviderExists,
        partiesPageSize,
        mockPartyRecordStore,
        TestPartySyncService(testTelemetrySetup.tracer),
        oneHour,
        ApiPartyManagementService.CreateSubmissionId.fixedForTests(aSubmissionId.toString),
        NoOpTelemetry,
        partyAllocationTracker,
        participantId = participantId,
        loggerFactory = loggerFactory,
      )

      // Kick the interaction off
      val future =
        apiPartyManagementService.allocateParty(AllocatePartyRequest("aParty", None, "", "", ""))

      // Close the service
      apiPartyManagementService.close()

      // Assert that it caused the appropriate failure
      future
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "ABORTED_DUE_TO_SHUTDOWN(1,0): request aborted due to shutdown",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "ABORTED_DUE_TO_SHUTDOWN",
                  Map(
                    "parties" -> "['aParty']",
                    "category" -> "1",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(10.seconds),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }
  }

  private def makePartyAllocationTracker(
      loggerFactory: NamedLoggerFactory
  ): PartyAllocation.Tracker =
    StreamTracker.withTimer[PartyAllocation.TrackerKey, PartyAllocation.Completed](
      timer = new java.util.Timer("test-timer"),
      itemKey = (_ => Some(PartyAllocation.TrackerKey.forTests(aSubmissionId))),
      inFlightCounter = InFlight.Limited(100, mock[com.daml.metrics.api.MetricHandle.Counter]),
      loggerFactory,
    )

  private def mockedServices(): (
      IdentityProviderExists,
      IndexPartyManagementService,
      UserManagementStore,
      PartyRecordStore,
  ) = {
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

    val mockUserManagementStore = mock[UserManagementStore]

    (
      mockIdentityProviderExists,
      mockIndexPartyManagementService,
      mockUserManagementStore,
      mockPartyRecordStore,
    )
  }
}

object ApiPartyManagementServiceSpec {

  val participantId = DefaultTestIdentities.participant1.toLf

  val partyDetails: IndexerPartyDetails = IndexerPartyDetails(
    party = Ref.Party.assertFromString("Bob"),
    isLocal = true,
  )
  val partyRecord: PartyRecord = PartyRecord(
    party = Ref.Party.assertFromString("Bob"),
    ObjectMeta.empty,
    IdentityProviderId.Default,
  )
  val protoPartyDetails: ProtoPartyDetails = ProtoPartyDetails(
    party = "Bob",
    localMetadata = Some(new com.daml.ledger.api.v2.admin.object_meta.ObjectMeta("", Map.empty)),
    isLocal = true,
    identityProviderId = "",
  )

  val aParty = Ref.Party.assertFromString("aParty")

  val oneHour = FiniteDuration(1, java.util.concurrent.TimeUnit.HOURS)

  private final case class TestPartySyncService(tracer: Tracer) extends state.PartySyncService {
    override def allocateParty(
        hint: Ref.Party,
        submissionId: Ref.SubmissionId,
        synchronizerIdO: Option[SynchronizerId],
        externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anUserIdSpanAttribute._1,
        anUserIdSpanAttribute._2,
      )
      FutureUnlessShutdown.pure(state.SubmissionResult.Acknowledged)
    }

    override def protocolVersionForSynchronizerId(
        synchronizerId: SynchronizerId
    ): Option[ProtocolVersion] = Some(BaseTest.testedProtocolVersion)
  }
}
