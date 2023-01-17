// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.util.concurrent.{CompletableFuture, CompletionStage}
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain.LedgerOffset.Absolute
import com.daml.ledger.api.domain.{IdentityProviderId, ObjectMeta}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
  IndexerPartyDetails,
  PartyEntry,
}
import com.daml.ledger.api.v1.admin.party_management_service.{PartyDetails => ProtoPartyDetails}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.admin.ApiPartyManagementService.blindAndConvertToProto
import com.daml.platform.apiserver.services.admin.ApiPartyManagementServiceSpec._
import com.daml.platform.localstore.api.{PartyRecord, PartyRecordStore}
import com.daml.tracing.TelemetrySpecBase._
import com.daml.tracing.{TelemetryContext, TelemetrySpecBase}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class ApiPartyManagementServiceSpec
    extends AnyWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ScalaFutures
    with ArgumentMatchersSugar
    with IntegrationPatience
    with AkkaBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val ec = ExecutionContext.global

  "ApiPartyManagementService" should {
    def blind(
        idpId: IdentityProviderId,
        partyDetails: IndexerPartyDetails,
        partyRecord: Option[PartyRecord],
    ): ProtoPartyDetails =
      blindAndConvertToProto(idpId)((partyDetails, partyRecord))

    "translate basic input to the output" in new TestScope {
      blind(IdentityProviderId.Default, partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
    }

    "blind identity_provider_id for non default IDP" in new TestScope {
      blind(IdentityProviderId("idp_1"), partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "blind identity_provider_id if record is for non default IDP" in new TestScope {
      blind(
        IdentityProviderId.Default,
        partyDetails,
        Some(partyRecord.copy(identityProviderId = IdentityProviderId("idp_1"))),
      ) shouldBe protoPartyDetails.copy(identityProviderId = "")
    }

    "not blind `isLocal` if local record does not exist" in new TestScope {
      blind(IdentityProviderId.Default, partyDetails, None) shouldBe protoPartyDetails
    }

    "blind `isLocal` if local record does not exist for non default IDP" in new TestScope {
      blind(IdentityProviderId("idp_1"), partyDetails, None) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "propagate trace context" in {
      val mockIndexTransactionsService = mock[IndexTransactionsService]
      val mockPartyRecordStore = mock[PartyRecordStore]
      val mockIdentityProviderExists = mock[IdentityProviderExists]
      when(mockIndexTransactionsService.currentLedgerEnd())
        .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))
      when(mockIdentityProviderExists.apply(IdentityProviderId.Default))
        .thenReturn(Future.successful(true))
      val mockIndexPartyManagementService = mock[IndexPartyManagementService]
      val party = Ref.Party.assertFromString("aParty")
      when(
        mockIndexPartyManagementService.partyEntries(any[Option[Absolute]])(any[LoggingContext])
      )
        .thenReturn(
          Source.single(
            PartyEntry.AllocationAccepted(
              Some("aSubmission"),
              IndexerPartyDetails(party, None, isLocal = true),
            )
          )
        )
      when(
        mockPartyRecordStore.createPartyRecord(any[PartyRecord])(any[LoggingContext])
      ).thenReturn(
        Future.successful(
          Right(PartyRecord(party, ObjectMeta.empty, IdentityProviderId.Default))
        )
      )
      when(
        mockPartyRecordStore.getPartyRecordO(any[Ref.Party])(any[LoggingContext])
      ).thenReturn(Future.successful(Right(None)))

      val apiService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockIdentityProviderExists,
        mockPartyRecordStore,
        mockIndexTransactionsService,
        TestWritePartyService,
        Duration.Zero,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
      )

      val span = anEmptySpan()
      val scope = span.makeCurrent()
      apiService
        .allocateParty(AllocatePartyRequest("aParty"))
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .futureValue

      spanExporter.finishedSpanAttributes should contain(anApplicationIdSpanAttribute)
    }
  }
}

object ApiPartyManagementServiceSpec {
  trait TestScope {
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
  }

  private object TestWritePartyService extends state.WritePartyService {
    override def allocateParty(
        hint: Option[Ref.Party],
        displayName: Option[String],
        submissionId: Ref.SubmissionId,
    )(implicit
        loggingContext: LoggingContext,
        telemetryContext: TelemetryContext,
    ): CompletionStage[state.SubmissionResult] = {
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
