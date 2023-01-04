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
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.admin.ApiPartyManagementServiceSpec._
import com.daml.platform.localstore.api.{PartyRecord, PartyRecordStore}
import com.daml.telemetry.TelemetrySpecBase._
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class ApiPartyManagementServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "ApiPartyManagementService" should {
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
        .map { _ =>
          spanExporter.finishedSpanAttributes should contain(anApplicationIdSpanAttribute)
          succeed
        }
    }
  }
}

object ApiPartyManagementServiceSpec {
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
