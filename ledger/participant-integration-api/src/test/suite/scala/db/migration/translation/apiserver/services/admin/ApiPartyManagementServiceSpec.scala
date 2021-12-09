// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}
import akka.stream.scaladsl.Source
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.domain.LedgerOffset.Absolute
import com.daml.ledger.api.domain.{PartyDetails, PartyEntry}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.party_management_service.AllocatePartyRequest
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexTransactionsService,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.admin.ApiPartyManagementServiceSpec._
import com.daml.telemetry.TelemetrySpecBase._
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ApiPartyManagementServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val errorCodesVersionSwitcher: ErrorCodesVersionSwitcher = mock[ErrorCodesVersionSwitcher]

  "ApiPartyManagementService" should {
    "propagate trace context" in {
      val mockIndexTransactionsService = mock[IndexTransactionsService]
      when(mockIndexTransactionsService.currentLedgerEnd())
        .thenReturn(Future.successful(Absolute(Ref.LedgerString.assertFromString("0"))))

      val mockIndexPartyManagementService = mock[IndexPartyManagementService]
      when(
        mockIndexPartyManagementService.partyEntries(any[Option[Absolute]])(any[LoggingContext])
      )
        .thenReturn(
          Source.single(
            PartyEntry.AllocationAccepted(
              Some("aSubmission"),
              PartyDetails(Ref.Party.assertFromString("aParty"), None, isLocal = true),
            )
          )
        )

      val apiService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockIndexTransactionsService,
        TestWritePartyService,
        Duration.ZERO,
        errorCodesVersionSwitcher,
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
          verifyZeroInteractions(errorCodesVersionSwitcher)
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
