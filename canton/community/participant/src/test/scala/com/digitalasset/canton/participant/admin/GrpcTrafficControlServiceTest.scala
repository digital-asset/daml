// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.admin.participant.v30.TrafficControlStateRequest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.grpc.GrpcTrafficControlService
import com.digitalasset.canton.participant.sync.{CantonSyncService, SyncDomain}
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.traffic.{MemberTrafficStatus, TopUpEvent}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.StatusRuntimeException
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class GrpcTrafficControlServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {

  "GrpcTrafficControlServiceTest" should {

    def setupTest = {
      val syncService = mock[CantonSyncService]
      val service =
        new GrpcTrafficControlService(syncService, loggerFactory)(parallelExecutionContext)
      (service, syncService)
    }

    "return traffic state for domain" in {
      val (service, syncService) = setupTest
      val did = DefaultTestIdentities.domainId
      val syncDomain = mock[SyncDomain]
      when(syncService.readySyncDomainById(did)).thenReturn(Some(syncDomain))
      val status = MemberTrafficStatus(
        DefaultTestIdentities.participant1,
        CantonTimestamp.now(),
        SequencedEventTrafficState(NonNegativeLong.tryCreate(3), NonNegativeLong.tryCreate(7)),
        List(TopUpEvent(PositiveLong.tryCreate(9), CantonTimestamp.now(), PositiveInt.tryCreate(5))),
      )
      when(syncDomain.getTrafficControlState).thenReturn(Future.successful(Some(status)))
      val response = timeouts.default.await("wait_for_response") {
        service.trafficControlState(TrafficControlStateRequest(did.toProtoPrimitive))
      }
      response.trafficState shouldBe Some(status.toProtoV30)
    }

    "return FAILED_PRECONDITION if the domain is not found" in {
      val (service, syncService) = setupTest
      val did = DefaultTestIdentities.domainId
      when(syncService.readySyncDomainById(did)).thenReturn(None)
      val response = the[StatusRuntimeException] thrownBy {
        timeouts.default.await("wait_for_response") {
          service.trafficControlState(TrafficControlStateRequest(did.toProtoPrimitive))
        }
      }
      response.getStatus.getCode.value() shouldBe io.grpc.Status.FAILED_PRECONDITION.getCode.value()
    }

    "return NOT_FOUND if traffic state is not available" in {
      val (service, syncService) = setupTest
      val did = DefaultTestIdentities.domainId
      val syncDomain = mock[SyncDomain]
      when(syncService.readySyncDomainById(did)).thenReturn(Some(syncDomain))
      when(syncDomain.getTrafficControlState).thenReturn(Future.successful(None))
      val response = the[StatusRuntimeException] thrownBy {
        timeouts.default.await("wait_for_response") {
          service.trafficControlState(TrafficControlStateRequest(did.toProtoPrimitive))
        }
      }
      response.getStatus.getCode.value() shouldBe io.grpc.Status.NOT_FOUND.getCode.value()
    }
  }
}
