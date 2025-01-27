// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.admin.participant.v30.TrafficControlStateRequest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.grpc.GrpcTrafficControlService
import com.digitalasset.canton.participant.admin.traffic.TrafficStateAdmin
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.DefaultTestIdentities
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

    "return traffic state for synchronizer" in {
      val (service, syncService) = setupTest
      val did = DefaultTestIdentities.synchronizerId
      val connectedSynchronizer = mock[ConnectedSynchronizer]
      when(syncService.readyConnectedSynchronizerById(did)).thenReturn(Some(connectedSynchronizer))
      val status = TrafficState(
        extraTrafficPurchased = NonNegativeLong.tryCreate(5),
        extraTrafficConsumed = NonNegativeLong.tryCreate(6),
        baseTrafficRemainder = NonNegativeLong.tryCreate(7),
        lastConsumedCost = NonNegativeLong.tryCreate(8),
        timestamp = CantonTimestamp.now(),
        serial = Some(PositiveInt.one),
      )
      when(connectedSynchronizer.getTrafficControlState).thenReturn(Future.successful(status))
      val response = timeouts.default.await("wait_for_response") {
        service.trafficControlState(TrafficControlStateRequest(did.toProtoPrimitive))
      }

      response.trafficState shouldBe Some(TrafficStateAdmin.toProto(status))
    }

    "return FAILED_PRECONDITION if the participant is not connected to the synchronizer" in {
      val (service, syncService) = setupTest
      val did = DefaultTestIdentities.synchronizerId
      when(syncService.readyConnectedSynchronizerById(did)).thenReturn(None)
      val response = the[StatusRuntimeException] thrownBy {
        timeouts.default.await("wait_for_response") {
          service.trafficControlState(TrafficControlStateRequest(did.toProtoPrimitive))
        }
      }
      response.getStatus.getCode.value() shouldBe io.grpc.Status.FAILED_PRECONDITION.getCode.value()
    }
  }
}
