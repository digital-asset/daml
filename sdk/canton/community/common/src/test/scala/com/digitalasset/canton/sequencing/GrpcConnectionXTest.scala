// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXError, ConnectionXState}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import io.grpc.Status
import org.scalatest.wordspec.AnyWordSpec

class GrpcConnectionXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  "ConnectionX" should {
    lazy val stubFactory = new SequencerConnectionXStubFactoryImpl(loggerFactory)

    "notify on state changes" in {
      withLowLevelConnection() { (connection, listener) =>
        connection.start()
        listener.shouldStabilizeOn(ConnectionXState.Started)

        listener.clear()
        connection.stop()
        listener.shouldStabilizeOn(ConnectionXState.Stopped)
      }
    }

    "fail gRPC calls with invalid state if not started" in {
      withLowLevelConnection() { (connection, _) =>
        val stub = stubFactory.createStub(connection, MetricsContext.Empty)
        val result = stub.getApiName().futureValueUS

        inside(result) {
          case Left(
                SequencerConnectionXStubError.ConnectionError(
                  ConnectionXError.InvalidStateError(message)
                )
              ) =>
            message should include("Connection is not started")
        }
      }
    }

    "fail gRPC calls with gRPC error if there is no server" in {
      withLowLevelConnection() { (connection, _) =>
        connection.start()

        val stub = stubFactory.createStub(connection, MetricsContext.Empty)

        val result = loggerFactory.assertLogs(
          stub.getApiName().futureValueUS,
          _.warningMessage should include("Request failed"),
        )

        inside(result) {
          case Left(
                SequencerConnectionXStubError.ConnectionError(
                  ConnectionXError.TransportError(
                    GrpcServiceUnavailable(_, _, status, _, _)
                  )
                )
              ) =>
            status.getCode shouldBe Status.Code.UNAVAILABLE
        }
      }
    }
  }
}
