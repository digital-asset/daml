// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.health.{HealthElement, HealthListener}
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXError, ConnectionXState}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import io.grpc.Status
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.blocking
import scala.concurrent.duration.DurationInt

class GrpcConnectionXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  "ConnectionX" should {
    lazy val stubFactory = SequencerConnectionXStubFactoryImpl

    "notify on state changes" in {
      ResourceUtil.withResource(mkConnection()) { connection =>
        val listener = new TestHealthListener(connection.health)
        connection.health.registerOnHealthChange(listener)

        connection.start()
        listener.shouldStabilizeOn(ConnectionXState.Started)

        listener.clear()
        connection.stop()
        listener.shouldStabilizeOn(ConnectionXState.Stopped)
      }
    }

    "fail gRPC calls with invalid state if not started" in {
      ResourceUtil.withResource(mkConnection()) { connection =>
        val stub = stubFactory.createStub(connection)
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
      ResourceUtil.withResource(mkConnection()) { connection =>
        connection.start()

        val stub = stubFactory.createStub(connection)

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

  private def mkConnection(): ConnectionX = {
    val config = mkDummyConnectionConfig(0)

    GrpcConnectionX(
      config,
      timeouts,
      loggerFactory,
    )
  }
}

class TestHealthListener(val element: HealthElement) extends HealthListener with Matchers {
  import scala.collection.mutable
  import BaseTest.eventuallyForever

  private val statesBuffer = mutable.ArrayBuffer[element.State]()

  def shouldStabilizeOn[T](state: T): Assertion =
    // Check that we reach the given state, and remain on it
    // The default 2 seconds is a bit short when machines are under heavy load
    eventuallyForever(timeUntilSuccess = 10.seconds) {
      statesBuffer.last shouldBe state
    }

  def clear(): Unit = statesBuffer.clear()

  override def name: String = s"${element.name}-test-listener"

  override def poke()(implicit traceContext: TraceContext): Unit = blocking {
    synchronized {
      val state = element.getState

      statesBuffer += state
    }
  }
}
