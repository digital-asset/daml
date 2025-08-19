// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  SequencerConnectionXError,
  SequencerConnectionXState,
}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level.INFO

class GrpcInternalSequencerConnectionXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  import ConnectionPoolTestHelpers.*

  "GrpcInternalSequencerConnectionX" should {
    "be validated in the happy path" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
      )
      withConnection(responses) { (connection, listener) =>
        connection.start().valueOrFail("start connection")

        listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        responses.assertAllResponsesSent()
      }
    }

    "refuse to start if it is in a fatal state" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")

            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Failed handshake"),
                "Handshake fails",
              )
            )
          ),
        )

        // Try to restart
        inside(connection.start()) {
          case Left(SequencerConnectionXError.InvalidStateError(message)) =>
            message shouldBe "The connection is in a fatal state and cannot be started"
        }

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the returned API is not for a sequencer" in {
      val responses = TestResponses(
        apiResponses = Seq(incorrectApiResponse)
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Bad API"),
                "API response is invalid",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if the protocol handshake fails" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(failedHandshake),
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            connection.attributes shouldBe None
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Validation failure: Failed handshake"),
                "Protocol handshake fails",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }

    "fail validation if an expected sequencer ID is specified and it is incorrect" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
      )
      val expectedSequencerIdO = Some(testSequencerId(666))
      withConnection(responses, expectedSequencerIdO = expectedSequencerIdO) {
        (connection, listener) =>
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              connection.start().valueOrFail("start connection")
              listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
              connection.attributes shouldBe None
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.warningMessage should include(
                    s"Validation failure: Connection is not on expected sequencer:" +
                      s" expected $expectedSequencerIdO, got ${testSequencerId(1)}"
                  ),
                  "Protocol handshake fails",
                )
              )
            ),
          )

          responses.assertAllResponsesSent()
      }
    }

    "pass validation if an expected sequencer ID is specified and it is correct" in {
      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
      )
      val expectedSequencerIdO = Some(testSequencerId(1))
      withConnection(responses, expectedSequencerIdO = expectedSequencerIdO) {
        (connection, listener) =>
          connection.start().valueOrFail("start connection")

          listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
          connection.attributes shouldBe Some(correctConnectionAttributes)

          responses.assertAllResponsesSent()
      }
    }

    "retry if the server is unavailable during any request" in {
      val responses = TestResponses(
        apiResponses = Seq(failureUnavailable, correctApiResponse),
        handshakeResponses = Seq(failureUnavailable, successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(failureUnavailable, correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(failureUnavailable, correctStaticParametersResponse),
      )
      withConnection(responses) { (connection, listener) =>
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(INFO))(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
            connection.attributes shouldBe Some(correctConnectionAttributes)
          },
          forExactly(4, _) {
            _.infoMessage should include("Waiting for 1ms before retrying...")
          },
        )

        responses.assertAllResponsesSent()
      }
    }

    "validate the connection attributes after restart" in {
      val responses = TestResponses(
        apiResponses = Seq.fill(2)(correctApiResponse),
        handshakeResponses = Seq.fill(2)(successfulHandshake),
        synchronizerAndSeqIdResponses =
          Seq(correctSynchronizerIdResponse1, correctSynchronizerIdResponse2),
        staticParametersResponses = Seq.fill(2)(correctStaticParametersResponse),
      )
      withConnection(responses) { (connection, listener) =>
        connection.start().valueOrFail("start connection")
        listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
        connection.attributes shouldBe Some(correctConnectionAttributes)

        listener.clear()
        connection.fail("test")
        listener.shouldStabilizeOn(SequencerConnectionXState.Stopped)
        listener.clear()

        // A different identity triggers a warning and the connection never gets validated
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            connection.start().valueOrFail("start connection")
            listener.shouldStabilizeOn(SequencerConnectionXState.Fatal)
            // Synchronizer info does not change
            connection.attributes shouldBe Some(correctConnectionAttributes)
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Sequencer connection has changed attributes"),
                "Different attributes after restart",
              )
            )
          ),
        )

        responses.assertAllResponsesSent()
      }
    }
  }
}
