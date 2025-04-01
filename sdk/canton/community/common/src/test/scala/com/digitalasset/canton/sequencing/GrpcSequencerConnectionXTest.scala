// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.SequencerConnectionXState
import com.digitalasset.canton.sequencing.protocol.{AcknowledgeRequest, SignedContent}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerConnectionXTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  "GrpcSequencerConnectionX" should {
    "have authentication hooks" in {
      val member = ParticipantId("test")

      val responses = TestResponses(
        apiResponses = Seq(correctApiResponse),
        handshakeResponses = Seq(successfulHandshake),
        synchronizerAndSeqIdResponses = Seq(correctSynchronizerIdResponse1),
        staticParametersResponses = Seq(correctStaticParametersResponse),
        acknowledgeResponses = Seq(positiveAcknowledgeResponse),
      )
      withConnection(responses) { case (internalConnection, listener) =>
        internalConnection.start().valueOrFail("start connection")

        listener.shouldStabilizeOn(SequencerConnectionXState.Validated)
        internalConnection.attributes shouldBe Some(correctConnectionAttributes)

        val connection =
          internalConnection
            .buildUserConnection(authConfig, testMember, testCrypto, wallClock)
            .valueOrFail("make authenticated")

        val acknowledgeRequest = AcknowledgeRequest(member, wallClock.now, testedProtocolVersion)
        val signedAcknowledgeRequest = SignedContent(
          acknowledgeRequest,
          SymbolicCrypto.emptySignature,
          None,
          testedProtocolVersion,
        )

        // The test stub checks that the call has the appropriate metadata suggesting the authenticating hooks are
        // properly set up. I haven't yet found a better way to test this because the hooks are actually exercised
        // only within the real gRPC implementation :-( .
        // At this point it seems a good-enough check, and not worth it to spend more time for better unit tests:
        // integration tests will quickly find issues if there are any.
        connection
          .acknowledgeSigned(signedAcknowledgeRequest, timeouts.network.unwrap)
          .valueOrFail("acknowledge")
          .futureValueUS shouldBe true

        responses.assertAllResponsesSent()
      }
    }
  }
}
