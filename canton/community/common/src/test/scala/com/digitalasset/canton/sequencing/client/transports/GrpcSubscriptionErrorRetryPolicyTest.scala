// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.DefaultTestIdentities
import io.grpc.{Metadata, Status}
import org.scalatest.wordspec.AnyWordSpec

class GrpcSubscriptionErrorRetryPolicyTest extends AnyWordSpec with BaseTest {

  private val requestDescription = "request description"
  private val serverName = "sequencer"

  "GrpcSubscriptionErrorRetryRule" should {
    val tokenExpiredMetadata = new Metadata()
    tokenExpiredMetadata.put(
      Constant.AUTHENTICATION_ERROR_CODE,
      MemberAuthentication.MissingToken(DefaultTestIdentities.participant1).code,
    )

    val recoverableErrors = Seq(
      GrpcError(requestDescription, serverName, Status.UNAVAILABLE.asRuntimeException()),
      GrpcError(
        requestDescription,
        serverName,
        Status.UNAUTHENTICATED.asRuntimeException(tokenExpiredMetadata),
      ),
    )

    forEvery(recoverableErrors) { error =>
      s"retry if sequencer temporarily fails with ${error.status.getCode}" in {
        shouldRetry(error) shouldBe true
      }
    }

  }

  def shouldRetry(grpcError: GrpcError): Boolean = {
    val rule = new GrpcSubscriptionErrorRetryPolicy(loggerFactory)

    rule.retryOnError(GrpcSubscriptionError(grpcError), receivedItems = false)
  }
}
