// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api.v1

import java.time.Instant

import com.daml.nonrepudiation.{
  AlgorithmString,
  FingerprintBytes,
  PayloadBytes,
  SignatureBytes,
  SignedPayload,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class SignedPayloadsEndpointSpec extends AnyFlatSpec with Matchers {

  behavior of "SignedPayloadEndpoint.toResponse"

  it should "convert the backend signed payload into the expected representation" in {

    val algorithm = "algorithm"
    val fingerprint = "fingerprint".getBytes
    val payload = "payload".getBytes
    val signature = "signature".getBytes
    val timestamp = 42L

    val signedPayload =
      SignedPayload(
        algorithm = AlgorithmString.wrap(algorithm),
        fingerprint = FingerprintBytes.wrap(fingerprint),
        payload = PayloadBytes.wrap(payload),
        signature = SignatureBytes.wrap(signature),
        timestamp = Instant.ofEpochMilli(timestamp),
      )

    SignedPayloadsEndpoint.toResponse(signedPayload) shouldBe SignedPayloadsEndpoint.Response(
      algorithm = algorithm,
      fingerprint = "ZmluZ2VycHJpbnQ=", // URL-safe base64 encoded "fingerprint"
      payload = "cGF5bG9hZA==", // URL-safe base64 encoded "payload"
      signature = "c2lnbmF0dXJl", // URL-safe base64 encoded "signature"
      timestamp = timestamp,
    )

  }

}
