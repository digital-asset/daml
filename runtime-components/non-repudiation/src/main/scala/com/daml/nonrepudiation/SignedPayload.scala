// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

object SignedPayload {

  def apply(
      algorithm: AlgorithmString,
      fingerprint: FingerprintBytes,
      payload: PayloadBytes,
      signature: SignatureBytes,
  ): SignedPayload =
    new SignedPayload(algorithm, fingerprint, payload, signature)

}

// Purposefully not a case class because it contains mutable arrays
final class SignedPayload(
    val algorithm: AlgorithmString,
    val fingerprint: FingerprintBytes,
    val payload: PayloadBytes,
    val signature: SignatureBytes,
)
