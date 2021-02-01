// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PublicKey

object SignatureData {

  def apply(
      algorithm: AlgorithmString,
      fingerprint: FingerprintBytes,
      key: PublicKey,
      signature: SignatureBytes,
  ): SignatureData =
    new SignatureData(algorithm, fingerprint, key, signature)

}

// Purposefully not a case class because it contains mutable arrays
final class SignatureData(
    val algorithm: AlgorithmString,
    val fingerprint: FingerprintBytes,
    val key: PublicKey,
    val signature: SignatureBytes,
) {
  def toSignedPayload(payload: Array[Byte]): SignedPayload =
    SignedPayload(
      algorithm = algorithm,
      fingerprint = fingerprint,
      payload = PayloadBytes.wrap(payload),
      signature = signature,
    )
}
