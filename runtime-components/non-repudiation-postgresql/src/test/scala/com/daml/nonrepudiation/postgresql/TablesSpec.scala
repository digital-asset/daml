// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.time.Instant

import com.daml.ledger.resources.ResourceContext
import com.daml.nonrepudiation.testing._
import com.daml.nonrepudiation.{
  AlgorithmString,
  FingerprintBytes,
  PayloadBytes,
  SignatureBytes,
  SignedPayload,
}
import com.daml.testing.postgresql.PostgresAroundEach
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

final class TablesSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with PostgresAroundEach {

  behavior of "Tables"

  private implicit val context: ResourceContext = ResourceContext(executionContext)

  it should "correctly read and write certificates" in {
    initializeDatabase(postgresDatabase.url, maxPoolSize = 10).use { db =>
      val (_, expectedCertificate) = generateKeyAndCertificate()
      val fingerprint = db.certificates.put(expectedCertificate)
      val certificate = db.certificates.get(fingerprint)
      certificate.value.getEncoded shouldEqual expectedCertificate.getEncoded
    }
  }

  it should "correctly read and write signed payloads" in {
    initializeDatabase(postgresDatabase.url, maxPoolSize = 10).use { db =>
      val (privateKey, expectedCertificate) = generateKeyAndCertificate()

      val expectedTimestamp = Instant.ofEpochMilli(42)

      val expectedAlgorithm =
        AlgorithmString.SHA256withRSA

      val expectedPayload =
        PayloadBytes.wrap(generateCommand("hello, world").toByteArray)

      val expectedKey =
        db.signedPayloads.keyEncoder.encode(expectedPayload)

      val expectedFingerprint =
        FingerprintBytes.compute(expectedCertificate)

      val expectedSignature =
        SignatureBytes.sign(
          expectedAlgorithm,
          privateKey,
          expectedPayload,
        )

      val expectedSignedPayload =
        SignedPayload(
          expectedAlgorithm,
          expectedFingerprint,
          expectedPayload,
          expectedSignature,
          expectedTimestamp,
        )

      db.signedPayloads.put(expectedSignedPayload)

      val result = db.signedPayloads.get(expectedKey)

      result should contain only expectedSignedPayload

    }

  }

}
