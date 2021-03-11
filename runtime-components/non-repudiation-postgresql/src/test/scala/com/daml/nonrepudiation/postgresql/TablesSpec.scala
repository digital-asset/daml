// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.time.Instant

import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.nonrepudiation.testing._
import com.daml.nonrepudiation.{
  AlgorithmString,
  FingerprintBytes,
  PayloadBytes,
  SignatureBytes,
  SignedPayload,
}
import com.daml.resources.{HasExecutionContext, ResourceOwnerFactories}
import com.daml.testing.postgresql.PostgresAroundEach
import doobie.util.log.LogHandler
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

final class TablesSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with PostgresAroundEach {

  behavior of "Tables"

  implicit val logHandler: LogHandler = Slf4jLogHandler(getClass)

  private val resourceFactory = new ResourceOwnerFactories[ExecutionContext] {
    override protected implicit val hasExecutionContext: HasExecutionContext[ExecutionContext] =
      HasExecutionContext.`ExecutionContext has itself`
  }

  it should "correctly read and write certificates" in {
    createTransactor(
      postgresDatabase.url,
      postgresDatabase.userName,
      postgresDatabase.password,
      maxPoolSize = 10,
      resourceFactory,
    ).use { transactor =>
      val db = Tables.initialize(transactor)
      val (_, expectedCertificate) = generateKeyAndCertificate()
      val fingerprint = db.certificates.put(expectedCertificate)
      val certificate = db.certificates.get(fingerprint)
      certificate.value.getEncoded shouldEqual expectedCertificate.getEncoded
    }
  }

  it should "guarantee that adding a certificate is idempotent" in {
    createTransactor(
      postgresDatabase.url,
      postgresDatabase.userName,
      postgresDatabase.password,
      maxPoolSize = 10,
      resourceFactory,
    ).use { transactor =>
      val db = Tables.initialize(transactor)
      val (_, expectedCertificate) = generateKeyAndCertificate()
      val fingerprint1 = db.certificates.put(expectedCertificate)
      val fingerprint2 = db.certificates.put(expectedCertificate)
      fingerprint1 shouldEqual fingerprint2
      val certificate = db.certificates.get(fingerprint1)
      certificate.value.getEncoded shouldEqual expectedCertificate.getEncoded
    }
  }

  it should "correctly read and write signed payloads" in {
    createTransactor(
      postgresDatabase.url,
      postgresDatabase.userName,
      postgresDatabase.password,
      maxPoolSize = 10,
      resourceFactory,
    ).use { transactor =>
      val db = Tables.initialize(transactor)
      val (privateKey, expectedCertificate) = generateKeyAndCertificate()

      val expectedTimestamp = Instant.ofEpochMilli(42)

      val expectedAlgorithm =
        AlgorithmString.SHA256withRSA

      val expectedPayload =
        PayloadBytes.wrap(generateCommand().toByteArray)

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
