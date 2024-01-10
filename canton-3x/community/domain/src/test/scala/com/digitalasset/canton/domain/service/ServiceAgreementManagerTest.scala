// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service

import better.files.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.ParticipantId
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ServiceAgreementManagerTest extends AsyncWordSpec with BaseTest {

  private def withServiceAgreementManager(
      testCode: ServiceAgreementManager => Future[Assertion]
  ): Future[Assertion] = {
    val tmpf = File.newTemporaryFile()

    try {
      tmpf.write("test")
      val hasher = new SymbolicPureCrypto
      val sam =
        ServiceAgreementManager.create(
          tmpf,
          new MemoryStorage(loggerFactory, timeouts),
          hasher,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      sam.fold(fail(_), testCode(_))
    } finally tmpf.delete()
  }

  "InMemoryServiceAgreementManager" should {

    "return service agreement from file" in withServiceAgreementManager { sam =>
      sam.agreement.text shouldBe "test"
    }

    "insert acceptance" in withServiceAgreementManager { sam =>
      val agreementId = sam.agreement.id
      val participantId = ParticipantId("p1")
      val signature = SymbolicCrypto.signature(ByteString.EMPTY, Fingerprint.tryCreate("p1"))
      val timestamp = CantonTimestamp.Epoch

      for {
        preAcceptances <- sam.listAcceptances()
        _ <- sam.insertAcceptance(agreementId, participantId, signature, timestamp)
        postAcceptances <- sam.listAcceptances()
      } yield {
        preAcceptances should have size 0
        postAcceptances should contain(
          ServiceAgreementAcceptance(agreementId, participantId, signature, timestamp)
        )
      }
    }

    "fail when creating with an invalid file" in {
      val hasher = new SymbolicPureCrypto
      val file = "/tmp/canton-invalid-file-for-testing"
      val sam =
        ServiceAgreementManager.create(
          File(file),
          new MemoryStorage(loggerFactory, timeouts),
          hasher,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      sam shouldBe Left(
        s"Unable to load service agreement file: java.nio.file.NoSuchFileException: $file"
      )
    }

  }

}
