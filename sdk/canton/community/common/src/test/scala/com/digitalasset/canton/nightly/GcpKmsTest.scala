// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.nightly

import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.kms.KmsError.KmsKeyDisabledError
import com.digitalasset.canton.crypto.kms.gcp.GcpKms
import com.digitalasset.canton.crypto.provider.kms.PredefinedGcpKmsKeys
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.FixtureAsyncWordSpec

class GcpKmsTest extends FixtureAsyncWordSpec with PredefinedGcpKmsKeys with ExternalKmsTest {
  override type KmsType = GcpKms

  override protected def defaultKmsConfig: KmsConfig.Gcp =
    KmsConfig.Gcp.defaultTestConfig

  override protected def newKms(config: KmsConfig.Gcp = defaultKmsConfig): GcpKms =
    GcpKms
      .create(config, timeouts, loggerFactory)
      .valueOrFail("create GCP KMS client")

  "GCP KMS" must {
    behave like kms()
    behave like externalKms(
      wrongKmsConfig = KmsConfig.Gcp(
        locationId = "wrong-region",
        projectId = "",
        keyRingId = "",
      )
    )

    "create MULTI-REGION symmetric key, encrypt/decrypt and delete that key" in { _ =>
      ResourceUtil.withResourceM(newKms(KmsConfig.Gcp.multiRegionTestConfig)) { kms =>
        for {
          keyIdMulti <- kms
            .generateSymmetricEncryptionKey()
            .valueOrFail("create KMS key")
            .failOnShutdown
          keyExists <- keyActivenessCheckAndDelete(kms, keyIdMulti).failOnShutdown
        } yield keyExists.left.value shouldBe a[KmsKeyDisabledError]
      }
    }
  }

}
