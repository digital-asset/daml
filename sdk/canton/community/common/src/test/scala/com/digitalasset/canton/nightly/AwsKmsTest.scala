// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.nightly

import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.kms.KmsError.{KmsDeleteKeyError, KmsKeyDisabledError}
import com.digitalasset.canton.crypto.kms.aws.AwsKms
import com.digitalasset.canton.crypto.provider.kms.HasPredefinedAwsKmsKeys
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.slf4j.event.Level.INFO

class AwsKmsTest extends FixtureAsyncWordSpec with ExternalKmsTest with HasPredefinedAwsKmsKeys {
  override type KmsType = AwsKms

  override protected def defaultKmsConfig: KmsConfig.Aws =
    KmsConfig.Aws.defaultTestConfig

  override protected def newKms(config: KmsConfig.Aws = defaultKmsConfig) =
    AwsKms
      .create(
        config,
        timeouts,
        loggerFactory,
        NoReportingTracerProvider,
      )
      .valueOrFail("create AWS KMS client")

  "AWS KMS" must {
    behave like kms()
    behave like externalKms(
      wrongKmsConfig = KmsConfig.Aws(
        region = "wrong-region"
      )
    )

    "create MULTI-REGION symmetric key, encrypt/decrypt and delete that key" in { _ =>
      ResourceUtil.withResourceM(newKms(KmsConfig.Aws.multiRegionTestConfig)) { kms =>
        for {
          keyIdMulti <- kms
            .generateSymmetricEncryptionKey()
            .valueOrFail("create KMS key")
            .failOnShutdown
          keyExists <- keyActivenessCheckAndDelete(kms, keyIdMulti).failOnShutdown
        } yield keyExists.left.value shouldBe a[KmsKeyDisabledError]
      }
    }

    "log requests and responses with trace contexts" in { fixture =>
      ResourceUtil.withResourceM(newKms(KmsConfig.Aws.testConfigWithAudit)) { kms =>
        TraceContext.withNewTraceContext("awk_kms") { tc =>
          val traceId = tc.traceId.value

          def checkTraceId(le: LogEntry) = le.mdc.get("trace-id").value shouldBe traceId

          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(INFO))(
            for {
              // Run a request that will fail to test that we log on failed requests as well
              deleteKeyFailed <- kms
                .deleteKey(kmsKeyIdWrong)(executionContext, tc)
                .value
                .failOnShutdown

              // Run a successful encrypt / decrypt tests
              symmetricKeyId <- kmsSymmetricEncryptionKeyId.failOnShutdown
              _ <- encryptDecryptSymmetricTest(kms, symmetricKeyId, tc = tc).failOnShutdown
              signingKey <- kmsSigningKey.failOnShutdown
              _ <- signVerifyTest(fixture.pureCrypto, kms, signingKey, tc = tc).failOnShutdown
            } yield {
              kms.close()
              deleteKeyFailed.left.value shouldBe a[KmsDeleteKeyError]
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  le => {
                    le.infoMessage should (include(
                      // Make sure plaintext is not logged
                      "SignRequest"
                    ) and not include plainTextData)
                    checkTraceId(le)
                  },
                  "signing request",
                ),
                (
                  le => {
                    le.infoMessage should include(
                      "GetPublicKeyRequest"
                    )
                    checkTraceId(le)
                  },
                  "get public key request",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      "Received response"
                    ) and include("GetPublicKeyResponse") and not include plainTextData)
                    checkTraceId(le)
                  },
                  "get public key response",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      "Received response"
                    ) and include("SignResponse") and not include plainTextData)
                    checkTraceId(le)
                  },
                  "signing response",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      // Make sure plaintext is not logged
                      "EncryptRequest"
                    ) and not include plainTextData)
                    checkTraceId(le)
                  },
                  "encrypt request",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      "Received response"
                    ) and include("EncryptResponse") and not include plainTextData and include(
                      "** Ciphertext placeholder **"
                    ))
                    checkTraceId(le)
                  },
                  "encrypt response",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      // Make sure plaintext is not logged
                      "DecryptRequest"
                    ) and not include plainTextData and include(
                      "** Ciphertext placeholder **"
                    ))
                    checkTraceId(le)
                  },
                  "decrypt request",
                ),
                (
                  le => {
                    le.infoMessage should include(
                      // Make sure plaintext is not logged
                      "DeleteKeyRequest"
                    )
                    checkTraceId(le)
                  },
                  "delete key request",
                ),
                (
                  le => {
                    le.infoMessage should (include(
                      "Received response"
                    ) and include("DecryptResponse") and not include plainTextData)
                    checkTraceId(le)
                  },
                  "decrypt response",
                ),
                (
                  le => {
                    le.warningMessage should (include("Request") and include("failed"))
                    le.throwable.value.getMessage should include("Invalid keyId 'key_wrong_id'")
                    checkTraceId(le)
                  },
                  "failed delete response",
                ),
              )
            ),
          )
        }
      }
    }
  }
}
