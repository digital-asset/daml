// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import com.digitalasset.canton.config
import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.crypto.kms.driver.api.v1.{KmsDriver, SigningAlgoSpec, SigningKeySpec}
import com.digitalasset.canton.crypto.kms.driver.testing.v1.KmsDriverTest

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps

class MockKmsDriverTest extends KmsDriverTest {

  private def newKmsDriverWithConfig(config: MockKmsDriverConfig): MockKmsDriver = {
    val factory = new MockKmsDriverFactory
    factory.create(config, simpleLoggerFactory, parallelExecutionContext)
  }

  override protected def newKmsDriver(): KmsDriver =
    newKmsDriverWithConfig(MockKmsDriverConfig())

  private lazy val signingLatencies =
    Map(
      "namespace" -> 2.seconds,
      "signing" -> 1.second,
    )

  "Mock KMS Driver" must {
    behave like kmsDriver(allowKeyGeneration = true)

    forAll(signingLatencies) { case (keyName, signingLatency) =>
      s"simulate signing latencies of $signingLatency for $keyName" in {
        val testData = "test".getBytes

        val driverConfig =
          MockKmsDriverConfig(signingLatencies =
            Map(KeyName.tryCreate(keyName) -> config.NonNegativeFiniteDuration(signingLatency))
          )

        val driver = newKmsDriverWithConfig(driverConfig)

        val beforeAndAfterF = for {
          keyId <- driver.generateSigningKeyPair(
            signingKeySpec = SigningKeySpec.EcP256,
            keyName = Some(keyName),
          )(emptyContext)

          before = wallClock.now
          _ <- driver.sign(testData, keyId, SigningAlgoSpec.EcDsaSha256)(emptyContext)
          after = wallClock.now
        } yield (before, after)

        beforeAndAfterF.map { case (before, after) =>
          val measuredSigningLatency = after - before
          assert(measuredSigningLatency.toScala >= signingLatency)
        }
      }
    }
  }

}
