// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.testing.v1

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.kms.driver.api.v1.{KmsDriverFactory, KmsDriverHealth}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import scala.concurrent.duration.{FiniteDuration, *}

trait KmsDriverFactoryTest extends AnyWordSpec with BaseTest {

  type Factory <: KmsDriverFactory

  /** Timeout for the driver to report healthy */
  protected val driverHealthyTimeout: FiniteDuration = 60.seconds

  /** Create a new specific KMS Driver Factory instance */
  protected val factory: Factory

  /** A valid KMS Driver specific config */
  protected val config: factory.ConfigType

  def kmsDriverFactory(): Unit = {

    def simpleLoggerFactory(clazz: Class[_]): Logger =
      loggerFactory.getLogger(clazz).underlying

    "KMS Driver Factory" must {

      "create new driver" in {
        val driver = factory.create(config, simpleLoggerFactory, directExecutionContext)

        eventually(driverHealthyTimeout) {
          driver.health.futureValue shouldBe a[KmsDriverHealth.Ok.type]
        }
      }

      "driver config confidential writing" in {
        factory.configWriter(confidential = true).to(config)
      }

      "driver config writing and reading" in {
        factory.configReader
          .from(factory.configWriter(confidential = false).to(config))
          .valueOrFail("failed to parse written config") shouldEqual config
      }

    }
  }

}
