// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait RandomTest {
  this: AsyncWordSpec with BaseTest =>

  def randomnessProvider(providerF: => Future[RandomOps]): Unit = {
    "Randomness provider" should {
      "generate fresh randomness" in {

        providerF.map { provider =>
          val random1 = provider.generateRandomByteString(32)
          val random2 = provider.generateRandomByteString(32)

          random1 should not equal random2
        }
      }
    }
  }
}
