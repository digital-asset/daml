// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import com.digitalasset.resources.FailingResourceOwner.FailingResourceFailedToOpen
import org.scalatest.{AsyncWordSpec, Matchers}

class ResourceSpec extends AsyncWordSpec with Matchers {
  "a Resource" should {
    "be used" in {
      val owner = ResourceOwner.successful(42)
      owner.acquire().use { value =>
        value should be(42)
      }
    }

    "clean up after use" in {
      val owner = TestResourceOwner(42)
      owner
        .acquire()
        .use { value =>
          owner.hasBeenAcquired should be(true)
          value should be(42)
        }
        .map { _ =>
          owner.hasBeenAcquired should be(false)
        }
    }

    "report errors in acquisition even after usage" in {
      val owner = FailingResourceOwner[Int]()
      owner
        .acquire()
        .use { _ =>
          fail("Can't use a failed resource.")
        }
        .failed
        .map { exception =>
          exception should be(a[FailingResourceFailedToOpen])
        }
    }

    "report errors in usage" in {
      val owner = TestResourceOwner(54)
      owner
        .acquire()
        .use { _ =>
          owner.hasBeenAcquired should be(true)
          sys.error("Uh oh.")
        }
        .failed
        .map { exception =>
          owner.hasBeenAcquired should be(false)
          exception.getMessage should be("Uh oh.")
        }
    }
  }
}
