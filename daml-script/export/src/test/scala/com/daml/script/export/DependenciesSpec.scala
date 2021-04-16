// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.language.LanguageVersion._

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class DependenciesSpec extends AnyFreeSpec with Matchers {
  import Dependencies._

  "targetLfVersion" - {
    "empty Seq" in {
      targetLfVersion(Seq.empty) shouldBe None
    }
    "single DALF" in {
      targetLfVersion(Seq(v1_11)) shouldBe Some(v1_11)
    }
    "multiple DALFs" in {
      targetLfVersion(Seq(v1_8, v1_11, v1_12)) shouldBe Some(v1_12)
    }
  }
  "targetFlag" - {
    "1.12" in {
      targetFlag(v1_12) shouldBe "--target=1.12"
    }
    "1.dev" in {
      targetFlag(v1_dev) shouldBe "--target=1.dev"
    }
  }
}
