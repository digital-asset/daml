// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model.converter

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ConverterSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {
  import Converter._

  "sequence" should {
    "satisfy identity, modulo list conversion" in forAll { xs: Vector[Int] =>
      sequence(xs map (Right(_))) shouldBe Right(xs.toList)
    }

    "report the last error encountered" in {
      sequence(Seq(Left(1), Right(2), Left(3))) shouldBe Left(3)
    }
  }
}
