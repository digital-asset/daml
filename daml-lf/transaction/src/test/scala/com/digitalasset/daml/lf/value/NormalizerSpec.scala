// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UtilSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  "normalize" should {

    "be equivalent to serialization followed by unserialization" in {
      forAll(test.ValueGenerators.valueGen, test.ValueGenerators.transactionVersionGen()) {
        (value, version) =>
          val reference = for {
            encoded <- ValueCoder.encodeValue(ValueCoder.CidEncoder, version, value)
            decoded <- ValueCoder.decodeValue(ValueCoder.CidDecoder, version, encoded)
          } yield decoded

          Util.normalize(value, version) shouldBe reference
      }
    }
  }

}
