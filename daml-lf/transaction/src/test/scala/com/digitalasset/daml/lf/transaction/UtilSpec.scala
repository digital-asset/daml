// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.value.ValueCoder
import com.daml.lf.value.test.ValueGenerators._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UtilSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  "normalize" should {

    "be equivalent to serialization followed by unserialization" in {
      forAll(valueGen(), transactionVersionGen()) { (v, version) =>
        val reference = for {
          encoded <-
            ValueCoder.encodeValue(ValueCoder.CidEncoder, version, v).left.map(_ => ())
          decoded <-
            ValueCoder.decodeValue(ValueCoder.CidDecoder, version, encoded).left.map(_ => ())
        } yield decoded

        Util.normalizeValue(v, version).left.map(_ => ()) shouldBe reference
      }
    }
  }

}
