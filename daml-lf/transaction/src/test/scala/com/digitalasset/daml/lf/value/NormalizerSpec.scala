// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.Ordering.Implicits.infixOrderingOps

class UtilSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  "normalize" should {

    "be equivalent to serialization followed by unserialization" in {
      forAll(test.ValueGenerators.valueGen, test.ValueGenerators.transactionVersionGen())(
        (value, version) =>
          whenever(transaction.test.TransactionBuilder.assertAssignVersion(value) <= version) {
            val Right(encoded) = ValueCoder.encodeValue(ValueCoder.CidEncoder, version, value)
            val Right(decoded) = ValueCoder.decodeValue(ValueCoder.CidDecoder, version, encoded)

            Util.normalize(value, version) should be(decoded)
          }
      )
    }
  }

}
