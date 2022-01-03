// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValueSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import ValueGen._

  "Value" should {
    "read(write(x)) yields x" in forAll { wv: Exists[WithValue] =>
      val (vc, t) = wv.run
      import vc._
      (vc.tName, Value.Decoder[wv.T].read(Value.Encoder[wv.T].write(t))) shouldBe (
        (
          vc.tName,
          Some(t),
        ),
      )
    }

    "be found for lists" in {
      import com.daml.ledger.client.binding.{Primitive => P}
      Value[P.List[P.Int64]]
      Value.Decoder[P.List[P.Int64]]
      Value.Encoder[P.List[P.Int64]]
    }
    "encode Numeric without exponents" in {
      import com.daml.ledger.client.binding.{Primitive => P}
      import com.daml.ledger.api.v1.value.Value.{Sum => VSum}
      import com.daml.ledger.api.v1.value.{Value => RpcValue}
      Value.encode(BigDecimal.exact("0.0000000000001"): P.Numeric) shouldBe RpcValue(
        VSum.Numeric("0.0000000000001")
      )
    }
    "fail to decode Numeric with exponent" in {
      import com.daml.ledger.client.binding.{Primitive => P}
      import com.daml.ledger.api.v1.value.Value.{Sum => VSum}
      import com.daml.ledger.api.v1.value.{Value => RpcValue}
      Value.decode[P.Numeric](RpcValue(VSum.Numeric("1E-13"))) shouldBe None
    }
  }
}
