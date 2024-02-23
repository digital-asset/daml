// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.test.ValueGenerators.coidGen
import com.daml.lf.value.Value as V
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.lfValueToApiValue
import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ApiValueToLfValueConverterTest
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  private[this] implicit val arbCid: Arbitrary[V.ContractId] = Arbitrary(coidGen)

  "apiValueToLfValue" should {
    import com.digitalasset.canton.http.util.ApiValueToLfValueConverter.apiValueToLfValue

    "retract lfValueToApiValue" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj] = va.injarb
      forAll(minSuccessful(20)) { (v: va.Inj) =>
        val vv = va.inj(v)
        val roundTrip =
          lfValueToApiValue(true, vv).toOption flatMap (x => apiValueToLfValue(x).toMaybe.toOption)
        roundTrip shouldBe Some(vv)
      }
    }
  }
}
