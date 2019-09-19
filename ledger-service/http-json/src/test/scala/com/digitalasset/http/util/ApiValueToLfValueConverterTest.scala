// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.TypedValueGenerators.genAddend
import com.digitalasset.platform.participant.util.LfEngineToApi.lfValueToApiValue

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import scalaz.\/-

class ApiValueToLfValueConverterTest
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  type Cid = V.AbsoluteContractId
  private[this] val genCid = Gen.zip(Gen.alphaChar, Gen.alphaStr) map {
    case (h, t) => V.AbsoluteContractId(Ref.ContractIdString assertFromString (h +: t))
  }

  "apiValueToLfValue" should {
    import ApiValueToLfValueConverter.apiValueToLfValue

    "retract lfValueToApiValue" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid))
      forAll(minSuccessful(20)) { v: va.Inj[Cid] =>
        val vv = va.inj(v)
        lfValueToApiValue(true, vv) map apiValueToLfValue should ===(Right(\/-(vv)))
      }
    }
  }
}
