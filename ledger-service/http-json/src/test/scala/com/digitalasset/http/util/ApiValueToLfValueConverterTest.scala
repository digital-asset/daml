// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import com.daml.lf.data.{Numeric => LfNumeric}
import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.test.ValueGenerators.coidGen
import com.daml.lf.value.{Value => V}
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.Equal
import scalaz.syntax.bifunctor._
import scalaz.std.option._
import scalaz.std.tuple._

class ApiValueToLfValueConverterTest
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  import ApiValueToLfValueConverterTest._

  private[this] implicit val arbCid: Arbitrary[V.ContractId] = Arbitrary(coidGen)

  "apiValueToLfValue" should {
    import ApiValueToLfValueConverter.apiValueToLfValue

    "retract lfValueToApiValue" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj] = va.injarb
      forAll(minSuccessful(20)) { v: va.Inj =>
        val vv = va.inj(v)
        val roundTrip =
          lfValueToApiValue(true, vv).toOption flatMap (x => apiValueToLfValue(x).toMaybe.toOption)
        assert(Equal[Option[V]].equal(roundTrip, Some(vv)))
      }
    }
  }
}

object ApiValueToLfValueConverterTest {
  // Numeric are normalized when converting from api to lf,
  // them we have to relax numeric equality
  private implicit def eqValue: Equal[V] = { (l, r) =>
    V.`Value Equal instance`
      .contramap[V](
        mapNumeric(_, n => LfNumeric assertFromUnscaledBigDecimal n.stripTrailingZeros)
      )
      .equal(l, r)
  }

  private[this] def mapNumeric(fa: V, f: LfNumeric => LfNumeric): V = {
    def go(fa: V): V = fa match {
      case V.ValueNumeric(m) => V.ValueNumeric(f(m))
      case _: V.ValueCidlessLeaf | V.ValueContractId(_) => fa
      case r @ V.ValueRecord(_, fields) => r copy (fields = fields map (_ rightMap go))
      case v @ V.ValueVariant(_, _, value) => v copy (value = go(value))
      case V.ValueList(fs) => V.ValueList(fs map go)
      case V.ValueOptional(o) => V.ValueOptional(o map go)
      case V.ValueTextMap(m) => V.ValueTextMap(m mapValue go)
      case V.ValueGenMap(m) => V.ValueGenMap(m map (_.bimap(go, go)))
    }
    go(fa)
  }
}
