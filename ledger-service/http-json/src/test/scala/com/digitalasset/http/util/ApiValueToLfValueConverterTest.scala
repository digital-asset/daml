// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import com.daml.lf.data.{Numeric => LfNumeric}
import com.daml.lf.value.test.TypedValueGenerators.genAddend
import com.daml.lf.value.{Value => V}
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import scalaz.Equal
import scalaz.syntax.bifunctor._
import scalaz.std.option._
import scalaz.std.tuple._

class ApiValueToLfValueConverterTest
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ApiValueToLfValueConverterTest._

  private[this] implicit val arbCid: Arbitrary[CidSrc] = Arbitrary(
    Gen.alphaStr map (t => V.ContractId.V0 assertFromString ('#' +: t)))

  "apiValueToLfValue" should {
    import ApiValueToLfValueConverter.apiValueToLfValue

    "retract lfValueToApiValue" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj[CidSrc]] = va.injarb
      forAll(minSuccessful(20)) { v: va.Inj[CidSrc] =>
        val vv = va.inj(v)
        val roundTrip = lfValueToApiValue(true, vv).right.toOption flatMap (x =>
          apiValueToLfValue(x).toMaybe.toOption)
        assert(Equal[Option[V[Cid]]].equal(roundTrip, Some(vv)))
      }
    }
  }
}

object ApiValueToLfValueConverterTest {

  type Cid = V.ContractId
  type CidSrc = V.ContractId.V0

  // Numeric are normalized when converting from api to lf,
  // them we have to relax numeric equality
  private implicit def eqValue: Equal[V[Cid]] = { (l, r) =>
    V.`Value Equal instance`[Cid]
      .contramap[V[Cid]](
        mapNumeric(_, n => LfNumeric assertFromUnscaledBigDecimal n.stripTrailingZeros))
      .equal(l, r)
  }

  private[this] def mapNumeric[C](fa: V[C], f: LfNumeric => LfNumeric): V[C] = {
    def go(fa: V[C]): V[C] = fa match {
      case V.ValueNumeric(m) => V.ValueNumeric(f(m))
      case _: V.ValueCidlessLeaf | V.ValueContractId(_) => fa
      case r @ V.ValueRecord(_, fields) => r copy (fields = fields map (_ rightMap go))
      case v @ V.ValueVariant(_, _, value) => v copy (value = go(value))
      case s @ V.ValueStruct(fields) => s copy (fields = fields mapValues go)
      case V.ValueList(fs) => V.ValueList(fs map go)
      case V.ValueOptional(o) => V.ValueOptional(o map go)
      case V.ValueTextMap(m) => V.ValueTextMap(m mapValue go)
      case V.ValueGenMap(m) => V.ValueGenMap(m map (_ bimap (go, go)))
    }
    go(fa)
  }
}
