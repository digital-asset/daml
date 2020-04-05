// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import com.daml.lf.data.ImmArray
import com.daml.lf.value.TypedValueGenerators.genAddend
import com.daml.lf.value.{Value => V}
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class ApiValueToLfValueConverterTest
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {

  private[this] val genCid = Gen.alphaStr map (t =>
    V.AbsoluteContractId.V0 assertFromString ('#' +: t))
  import ApiValueToLfValueConverterTest._

  "apiValueToLfValue" should {
    import ApiValueToLfValueConverter.apiValueToLfValue

    "retract lfValueToApiValue" in forAll(genAddend, minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb(Arbitrary(genCid))
      forAll(minSuccessful(20)) { v: va.Inj[Cid] =>
        val vv = va.inj(v)
        val roundTrip = lfValueToApiValue(true, vv).right.toOption flatMap (x =>
          apiValueToLfValue(x).toMaybe.toOption)
        assert(roundTrip === Some(vv))
      }
    }
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object ApiValueToLfValueConverterTest {

  import org.scalactic.Equality
  import org.scalactic.TripleEquals._

  type Cid = V.AbsoluteContractId

  private implicit def eqOption[X](implicit eq: Equality[X]): Equality[Option[X]] = {
    case (Some(x), Some(y)) => x === y
    case (x, y) => x == y
  }

  private implicit def eqImmArray[X](implicit eq: Equality[X]): Equality[ImmArray[X]] = {
    case (x, y: ImmArray[_]) => x.length == y.length && x.indices.forall(i => x(i) === y(i))
    case _ => false
  }

  private implicit def eqTuple[X, Y](
      implicit xEq: Equality[X],
      yEq: Equality[Y]): Equality[(X, Y)] = {
    case ((x1, y1), (x2, y2)) => x1 === x2 && y1 === y2
    case _ => false
  }

  // Numeric are normalized when converting from api to lf,
  // them we have to relax numeric equality
  private implicit def eqValue: Equality[V[Cid]] = {
    case (
        x @ (_: V.ValueInt64 | _: V.ValueText | _: V.ValueTimestamp | _: V.ValueParty |
        _: V.ValueBool | _: V.ValueDate | V.ValueUnit | _: V.ValueEnum | _: V.ValueContractId[_]),
        y) =>
      x == y
    case (V.ValueNumeric(m), V.ValueNumeric(n)) =>
      m.stripTrailingZeros == n.stripTrailingZeros
    case (V.ValueRecord(tycon1, fields1), V.ValueRecord(tycon2, fields2)) =>
      tycon1 == tycon2 && fields1 === fields2
    case (V.ValueVariant(tycon1, variant1, value1), V.ValueVariant(tycon2, variant2, value2)) =>
      tycon1 == tycon2 && variant1 === variant2 && value1 === value2
    case (V.ValueList(values), V.ValueList(values2)) =>
      values.toImmArray === values2.toImmArray
    case (V.ValueOptional(value1), V.ValueOptional(value2)) =>
      value1 === value2
    case (V.ValueStruct(fields), V.ValueStruct(fields2)) =>
      fields === fields2
    case (V.ValueTextMap(map1), V.ValueTextMap(map2)) =>
      map1.toImmArray === map2.toImmArray
    case _ =>
      false
  }

}
