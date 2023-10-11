// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.ParameterValue
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.QueryPart
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ComposableQuerySpec extends AnyWordSpec with Matchers {
  import ComposableQuerySpec.TestInterpolation
  import ComposableQuery.SqlStringInterpolation

  "flattenComposite" should {

    "flatten correctly in a nested happy path case" in {
      test"a ${10} b ${cSQL"${11L} ${cSQL"${"12"}"}"} c" shouldBe (
        (
          List("a ", " b ", " ", " c"),
          List(
            ParameterValue.from(10),
            ParameterValue.from(11L),
            ParameterValue.from("12"),
          ),
        ),
      )
    }

    "flatten correctly with no interpolation" in {
      test"ac" shouldBe (
        (
          List("ac"),
          List(),
        ),
      )
    }

    "flatten correctly with empty string" in {
      test"" shouldBe (
        (
          List(""),
          List(),
        ),
      )
    }

    "flatten correctly with starting with a value" in {
      test"${1}ac" shouldBe (
        (
          List("", "ac"),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with ending with a value" in {
      test"ac${1}" shouldBe (
        (
          List("ac", ""),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with starting with a nested value which starts with value" in {
      test"${cSQL"${1}b"}ac" shouldBe (
        (
          List("", "bac"),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with ending with a nested value which ends with value" in {
      test"ac${cSQL"b${1}"}" shouldBe (
        (
          List("acb", ""),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with starting with a nested value which starts with string" in {
      test"${cSQL"d${1}b"}ac" shouldBe (
        (
          List("d", "bac"),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with ending with a nested value which ends with string" in {
      test"ac${cSQL"${1}b"}" shouldBe (
        (
          List("ac", "b"),
          List(
            ParameterValue.from(1)
          ),
        ),
      )
    }

    "flatten correctly with a multiple nested case" in {
      val simpleNested = cSQL"a ${1} b"
      val twoSimpleNested = cSQL"c ${2} d ${3} e"
      val twoOneLevelNested = cSQL"f $simpleNested g $simpleNested h"
      val oneTwoLevelNested = cSQL"i $twoOneLevelNested j"
      test"k $simpleNested l $twoSimpleNested m $twoOneLevelNested n $oneTwoLevelNested o" shouldBe (
        (
          List(
            "k a ",
            " b l c ",
            " d ",
            " e m f a ",
            " b g a ",
            " b h n i f a ",
            " b g a ",
            " b h j o",
          ),
          List(
            ParameterValue.from(1),
            ParameterValue.from(2),
            ParameterValue.from(3),
            ParameterValue.from(1),
            ParameterValue.from(1),
            ParameterValue.from(1),
            ParameterValue.from(1),
          ),
        ),
      )
    }
  }

}

object ComposableQuerySpec {

  implicit class TestInterpolation(val sc: StringContext) extends AnyVal {
    def test(args: QueryPart*): (Seq[String], Seq[ParameterValue]) =
      ComposableQuery.flattenComposite(sc.parts, args)
  }
}
