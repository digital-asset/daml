// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import com.digitalasset.canton.BaseTestWordSpec
import org.scalatest.wordspec.AnyWordSpec

class SingletonTraverseTest extends AnyWordSpec with BaseTestWordSpec {
  "Laws for Id" should {
    checkAllLaws(
      "Id",
      SingletonTraverseTests[Id].singletonTraverse[Int, Int, Int, Int, Option, Option],
    )
  }

  "Laws for Option" should {
    checkAllLaws(
      "Option",
      SingletonTraverseTests[Option].singletonTraverse[Int, Int, Int, Int, Option, Option],
    )
  }

  "Laws for Tuple2[String, *]" should {
    checkAllLaws(
      "Tuple2",
      SingletonTraverseTests[(String, *)].singletonTraverse[Int, Int, Int, Int, Option, Option],
    )
  }

  "Laws for Either[String, *]" should {
    checkAllLaws(
      "Either",
      SingletonTraverseTests[Either[String, *]]
        .singletonTraverse[Int, Int, Int, Int, Option, Option],
    )
  }
}
