// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ImplicitPreferenceSpec extends AnyWordSpec with Matchers {
  import ImplicitPreferenceSpec._

  "a single preference" should {
    implicit val elephant: Animal with ImplicitPreference[Any] =
      ImplicitPreference[Animal, Any](new Elephant)
    @annotation.unused("cat=unused&msg=val rhino")
    implicit val rhino: Animal = new Rhino

    "win out over the untagged one" in {
      implicitly[Animal] shouldBe an[Elephant]
    }
  }

  "two preferences" should {
    implicit val elephant: Animal with ImplicitPreference[Elephant] =
      ImplicitPreference[Animal, Elephant](new Elephant)
    @annotation.unused("cat=unused&msg=val rhino")
    implicit val rhino: Animal with ImplicitPreference[Animal] =
      ImplicitPreference[Animal, Animal](new Rhino)

    "select the more specific preference tparam" in {
      implicitly[Animal] shouldBe an[Elephant]
    }
  }
}

object ImplicitPreferenceSpec {
  class Animal
  class Elephant extends Animal
  class Rhino extends Animal
}
