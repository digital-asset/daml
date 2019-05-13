// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import com.digitalasset.daml.lf.data.Ref.{DottedName, PackageId, Party, QualifiedName}
import org.scalatest.{FreeSpec, Matchers}

class RefTest extends FreeSpec with Matchers {
  "DottedName" - {
    "rejects bad segments" - {
      "digit at the start" in {
        DottedName.fromString("9test") shouldBe 'left
      }

      "bad symbols" in {
        DottedName.fromString("test%") shouldBe 'left
        DottedName.fromString("test-") shouldBe 'left
        DottedName.fromString("test@") shouldBe 'left
      }

      "unicode" in {
        DottedName.fromString("à") shouldBe 'left
        DottedName.fromString("ਊ") shouldBe 'left
      }

      "colon" in {
        DottedName.fromString("foo:bar") shouldBe 'left
      }
    }

    "rejects empty segments" in {
      DottedName.fromString(".") shouldBe 'left
      DottedName.fromString(".foo.") shouldBe 'left
      DottedName.fromString(".foo") shouldBe 'left
      DottedName.fromString("foo.") shouldBe 'left
      DottedName.fromString("foo..bar") shouldBe 'left
    }

    "accepts good segments" - {
      "dollar" in {
        DottedName
          .fromString("$.$blAH9.foo$bar.baz$")
          .getOrElse(sys.error("expect right found left"))
          .segments shouldBe
          ImmArray("$", "$blAH9", "foo$bar", "baz$")
      }

      "underscore" in {
        DottedName
          .fromString("_._blAH9.foo_bar.baz_")
          .getOrElse(sys.error("expect right found left"))
          .segments shouldBe
          ImmArray("_", "_blAH9", "foo_bar", "baz_")
      }
    }
  }

  "QualifiedName" - {
    "rejects no colon" in {
      QualifiedName.fromString("foo") shouldBe 'left
    }

    "rejects multiple colons" in {
      QualifiedName.fromString("foo:bar:baz") shouldBe 'left
    }

    "rejects empty dotted names" in {
      QualifiedName.fromString(":bar") shouldBe 'left
      QualifiedName.fromString("bar:") shouldBe 'left
    }
  }

  "Party and PackageId" - {

    val simpleChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "

    "accepts simple characters" in {
      for (c <- simpleChars) {
        Party.fromString(s"the character $c is simple") shouldBe 'right
        PackageId.fromString(s"the character $c is simple") shouldBe 'right
      }
    }

    "rejects the empty string" in {
      Party.fromString("") shouldBe 'left
      PackageId.fromString("") shouldBe 'left
    }

    "rejects non simple US-ASCII characters" in {
      for (c <- '\u0001' to '\u007f' if !simpleChars.contains(c)) {
        Party.fromString(s"the US-ASCII character $c is not simple") shouldBe 'left
        PackageId.fromString(s"the US-ASCII character $c is not simple") shouldBe 'left
      }
    }

    "rejects no US-ASCII characters" in {
      for (c <- '\u0080' to '\u00ff') {
        Party.fromString(s"the character $c is not US-ASCII") shouldBe 'left
        PackageId.fromString(s"the character $c is not US-ASCII") shouldBe 'left
      }
      for (s <- List(
          "español",
          "東京",
          "Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))"
        )) {
        Party.fromString(s) shouldBe 'left
        PackageId.fromString(s) shouldBe 'left
      }
    }
  }
}
