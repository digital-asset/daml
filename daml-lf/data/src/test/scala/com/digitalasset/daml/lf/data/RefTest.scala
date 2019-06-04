// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import com.digitalasset.daml.lf.data.Ref.{DottedName, LedgerString, PackageId, Party, QualifiedName}
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

    val packageIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
    val partyIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:-_ "
    val ledgerStringChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-# "

    def makeString(c: Char): String = s"the character $c is not US-ASCII"

    "rejects the empty string" in {
      PackageId.fromString("") shouldBe 'left
      Party.fromString("") shouldBe 'left
      LedgerString.fromString("") shouldBe 'left
    }

    "treats US-ASCII characters as expected" in {
      for (c <- '\u0001' to '\u007f') {
        val s = makeString(c)
        PackageId.fromString(s) shouldBe (if (packageIdChars.contains(c)) 'right else 'left)
        Party.fromString(s) shouldBe (if (partyIdChars.contains(c)) 'right else 'left)
        LedgerString.fromString(s) shouldBe (if (ledgerStringChars.contains(c)) 'right else 'left)
      }
    }

    "rejects no US-ASCII characters" in {

      val negativeTestCase = makeString('a')

      PackageId.fromString(negativeTestCase) shouldBe 'right
      Party.fromString(negativeTestCase) shouldBe 'right
      LedgerString.fromString(negativeTestCase) shouldBe 'right

      for (c <- '\u0080' to '\u00ff') {
        val positiveTestCase = makeString(c)
        PackageId.fromString(positiveTestCase) shouldBe 'left
        Party.fromString(positiveTestCase) shouldBe 'left
        LedgerString.fromString(positiveTestCase) shouldBe 'left
      }
      for (positiveTestCase <- List(
          "español",
          "東京",
          "Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))"
        )) {
        Party.fromString(positiveTestCase) shouldBe 'left
        PackageId.fromString(positiveTestCase) shouldBe 'left
      }
    }

    "LedgerString should reject too long strings" in {
      val negativeTestCase = "a" * 255
      val positiveTestCase1 = "a" * 256
      val positiveTestCase2 = "a" * 500
      LedgerString.fromString(negativeTestCase) shouldBe 'right
      LedgerString.fromString(positiveTestCase1) shouldBe 'left
      LedgerString.fromString(positiveTestCase2) shouldBe 'left
    }
  }
}
