// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.lf.data.Ref.{DottedName, Identifier, LedgerString, PackageId, Party, QualifiedName}
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class RefTest extends AnyFreeSpec with Matchers with EitherValues {

  "DottedName.string" - {
    "rejects bad segments" - {
      "digit at the start" in {
        DottedName.fromString("9test") shouldBe a[Left[_, _]]
      }

      "bad symbols" in {
        DottedName.fromString("test%") shouldBe a[Left[_, _]]
        DottedName.fromString("test-") shouldBe a[Left[_, _]]
        DottedName.fromString("test@") shouldBe a[Left[_, _]]
      }

      "unicode" in {
        DottedName.fromString("à") shouldBe a[Left[_, _]]
        DottedName.fromString("ਊ") shouldBe a[Left[_, _]]
      }

      "colon" in {
        DottedName.fromString("foo:bar") shouldBe a[Left[_, _]]
      }
    }

    "rejects empty segments" in {
      DottedName.fromString(".") shouldBe a[Left[_, _]]
      DottedName.fromString(".foo.") shouldBe a[Left[_, _]]
      DottedName.fromString(".foo") shouldBe a[Left[_, _]]
      DottedName.fromString("foo.") shouldBe a[Left[_, _]]
      DottedName.fromString("foo..bar") shouldBe a[Left[_, _]]
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

  private[this] val dottedNamesInOrder = List(
    DottedName.assertFromString("a"),
    DottedName.assertFromString("a.a"),
    DottedName.assertFromString("aa"),
    DottedName.assertFromString("b"),
    DottedName.assertFromString("b.a"),
  )

  testOrdered("DottedName", dottedNamesInOrder)

  "QualifiedName.fromString" - {
    "rejects no colon" in {
      QualifiedName.fromString("foo") shouldBe a[Left[_, _]]
    }

    "rejects multiple colons" in {
      QualifiedName.fromString("foo:bar:baz") shouldBe a[Left[_, _]]
    }

    "rejects empty dotted names" in {
      QualifiedName.fromString(":bar") shouldBe a[Left[_, _]]
      QualifiedName.fromString("bar:") shouldBe a[Left[_, _]]
    }

    "accepts valid qualified names" in {
      QualifiedName.fromString("foo:bar").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo"),
        name = DottedName.assertFromString("bar"),
      )
      QualifiedName.fromString("foo.bar:baz").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo.bar"),
        name = DottedName.assertFromString("baz"),
      )
      QualifiedName.fromString("foo:bar.baz").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo"),
        name = DottedName.assertFromString("bar.baz"),
      )
      QualifiedName.fromString("foo.bar:baz.quux").toOption.get shouldBe QualifiedName(
        module = DottedName.assertFromString("foo.bar"),
        name = DottedName.assertFromString("baz.quux"),
      )
    }
  }

  private[this] val qualifiedNamesInOrder =
    for {
      modNane <- dottedNamesInOrder
      name <- dottedNamesInOrder
    } yield QualifiedName(modNane, name)

  testOrdered("QualifiedName", qualifiedNamesInOrder)

  "Identifier.fromString" - {

    val errorMessageBeginning =
      "Separator ':' between package identifier and qualified name not found in "

    "rejects strings without any colon" in {
      Identifier.fromString("foo").left.value should startWith(errorMessageBeginning)
    }

    "rejects strings with empty segments but the error is caught further down the stack" in {
      Identifier.fromString(":bar").left.value should not startWith errorMessageBeginning
      Identifier.fromString("bar:").left.value should not startWith errorMessageBeginning
      Identifier.fromString("::").left.value should not startWith errorMessageBeginning
      Identifier.fromString("bar:baz").left.value should not startWith errorMessageBeginning
    }

    "accepts valid identifiers" in {
      Identifier.fromString("foo:bar:baz").toOption.get shouldBe Identifier(
        packageId = PackageId.assertFromString("foo"),
        qualifiedName = QualifiedName.assertFromString("bar:baz"),
      )
    }
  }

  private[this] val pkgIdsInOrder = List(
    Ref.PackageId.assertFromString("a"),
    Ref.PackageId.assertFromString("aa"),
    Ref.PackageId.assertFromString("b"),
  )

  private[this] val identifiersInOrder =
    for {
      pkgId <- pkgIdsInOrder
      qualifiedName <- qualifiedNamesInOrder
    } yield Ref.Identifier(pkgId, qualifiedName)

  testOrdered("Indenfitiers", identifiersInOrder)

  "Party and PackageId" - {

    val packageIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
    val partyIdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:-_ "
    val ledgerStringChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-#/ "

    def makeString(c: Char): String = s"the character $c is not US-ASCII"

    "rejects the empty string" in {
      PackageId.fromString("") shouldBe a[Left[_, _]]
      Party.fromString("") shouldBe a[Left[_, _]]
      LedgerString.fromString("") shouldBe a[Left[_, _]]
    }

    "treats US-ASCII characters as expected" in {
      for (c <- '\u0001' to '\u007f') {
        val s = makeString(c)
        PackageId.fromString(s) shouldBe (if (packageIdChars.contains(c)) a[Right[_, _]]
                                          else a[Left[_, _]])
        Party.fromString(s) shouldBe (if (partyIdChars.contains(c)) a[Right[_, _]]
                                      else a[Left[_, _]])
        LedgerString.fromString(s) shouldBe (if (ledgerStringChars.contains(c)) a[Right[_, _]]
                                             else a[Left[_, _]])
      }
    }

    "rejects no US-ASCII characters" in {

      val negativeTestCase = makeString('a')

      PackageId.fromString(negativeTestCase) shouldBe a[Right[_, _]]
      Party.fromString(negativeTestCase) shouldBe a[Right[_, _]]
      LedgerString.fromString(negativeTestCase) shouldBe a[Right[_, _]]

      for (c <- '\u0080' to '\u00ff') {
        val positiveTestCase = makeString(c)
        PackageId.fromString(positiveTestCase) shouldBe a[Left[_, _]]
        Party.fromString(positiveTestCase) shouldBe a[Left[_, _]]
        LedgerString.fromString(positiveTestCase) shouldBe a[Left[_, _]]
      }
      for (
        positiveTestCase <- List(
          "español",
          "東京",
          "Λ (τ : ⋆) (σ: ⋆ → ⋆). λ (e : ∀ (α : ⋆). σ α) → (( e @τ ))",
        )
      ) {
        Party.fromString(positiveTestCase) shouldBe a[Left[_, _]]
        PackageId.fromString(positiveTestCase) shouldBe a[Left[_, _]]
      }
    }

    "reject too long string" in {
      Party.fromString("p" * 255) shouldBe a[Right[_, _]]
      Party.fromString("p" * 256) shouldBe a[Left[_, _]]
    }
  }

  "LedgerString" - {
    "reject too long strings" in {
      val negativeTestCase = "a" * 255
      val positiveTestCase1 = "a" * 256
      val positiveTestCase2 = "a" * 500
      LedgerString.fromString(negativeTestCase) shouldBe a[Right[_, _]]
      LedgerString.fromString(positiveTestCase1) shouldBe a[Left[_, _]]
      LedgerString.fromString(positiveTestCase2) shouldBe a[Left[_, _]]
    }
  }

  private def testOrdered[X <: Ordered[X]](name: String, elems: Iterable[X]) =
    s"$name#compare" - {
      "agrees with equality" in {
        for {
          x <- elems
          y <- elems
        } ((x compare y) == 0) shouldBe (x == y)
      }

      "is reflexive" - {
        for {
          x <- elems
        } (x compare x) shouldBe 0
      }

      "is symmetric" - {
        for {
          x <- elems
          y <- elems
        } (x compare y) shouldBe -(y compare x)
      }

      "is transitive on comparable values" - {
        for {
          x <- elems
          y <- elems
          if (x compare y) <= 0
          z <- elems
          if (y compare z) <= 0
        } (x compare z) shouldBe <=(0)
      }
    }

}
