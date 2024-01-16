// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import org.scalatest.wordspec.AnyWordSpec

class IdentifierTest extends AnyWordSpec with BaseTest {

  "safe simple string" when {

    "should" should {
      "be a happy cookie and not return an error" in {

        assert(SafeSimpleString.fromProtoPrimitive("aAbbZ09-").isRight)
      }

      "complain on any non simple string character" in {
        "#%!><,;".foreach(x => assert(SafeSimpleString.fromProtoPrimitive(x.toString).isLeft))

      }

      "complain if the delimiter is used" in {
        Seq("not::ok", "::not::ok", "::notok", "::not::ok::", "notok::").foreach(ss =>
          SafeSimpleString.fromProtoPrimitive(ss).left.value shouldBe a[String]
        )
      }

    }

  }

  "identifier" should {
    "contain only simple characters" in {
      forEvery(Seq("#a", "\\a", "/a", "ä")) { s =>
        Identifier.create(s).left.value shouldBe a[String]
        an[IllegalArgumentException] shouldBe thrownBy(Identifier.tryCreate(s))
        Identifier.fromProtoPrimitive(s).left.value shouldBe a[String]
      }
    }
  }

  val uid: UniqueIdentifier = UniqueIdentifier.tryCreate("ABCefg123", "12345678")

  "unique identifier" when {
    "simple string conversion" should {
      "should be identical" in {
        UniqueIdentifier.fromProtoPrimitive_(uid.toProtoPrimitive) shouldBe Right(uid)
      }
    }

    "reading from string" should {
      "should yield the same identifier" in {
        assertResult(uid)(
          UniqueIdentifier.tryFromProtoPrimitive(
            uid.id.unwrap + "::" + uid.namespace.fingerprint.unwrap
          )
        )
      }

      "fail for invalid string using the delimiter" in {
        Seq("::not::ok", "::not::ok", "::notok", "::not::ok::", "notok::").foreach(ss =>
          UniqueIdentifier.fromProtoPrimitive_(ss).left.value shouldBe a[String]
        )
      }

      "fail for generally invalid strings" in {
        val templates =
          Seq[String => String](
            x => s"${x}not::ok",
            x => s"not$x::ok",
            x => s"not::${x}ok",
            x => s"not::ok$x",
          )
        val checks = Seq("#", "%", "!", ">", "<", ",", ";", "::", ":::", "::::").flatMap(x =>
          templates.map(_(x))
        )
        forEvery(checks) { ss =>
          UniqueIdentifier.fromProtoPrimitive_(ss).left.value shouldBe a[String]
        }
      }

      "succeed for valid strings" in {
        Seq("is::ok", ":is::ok", "is::ok", ":is::ok:", "is::o:k:r:l:y").foreach(ss =>
          UniqueIdentifier.fromProtoPrimitive_(ss).value shouldBe a[UniqueIdentifier]
        )
      }

      "throw an exception when using invalid characters" in {
        assertThrows[IllegalArgumentException] {
          UniqueIdentifier.tryFromProtoPrimitive("%%##!!:!@#@#")
        }
      }

      "produce sensible error messages " in {
        UniqueIdentifier.fromProtoPrimitive_("Bank::") shouldEqual Left(
          "Fingerprint decoding of `Bank::` failed with: StringConversionError(Daml-LF Party is empty)"
        )
        UniqueIdentifier.fromProtoPrimitive_("") shouldEqual Left(
          "Empty string is not a valid unique identifier."
        )
        UniqueIdentifier.fromProtoPrimitive_("::Wurst") shouldEqual Left(
          "Invalid unique identifier `::Wurst` with empty identifier."
        )
        UniqueIdentifier.fromProtoPrimitive_("aa::Wur:st::") shouldEqual Left(
          "Fingerprint decoding of `aa::Wur:st::` failed with: StringConversionError(String contains reserved delimiter `::`.)"
        )
        UniqueIdentifier.fromProtoPrimitive_("::") shouldEqual Left(
          "Invalid unique identifier `::` with empty identifier."
        )
      }

      "throw if namespace was ommitted" in {
        assertThrows[IllegalArgumentException] {
          UniqueIdentifier.tryFromProtoPrimitive("123456")
        }
      }

    }

  }

  "key owner serialization" should {
    "be able to convert back and forth" in {
      val pid = DefaultTestIdentities.participant1
      KeyOwner.fromProtoPrimitive(pid.toProtoPrimitive, "Pid") shouldBe Right(pid)
      Member.fromProtoPrimitive(pid.toProtoPrimitive, "Pid") shouldBe Right(pid)
    }

    "act sanely on invalid inputs" in {
      Seq(
        "nothing valid",
        "not::enough",
        "INVALID::da::default",
        "::::",
        "PAR::::",
        "PAR::da::",
        "::da::default",
      )
        .foreach { str =>
          KeyOwner.fromProtoPrimitive(str, "owner").left.value shouldBe a[ProtoDeserializationError]
        }
    }

  }

  "SequencerId serialization" should {
    "be able to convert back and forth" in {
      val sequencerId = DefaultTestIdentities.sequencerId
      SequencerId
        .fromProtoPrimitive(sequencerId.toProtoPrimitive, "sequencerId")
        .value shouldBe sequencerId
    }
  }

}
