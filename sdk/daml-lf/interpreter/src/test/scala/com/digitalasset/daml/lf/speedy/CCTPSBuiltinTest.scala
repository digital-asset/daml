package com.daml.lf

import com.digitalasset.daml.lf.language.LanguageMajorVersion
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class CCTPSBuiltinTestV2 extends CCTPSBuiltinTest(LanguageMajorVersion.V2)

class CCTPSBuiltinTest(majorLanguageVersion: LanguageMajorVersion)
  extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with BeforeAndAfterAll
    with Inside {

  val helpers = new SBuiltinTestHelpers(majorLanguageVersion)

  import helpers.{parserParameters => _, _}

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  "KECCAK256_TEXT" - {
    // FIXME:
    "correctly digest hex strings" in {
      val testCases = Table(
        "" ->
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
          "cd372fb85148700fa88095e3492d3f9f5beb43e555e5ff26d95f5a6adc36f8e6",
      )
      forEvery(testCases) { (input, output) =>
        eval(e"""KECCAK256_TEXT "$input"""") shouldBe Right(SText(output))
      }
    }

    // FIXME:
    "fail to digest non-hex strings" in {
      val testCases = Table(
        "input" -> "output",
        "" ->
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "abcdefg" ->
          "",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ->
          "cd372fb85148700fa88095e3492d3f9f5beb43e555e5ff26d95f5a6adc36f8e6",
        """Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
          |eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
          |minim veniam, quis nostrud exercitation ullamco laboris nisi ut
          |aliquip ex ea commodo consequat. Duis aute irure dolor in
          |reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
          |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
          |culpa qui officia deserunt mollit anim id est laborum..."""
          .replaceAll("\r", "")
          .stripMargin ->
          "c045064089460b634bb47e71d2457cd0e8dbc1327aaf9439c275c9796c073620",
        "a¶‱😂" ->
          "8f1cc14a85321115abcd2854e34f9ca004f4f199d367c3c9a84a355f287cec2e",
      )
      forEvery(testCases) { (input, output) =>
        inside(eval(e"""KECCAK256_TEXT "$input"""")) {
          case Left(SErrorCrash(_, reason)) =>
            output shouldBe reason
        }
      }
    }
  }

  "SECP256K1_BOOL" - {
    "correctly verify valid signatures" in {
      // TODO:
      succeed
    }

    "non-hex encoded data" - {
      "fail with invalid signature encodings" in {
        // TODO:
        succeed
      }

      "fail with invalid message encodings" in {
        // TODO:
        succeed
      }

      "fail with invalid public key encodings" in {
        // TODO:
        succeed
      }
    }

    "fail with invalid public keys" in {
      // TODO:
      succeed
    }

    "fail with invalid signatures" in {
      // TODO:
      succeed
    }
  }
}
