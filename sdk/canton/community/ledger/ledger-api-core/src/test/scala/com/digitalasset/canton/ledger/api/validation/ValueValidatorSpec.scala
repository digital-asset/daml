// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.api.v1.value.Value.Sum
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ValueValidatorSpec
    extends AnyFlatSpec
    with Matchers
    with ValidatorTestUtils
    with ScalaCheckPropertyChecks {

  import ValueValidatorSpec.*

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

  private def failNumericValidation(validator: ValueValidator)(value: String) =
    requestMustFailWith(
      request = validator.validateValue(Value(Sum.Numeric(value))),
      code = INVALID_ARGUMENT,
      description =
        s"""INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Could not read Numeric string "$value"""",
    )

  private def failPartyValidation(
      validator: ValueValidator
  )(value: String, error: String => String) =
    requestMustFailWith(
      request = validator.validateValue(Value(Sum.Party(value))),
      code = INVALID_ARGUMENT,
      description = error(value),
    )

  behavior of ValueValidator.getClass.getSimpleName.stripSuffix("$")
  it should "not allow too large numbers" in forEvery(tooLarge)(
    failNumericValidation(ValueValidator)
  )
  it should "not allow too precise numbers" in forEvery(tooPrecise)(
    failNumericValidation(ValueValidator)
  )
  it should "not allow improper numbers" in forEvery(improperNumbers)(
    failNumericValidation(ValueValidator)
  )
  it should "allow all proper numbers" in forEvery(properNumbers) { input =>
    assert(ValueValidator.validateValue(Value(Sum.Numeric(input))).isRight)
  }
  it should "not allow improper parties" in forEvery(improperParties) { (input, _) =>
    failPartyValidation(ValueValidator)(
      input,
      value =>
        s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x23 in Daml-LF Party \"$value\"",
    )
  }
  it should "allow strictly improper parties" in forEvery(strictImproperParties) { (input, _) =>
    assert(ValueValidator.validateValue(Value(Sum.Party(input))).isRight)
  }
  it should "allow all proper parties" in forEvery(properParties) { input =>
    assert(ValueValidator.validateValue(Value(Sum.Party(input))).isRight)
  }

  behavior of StricterNumericValueValidator.getClass.getSimpleName.stripSuffix("$")
  it should "not allow too large numbers" in forEvery(tooLarge)(
    failNumericValidation(StricterNumericValueValidator)
  )
  it should "not allow too precise numbers" in forEvery(tooPrecise)(
    failNumericValidation(StricterNumericValueValidator)
  )
  it should "not allow improper numbers" in forEvery(improperNumbers)(
    failNumericValidation(StricterNumericValueValidator)
  )
  it should "not allow strictly improper numbers" in forEvery(strictImproperNumbers)(
    failNumericValidation(StricterNumericValueValidator)
  )
  it should "allow all proper numbers" in forEvery(properNumbers) { input =>
    assert(StricterNumericValueValidator.validateValue(Value(Sum.Numeric(input))).isRight)
  }

  behavior of StricterPartyValueValidator.getClass.getSimpleName.stripSuffix("$")
  it should "not allow improper parties" in forEvery(improperParties)(
    failPartyValidation(StricterPartyValueValidator)
  )
  it should "not allow strictly improper parties" in forEvery(strictImproperParties)(
    failPartyValidation(StricterPartyValueValidator)
  )
  it should "allow all proper parties" in forEvery(properParties) { input =>
    assert(StricterNumericValueValidator.validateValue(Value(Sum.Party(input))).isRight)
  }

}

object ValueValidatorSpec {
  import org.scalatest.prop.Tables.Table

  private val properNumbers = Table[String](
    "string",
    "99999999999999999999999999999999999999.",
    "-99999999999999999999999999999999999999.",
    "9999999999999999999999999999.9999999999",
    "-9999999999999999999999999999.9999999999",
    "82845904523536028.747135266249775724701",
    "-82845904523536028.747135266249775724701",
    "9876543210.0123456789",
    "-9876543210.0123456789",
    "10.01234567890000000",
    "-10.01234567890000000",
    "9.9999999999999999999999999999999999999",
    "-9.9999999999999999999999999999999999999",
    "9.0000000000000000000016180339887499677",
    "-9.0000000000000000000016180339887499677",
    "1.",
    "-1.",
    "1.00000",
    "-1.00000",
    "0.1",
    "-0.1",
    "0.3141592653589793238462643383279502884",
    "-0.3141592653589793238462643383279502884",
    "0.000000000000000000000000000000000001",
    "-0.0000000000000000000000000000000000001",
    "0.",
    "0.0",
    "0.0000",
    "-0.0",
  )

  private val tooLarge = Table[String](
    "string",
    "100000000000000000000000000000000000000.",
    "-100000000000000000000000000000000000000.",
    "999999999999999999999999999999999999999.",
  )

  private val tooPrecise = Table[String](
    "string",
    "0.0000000000000000000000000000000000000000100",
    "-0.00000000000000000000000000000000000001",
    "82845904523536028.7471352662497757247012",
    "0.31415926535897932384626433832795028842",
  )

  private val improperNumbers = Table[String](
    "string",
    "not a numeric",
    "1E10",
    "1..0",
    ".4",
    "--1.1",
    "2.1.0",
  )

  private val strictImproperNumbers = Table[String](
    "string",
    "00.0",
    "+0.1",
  )

  private val properParties = Table[String](
    "string",
    "Alice::12202687de0c620c39eacf027b608cc279b485eb2df2d97602371754549e1ff0888a",
    "Special:-_ ::12202687de0c620c39eacf027b608cc279b485eb2df2d97602371754549e1ff0888a",
    // These shouldn't be accepted by the strict validator, but they are
    "ShortFingerprint::1",
    "Alice::ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
  )

  private def missingNamespace(value: String) =
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Invalid unique identifier `$value` with missing namespace."
  private def fingerprintError(value: String) =
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Fingerprint decoding of `$value` failed with: StringConversionError(Daml-LF Party is empty)"
  private def longFingerPrint(value: String) = {
    val fingerprint = value.split(':').last
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Fingerprint decoding of `$value` failed with: " +
      s"InvariantViolation(The given Fingerprint has a maximum length of 68 but a Fingerprint of length 69 ('$fingerprint.') was given)"
  }
  private def badFingerPrint(badChar: Char)(value: String) = {
    val fingerprint = value.split(':').last
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Fingerprint decoding of `$value` failed with: " +
      s"StringConversionError(non expected character 0x${badChar.toInt.toHexString} in Daml-LF Party \"$fingerprint\")"
  }
  private def emptyIdentifier(value: String) =
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Invalid unique identifier `$value` with empty identifier."
  private def invalidIdentifier(badChar: Char)(value: String) = {
    val identifier = value.split(':').head
    s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Identifier decoding of `$value` " +
      s"failed with: non expected character 0x${badChar.toInt.toHexString} in Daml-LF Party \"$identifier\""
  }

  private val strictImproperParties = Table[String, String => String](
    ("string", "error"),
    ("Now I Am Become Death the Destroyer of Worlds", missingNamespace),
    ("Separator::", fingerprintError),
    (
      "Long::12202687de0c620c39eacf027b608cc279b485eb2df2d97602371754549e1ff0888aA",
      longFingerPrint,
    ),
    ("::12202687de0c620c39eacf027b608cc279b485eb2df2d97602371754549e1ff0888aA", emptyIdentifier),
  )

  private val improperParties = Table[String, String => String](
    ("string", "error"),
    (
      "Alice#::12202687de0c620c39eacf027b608cc279b485eb2df2d97602371754549e1ff0888a",
      invalidIdentifier('#'),
    ),
    (
      "Bad::12202687de0c620c39eacf027b608#c279b485eb2df2d97602371754549e1ff0888a",
      badFingerPrint('#'),
    ),
  )

}
