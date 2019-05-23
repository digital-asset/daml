// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import java.time.Instant

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.daml.lf.command.{Commands => LfCommands}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value.ValueRecord
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.DomainMocks.{applicationId, commandId, workflowId}
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{
  List => ApiList,
  Map => ApiMap,
  Optional => ApiOptional,
  _
}
import com.digitalasset.ledger.api.DomainMocks
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.scalatest.WordSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import scalaz.syntax.tag._

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SubmitRequestValidatorTest
    extends WordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks {

  val ledgerId: Ref.LedgerIdString = Ref.LedgerIdString.assertFromString("ledger-id")

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
    val int64 = Sum.Int64(1)
    val label = "label"
    val constructor = "constructor"
    val workflowId = "workflowId"
    val applicationId = "applicationId"
    val commandId = "commandId"
    val submitter = "party"
    val let = TimestampConversion.fromInstant(Instant.now)
    val mrt = TimestampConversion.fromInstant(Instant.now)

    val commands = Commands(
      ledgerId,
      workflowId,
      applicationId,
      commandId,
      submitter,
      Some(let),
      Some(mrt),
      Seq.empty
    )
  }

  object internal {

    val let = TimestampConversion.toInstant(api.let)
    val mrt = TimestampConversion.toInstant(api.mrt)

    val emptyCommands = ApiCommands(
      ledgerId,
      Some(workflowId),
      applicationId,
      commandId,
      DomainMocks.party,
      let,
      mrt,
      LfCommands(
        ImmArray.empty,
        Time.Timestamp.assertFromInstant(let),
        workflowId.unwrap
      )
    )
  }

  val commandsValidator =
    new CommandsValidator(ledgerId, new IdentifierResolver(_ => Future.successful(None)))

  def recordFieldWithValue(value: Value) = RecordField("label", Some(value))

  private def unexpectedError = sys.error("unexpected error")

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "convert valid requests with empty commands" in {
        commandsValidator.validateCommands(api.commands) shouldEqual Right(internal.emptyCommands)
      }

      "not allow missing ledgerId" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.withLedgerId("")),
          INVALID_ARGUMENT,
          "Missing field: leger_id")
      }

      "tolerate a missing workflowId" in {
        commandsValidator.validateCommands(api.commands.withWorkflowId("")) shouldEqual Right(
          internal.emptyCommands.copy(
            workflowId = None,
            commands = internal.emptyCommands.commands.copy(commandsReference = "")))
      }

      "not allow missing applicationId" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.withApplicationId("")),
          INVALID_ARGUMENT,
          "Missing field: application_id")
      }

      "not allow missing commandId" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.withCommandId("")),
          INVALID_ARGUMENT,
          "Missing field: command_id")
      }

      "not allow missing submitter" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.withParty("")),
          INVALID_ARGUMENT,
          """Missing field: party"""
        )
      }

      "not allow missing let" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.copy(ledgerEffectiveTime = None)),
          INVALID_ARGUMENT,
          "Missing field: ledger_effective_time")

      }

      "not allow missing mrt" in {
        requestMustFailWith(
          commandsValidator.validateCommands(api.commands.copy(maximumRecordTime = None)),
          INVALID_ARGUMENT,
          "Missing field: maximum_record_time")
      }
    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Ref.ContractIdString.assertFromString("coid")

        val input = Value(Sum.ContractId(coid))
        val expected = Lf.ValueContractId(Lf.AbsoluteContractId(coid))

        commandsValidator.validateValue(input) shouldEqual Right(expected)
      }
    }

    "validating party values" should {
      "convert valid party" in {
        commandsValidator.validateValue(DomainMocks.values.validApiParty) shouldEqual Right(
          DomainMocks.values.validLfParty)
      }

      "reject non valid party" in {
        requestMustFailWith(
          commandsValidator.validateValue(DomainMocks.values.invalidApiParty),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg
        )
      }
    }

    "validating decimal values" should {
      "convert valid decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "absolute values",
            "0",
            "0.0",
            "3.1415926536",
            "1" + "0" * 27,
            "1" + "0" * 27 + "." + "0" * 9 + "1",
            "0." + "0" * 9 + "1"
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { absoluteValue =>
            val s = sign + absoluteValue
            val input = Value(Sum.Decimal(s))
            val expected = Lf.ValueDecimal(Decimal.fromString(s).getOrElse(unexpectedError))
            commandsValidator.validateValue(input) shouldEqual Right(expected)
          }
        }
      }

      "reject out-of-bound decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "absolute values",
            "1" + "0" * 28,
            "1" + "0" * 28 + "." + "0" * 9 + "1",
            "1" + "0" * 28 + "." + "0" * 10 + "1",
            "1" + "0" * 27 + "." + "0" * 10 + "1",
            "0." + "0" * 10 + "1",
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { absoluteValue =>
            val s = sign + absoluteValue
            val input = Value(Sum.Decimal(s))
            requestMustFailWith(
              commandsValidator.validateValue(input),
              INVALID_ARGUMENT,
              s"""Invalid argument: Could not read Decimal string "$s""""
            )
          }
        }
      }

      "reject invalid decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "invalid absolute values",
            "zero",
            "1E10",
            "+",
            "-",
            "0x01",
            ".",
            "",
            "0.",
            ".0",
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { absoluteValue =>
            val s = sign + absoluteValue
            val input = Value(Sum.Decimal(s))
            requestMustFailWith(
              commandsValidator.validateValue(input),
              INVALID_ARGUMENT,
              s"""Invalid argument: Could not read Decimal string "$s""""
            )
          }
        }
      }

    }

    "validating text values" should {
      "accept any of them" in {
        val strings =
          Table("string", "", "a¶‱😂", "a¶‱😃")

        forEvery(strings) { s =>
          val input = Value(Sum.Text(s))
          val expected = Lf.ValueText(s)
          commandsValidator.validateValue(input) shouldEqual Right(expected)
        }
      }
    }

    "validating timestamp values" should {
      "accept valid timestamp" in {
        val testCases = Table(
          "long/timestamp",
          Time.Timestamp.MinValue.micros -> Time.Timestamp.MinValue,
          0L -> Time.Timestamp.Epoch,
          Time.Timestamp.MaxValue.micros -> Time.Timestamp.MaxValue,
        )

        forEvery(testCases) {
          case (long, timestamp) =>
            val input = Value(Sum.Timestamp(long))
            val expected = Lf.ValueTimestamp(timestamp)
            commandsValidator.validateValue(input) shouldEqual Right(expected)
        }
      }

      "reject out-of-bound timestamp" in {
        val testCases = Table(
          "long/timestamp",
          Long.MinValue,
          Time.Timestamp.MinValue.micros - 1,
          Time.Timestamp.MaxValue.micros + 1,
          Long.MaxValue)

        forEvery(testCases) { long =>
          val input = Value(Sum.Timestamp(long))
          requestMustFailWith(
            commandsValidator.validateValue(input),
            INVALID_ARGUMENT,
            s"Invalid argument: out of bound Timestamp $long"
          )
        }
      }
    }

    "validating date values" should {
      "accept valid date" in {
        val testCases = Table(
          "int/date",
          Time.Date.MinValue.days -> Time.Date.MinValue,
          0 -> Time.Date.Epoch,
          Time.Date.MaxValue.days -> Time.Date.MaxValue,
        )

        forEvery(testCases) {
          case (int, date) =>
            val input = Value(Sum.Date(int))
            val expected = Lf.ValueDate(date)
            commandsValidator.validateValue(input) shouldEqual Right(expected)
        }
      }

      "reject out-of-bound date" in {
        val testCases = Table(
          "int/date",
          Int.MinValue,
          Time.Date.MinValue.days - 1,
          Time.Date.MaxValue.days + 1,
          Int.MaxValue)

        forEvery(testCases) { int =>
          val input = Value(Sum.Date(int))
          requestMustFailWith(
            commandsValidator.validateValue(input),
            INVALID_ARGUMENT,
            s"Invalid argument: out of bound Date $int"
          )
        }
      }
    }

    "validating boolean values" should {
      "accept any of them" in {
        commandsValidator.validateValue(Value(Sum.Bool(true))) shouldEqual Right(Lf.ValueBool(true))
        commandsValidator.validateValue(Value(Sum.Bool(false))) shouldEqual Right(
          Lf.ValueBool(false))
      }
    }

    "validating unit values" should {
      "succeed" in {
        commandsValidator.validateValue(Value(Sum.Unit(Empty()))) shouldEqual Right(Lf.ValueUnit)
      }
    }

    "validating record values" should {
      "convert valid records" in {
        val record =
          Value(
            Sum.Record(
              Record(Some(api.identifier), Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          Lf.ValueRecord(
            Some(DomainMocks.identifier),
            ImmArray(Some(DomainMocks.label) -> DomainMocks.values.int64))
        commandsValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing identifiers in records" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          Lf.ValueRecord(None, ImmArray(Some(DomainMocks.label) -> DomainMocks.values.int64))
        commandsValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing labels in record fields" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField("", Some(Value(api.int64)))))))
        val expected =
          ValueRecord(None, ImmArray(None -> DomainMocks.values.int64))
        commandsValidator.validateValue(record) shouldEqual Right(expected)
      }

      "not allow missing record values" in {
        val record =
          Value(Sum.Record(Record(Some(api.identifier), Seq(RecordField(api.label, None)))))
        requestMustFailWith(
          commandsValidator.validateValue(record),
          INVALID_ARGUMENT,
          "Missing field: value")
      }

    }

    "validating variant values" should {

      "convert valid variants" in {
        val variant =
          Value(Sum.Variant(Variant(Some(api.identifier), api.constructor, Some(Value(api.int64)))))
        val expected = Lf.ValueVariant(
          Some(DomainMocks.identifier),
          DomainMocks.values.constructor,
          DomainMocks.values.int64)
        commandsValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "tolerate missing identifiers" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, Some(Value(api.int64)))))
        val expected =
          Lf.ValueVariant(None, DomainMocks.values.constructor, DomainMocks.values.int64)

        commandsValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "not allow missing constructor" in {
        val variant = Value(Sum.Variant(Variant(None, "", Some(Value(api.int64)))))
        requestMustFailWith(
          commandsValidator.validateValue(variant),
          INVALID_ARGUMENT,
          "Missing field: constructor")
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        requestMustFailWith(
          commandsValidator.validateValue(variant),
          INVALID_ARGUMENT,
          "Missing field: value")
      }

    }

    "validating list values" should {
      "convert empty lists" in {
        val input = Value(Sum.List(ApiList(List.empty)))
        val expected =
          Lf.ValueList(FrontStack.empty)

        commandsValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid lists" in {
        val list = Value(Sum.List(ApiList(Seq(Value(api.int64), Value(api.int64)))))
        val expected =
          Lf.ValueList(FrontStack(ImmArray(DomainMocks.values.int64, DomainMocks.values.int64)))
        commandsValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject lists containing invalid values" in {
        val input = Value(
          Sum.List(
            ApiList(Seq(DomainMocks.values.validApiParty, DomainMocks.values.invalidApiParty))))
        requestMustFailWith(
          commandsValidator.validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

    "validating optional values" should {
      "convert empty optionals" in {
        val input = Value(Sum.Optional(ApiOptional(None)))
        val expected = Lf.ValueOptional(None)

        commandsValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid non-empty optionals" in {
        val list = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.validApiParty))))
        val expected = Lf.ValueOptional(Some(DomainMocks.values.validLfParty))
        commandsValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject optional containing invalid values" in {
        val input = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.invalidApiParty))))
        requestMustFailWith(
          commandsValidator.validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

    "validating map values" should {
      "convert empty maps" in {
        val input = Value(Sum.Map(ApiMap(List.empty)))
        val expected = Lf.ValueMap(SortedLookupList.empty)
        commandsValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid maps" in {
        val entries = ImmArray(1 until 5).map { x =>
          Utf8.sha256(x.toString) -> x.toLong
        }
        val apiEntries = entries.map {
          case (k, v) => ApiMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.Map(ApiMap(apiEntries.toSeq)))
        val lfEntries = entries.map { case (k, v) => k -> Lf.ValueInt64(v) }
        val expected =
          Lf.ValueMap(SortedLookupList.fromImmArray(lfEntries).getOrElse(unexpectedError))

        commandsValidator.validateValue(input) shouldEqual Right(expected)
      }

      "reject maps with repeated keys" in {
        val entries = ImmArray(1 +: (1 until 5)).map { x =>
          Utf8.sha256(x.toString) -> x.toLong
        }
        val apiEntries = entries.map {
          case (k, v) => ApiMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.Map(ApiMap(apiEntries.toSeq)))
        requestMustFailWith(
          commandsValidator.validateValue(input),
          INVALID_ARGUMENT,
          s"Invalid argument: key ${Utf8.sha256("1")} duplicated when trying to build map")
      }

      "reject maps containing invalid value" in {
        val apiEntries =
          List(
            ApiMap.Entry("1", Some(DomainMocks.values.validApiParty)),
            ApiMap.Entry("2", Some(DomainMocks.values.invalidApiParty)))
        val input = Value(Sum.Map(ApiMap(apiEntries)))
        requestMustFailWith(
          commandsValidator.validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

  }

}
