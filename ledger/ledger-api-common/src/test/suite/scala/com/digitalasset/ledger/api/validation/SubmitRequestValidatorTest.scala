// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import java.time.Instant

import com.daml.api.util.DurationConversion
import com.daml.api.util.TimestampConversion
import com.daml.lf.command.{Commands => LfCommands, CreateCommand => LfCreateCommand}
import com.daml.lf.data._
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.{Value => Lf}
import com.daml.ledger.api.DomainMocks
import com.daml.ledger.api.DomainMocks.{applicationId, commandId, workflowId}
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{List => ApiList, Map => ApiMap, Optional => ApiOptional, _}
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.scalatest.WordSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import scalaz.syntax.tag._
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SubmitRequestValidatorTest
    extends WordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks {

  val ledgerId = LedgerId("ledger-id")

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
    val int64 = Sum.Int64(1)
    val label = "label"
    val constructor = "constructor"
    val workflowId = "workflowId"
    val applicationId = "applicationId"
    val commandId = "commandId"
    val submitter = "party"
    val deduplicationTime = new Duration().withSeconds(10)
    val command =
      Command(
        Command.Command.Create(CreateCommand(
          Some(Identifier("package", moduleName = "module", entityName = "entity")),
          Some(Record(
            Some(Identifier("package", moduleName = "module", entityName = "entity")),
            Seq(RecordField("something", Some(Value(Value.Sum.Bool(true)))))))
        )))

    val commands = Commands(
      ledgerId = ledgerId.unwrap,
      workflowId = workflowId,
      applicationId = applicationId,
      commandId = commandId,
      party = submitter,
      commands = Seq(command),
      deduplicationTime = Some(deduplicationTime),
      minLedgerTimeAbs = None,
      minLedgerTimeRel = None,
    )
  }

  object internal {

    val ledgerTime = Instant.EPOCH.plusSeconds(10)
    val submittedAt = Instant.now
    val timeDelta = java.time.Duration.ofSeconds(1)
    val maxDeduplicationTime = java.time.Duration.ofDays(1)
    val deduplicateUntil = submittedAt.plusSeconds(api.deduplicationTime.seconds)

    val emptyCommands = ApiCommands(
      ledgerId = ledgerId,
      workflowId = Some(workflowId),
      applicationId = applicationId,
      commandId = commandId,
      submitter = DomainMocks.party,
      submittedAt = submittedAt,
      deduplicateUntil = deduplicateUntil,
      commands = LfCommands(
        DomainMocks.party,
        ImmArray(
          LfCreateCommand(
            Ref.Identifier(
              Ref.PackageId.assertFromString("package"),
              Ref.QualifiedName(
                Ref.ModuleName.assertFromString("module"),
                Ref.DottedName.assertFromString("entity"))),
            Lf.ValueRecord(
              Option(
                Ref.Identifier(
                  Ref.PackageId.assertFromString("package"),
                  Ref.QualifiedName(
                    Ref.ModuleName.assertFromString("module"),
                    Ref.DottedName.assertFromString("entity")))),
              ImmArray((Option(Ref.Name.assertFromString("something")), Lf.ValueTrue))
            )
          )),
        Time.Timestamp.assertFromInstant(ledgerTime),
        workflowId.unwrap
      )
    )
  }

  private[this] def withLedgerTime(commands: ApiCommands, let: Instant): ApiCommands =
    commands.copy(
      commands = commands.commands.copy(
        ledgerEffectiveTime = Time.Timestamp.assertFromInstant(let),
      ),
    )

  val commandsValidator = new CommandsValidator(ledgerId)
  import ValueValidator.validateValue

  private def unexpectedError = sys.error("unexpected error")

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "reject requests with empty commands" in {
        requestMustFailWith(
          commandsValidator.validateCommands(
            api.commands.withCommands(Seq.empty),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          "Missing field: commands"
        )
      }

      "not allow missing ledgerId" in {
        requestMustFailWith(
          commandsValidator
            .validateCommands(
              api.commands.withLedgerId(""),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          "Missing field: ledger_id"
        )
      }

      "tolerate a missing workflowId" in {
        commandsValidator.validateCommands(
          api.commands.withWorkflowId(""),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationTime) shouldEqual Right(
          internal.emptyCommands.copy(
            workflowId = None,
            commands = internal.emptyCommands.commands.copy(commandsReference = "")))
      }

      "not allow missing applicationId" in {
        requestMustFailWith(
          commandsValidator.validateCommands(
            api.commands.withApplicationId(""),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          "Missing field: application_id"
        )
      }

      "not allow missing commandId" in {
        requestMustFailWith(
          commandsValidator.validateCommands(
            api.commands.withCommandId(""),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          "Missing field: command_id"
        )
      }

      "not allow missing submitter" in {
        requestMustFailWith(
          commandsValidator
            .validateCommands(
              api.commands.withParty(""),
              internal.ledgerTime,
              internal.submittedAt,
              internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          """Missing field: party"""
        )
      }

      "advance ledger time if minLedgerTimeAbs is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)
        commandsValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeAbs = Some(TimestampConversion.fromInstant(minLedgerTimeAbs))),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationTime
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "advance ledger time if minLedgerTimeRel is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)
        commandsValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeRel = Some(DurationConversion.toProto(internal.timeDelta))),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationTime
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "not allow negative deduplication time" in {
        requestMustFailWith(
          commandsValidator.validateCommands(
            api.commands.copy(deduplicationTime = Some(Duration.of(-1, 0))),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          "Invalid field deduplication_time: Duration must be positive"
        )
      }

      "not allow deduplication time exceeding maximum deduplication time" in {
        val manySeconds = 100000L
        requestMustFailWith(
          commandsValidator.validateCommands(
            api.commands.copy(deduplicationTime = Some(Duration.of(manySeconds, 0))),
            internal.ledgerTime,
            internal.submittedAt,
            internal.maxDeduplicationTime),
          INVALID_ARGUMENT,
          s"Invalid field deduplication_time: The given deduplication time of ${java.time.Duration
            .ofSeconds(manySeconds)} exceeds the maximum deduplication time of ${internal.maxDeduplicationTime}"
        )
      }

      "default to maximum deduplication time if deduplication time is missing" in {
        commandsValidator.validateCommands(
          api.commands.copy(deduplicationTime = None),
          internal.ledgerTime,
          internal.submittedAt,
          internal.maxDeduplicationTime) shouldEqual Right(
          internal.emptyCommands.copy(
            deduplicateUntil = internal.submittedAt.plus(internal.maxDeduplicationTime)))
      }

    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Ref.ContractIdString.assertFromString("#coid")

        val input = Value(Sum.ContractId(coid))
        val expected = Lf.ValueContractId(Lf.AbsoluteContractId.V0(coid))

        validateValue(input) shouldEqual Right(expected)
      }
    }

    "validating party values" should {
      "convert valid party" in {
        validateValue(DomainMocks.values.validApiParty) shouldEqual Right(
          DomainMocks.values.validLfParty)
      }

      "reject non valid party" in {
        requestMustFailWith(
          validateValue(DomainMocks.values.invalidApiParty),
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
            "absolute values" -> "scale",
            "0" -> 0,
            "0.0" -> 0,
            "1.0000" -> 0,
            "3.1415926536" -> 10,
            "1" + "0" * 27 -> 0,
            "1" + "0" * 27 + "." + "0" * 9 + "1" -> 10,
            "0." + "0" * 9 + "1" -> 10,
            "0." -> 0,
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { (absoluteValue, expectedScale) =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            val expected =
              Lf.ValueNumeric(Numeric
                .assertFromBigDecimal(Numeric.Scale.assertFromInt(expectedScale), BigDecimal(s)))
            validateValue(input) shouldEqual Right(expected)
          }
        }

      }

      "reject out-of-bound decimals" in {
        val signs = Table("signs", "", "+", "-")
        val absoluteValues =
          Table(
            "absolute values" -> "scale",
            "1" + "0" * 38 -> 0,
            "1" + "0" * 28 + "." + "0" * 10 + "1" -> 11,
            "1" + "0" * 27 + "." + "0" * 11 + "1" -> 12
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { (absoluteValue, scale) =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            requestMustFailWith(
              validateValue(input),
              INVALID_ARGUMENT,
              s"""Invalid argument: Could not read Numeric string "$s""""
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
            ".0",
            "0." + "0" * 37 + "1",
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { absoluteValue =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            requestMustFailWith(
              validateValue(input),
              INVALID_ARGUMENT,
              s"""Invalid argument: Could not read Numeric string "$s""""
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
          validateValue(input) shouldEqual Right(expected)
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
            validateValue(input) shouldEqual Right(expected)
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
            validateValue(input),
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
            validateValue(input) shouldEqual Right(expected)
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
            validateValue(input),
            INVALID_ARGUMENT,
            s"Invalid argument: out of bound Date $int"
          )
        }
      }
    }

    "validating boolean values" should {
      "accept any of them" in {
        validateValue(Value(Sum.Bool(true))) shouldEqual Right(Lf.ValueTrue)
        validateValue(Value(Sum.Bool(false))) shouldEqual Right(Lf.ValueFalse)
      }
    }

    "validating unit values" should {
      "succeed" in {
        validateValue(Value(Sum.Unit(Empty()))) shouldEqual Right(Lf.ValueUnit)
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
        validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing identifiers in records" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          Lf.ValueRecord(None, ImmArray(Some(DomainMocks.label) -> DomainMocks.values.int64))
        validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing labels in record fields" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField("", Some(Value(api.int64)))))))
        val expected =
          ValueRecord(None, ImmArray(None -> DomainMocks.values.int64))
        validateValue(record) shouldEqual Right(expected)
      }

      "not allow missing record values" in {
        val record =
          Value(Sum.Record(Record(Some(api.identifier), Seq(RecordField(api.label, None)))))
        requestMustFailWith(validateValue(record), INVALID_ARGUMENT, "Missing field: value")
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
        validateValue(variant) shouldEqual Right(expected)
      }

      "tolerate missing identifiers" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, Some(Value(api.int64)))))
        val expected =
          Lf.ValueVariant(None, DomainMocks.values.constructor, DomainMocks.values.int64)

        validateValue(variant) shouldEqual Right(expected)
      }

      "not allow missing constructor" in {
        val variant = Value(Sum.Variant(Variant(None, "", Some(Value(api.int64)))))
        requestMustFailWith(validateValue(variant), INVALID_ARGUMENT, "Missing field: constructor")
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        requestMustFailWith(validateValue(variant), INVALID_ARGUMENT, "Missing field: value")
      }

    }

    "validating list values" should {
      "convert empty lists" in {
        val input = Value(Sum.List(ApiList(List.empty)))
        val expected =
          Lf.ValueNil

        validateValue(input) shouldEqual Right(expected)
      }

      "convert valid lists" in {
        val list = Value(Sum.List(ApiList(Seq(Value(api.int64), Value(api.int64)))))
        val expected =
          Lf.ValueList(FrontStack(ImmArray(DomainMocks.values.int64, DomainMocks.values.int64)))
        validateValue(list) shouldEqual Right(expected)
      }

      "reject lists containing invalid values" in {
        val input = Value(
          Sum.List(
            ApiList(Seq(DomainMocks.values.validApiParty, DomainMocks.values.invalidApiParty))))
        requestMustFailWith(
          validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

    "validating optional values" should {
      "convert empty optionals" in {
        val input = Value(Sum.Optional(ApiOptional(None)))
        val expected = Lf.ValueNone

        validateValue(input) shouldEqual Right(expected)
      }

      "convert valid non-empty optionals" in {
        val list = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.validApiParty))))
        val expected = Lf.ValueOptional(Some(DomainMocks.values.validLfParty))
        validateValue(list) shouldEqual Right(expected)
      }

      "reject optional containing invalid values" in {
        val input = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.invalidApiParty))))
        requestMustFailWith(
          validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

    "validating map values" should {
      "convert empty maps" in {
        val input = Value(Sum.Map(ApiMap(List.empty)))
        val expected = Lf.ValueTextMap(SortedLookupList.empty)
        validateValue(input) shouldEqual Right(expected)
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
          Lf.ValueTextMap(SortedLookupList.fromImmArray(lfEntries).getOrElse(unexpectedError))

        validateValue(input) shouldEqual Right(expected)
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
          validateValue(input),
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
          validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg)
      }
    }

  }

}
