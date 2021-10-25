// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.api.util.{DurationConversion, TimestampConversion}
import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher, NoLogging}
import com.daml.ledger.api.DomainMocks.{applicationId, commandId, submissionId, workflowId}
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.v1.commands.Commands.{DeduplicationPeriod => DeduplicationPeriodProto}
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{List => ApiList, Map => ApiMap, Optional => ApiOptional, _}
import com.daml.ledger.api.{DeduplicationPeriod, DomainMocks}
import com.daml.lf.command.{Commands => LfCommands, CreateCommand => LfCreateCommand}
import com.daml.lf.data._
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.{Value => Lf}
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code
import io.grpc.Status.Code.{INVALID_ARGUMENT, UNAVAILABLE, NOT_FOUND}
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.tag._

import java.time.{Instant, Duration => JDuration}
import scala.annotation.nowarn

@nowarn("msg=deprecated")
class SubmitRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks
    with MockitoSugar {
  private val ledgerId = LedgerId("ledger-id")
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

  private object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
    val int64 = Sum.Int64(1)
    val label = "label"
    val constructor = "constructor"
    val submitter = "party"
    val deduplicationDuration = new Duration().withSeconds(10)
    val command = Command.of(
      Command.Command.Create(
        CreateCommand.of(
          Some(Identifier("package", moduleName = "module", entityName = "entity")),
          Some(
            Record(
              Some(Identifier("package", moduleName = "module", entityName = "entity")),
              Seq(RecordField("something", Some(Value(Value.Sum.Bool(true))))),
            )
          ),
        )
      )
    )

    val commands = Commands(
      ledgerId = ledgerId.unwrap,
      workflowId = workflowId.unwrap,
      applicationId = applicationId.unwrap,
      submissionId = submissionId.unwrap,
      commandId = commandId.unwrap,
      party = submitter,
      commands = Seq(command),
      deduplicationPeriod = DeduplicationPeriodProto.DeduplicationTime(deduplicationDuration),
      minLedgerTimeAbs = None,
      minLedgerTimeRel = None,
    )
  }

  private object internal {
    val ledgerTime = Instant.EPOCH.plusSeconds(10)
    val submittedAt = Instant.now
    val timeDelta = java.time.Duration.ofSeconds(1)
    val maxDeduplicationDuration = java.time.Duration.ofDays(1)
    val deduplicationDuration = java.time.Duration.ofSeconds(
      api.deduplicationDuration.seconds,
      api.deduplicationDuration.nanos.toLong,
    )

    val emptyCommands = ApiCommands(
      ledgerId = ledgerId,
      workflowId = Some(workflowId),
      applicationId = applicationId,
      commandId = commandId,
      submissionId = submissionId,
      actAs = Set(DomainMocks.party),
      readAs = Set.empty,
      submittedAt = submittedAt,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(deduplicationDuration),
      commands = LfCommands(
        ImmArray(
          LfCreateCommand(
            Ref.Identifier(
              Ref.PackageId.assertFromString("package"),
              Ref.QualifiedName(
                Ref.ModuleName.assertFromString("module"),
                Ref.DottedName.assertFromString("entity"),
              ),
            ),
            Lf.ValueRecord(
              Option(
                Ref.Identifier(
                  Ref.PackageId.assertFromString("package"),
                  Ref.QualifiedName(
                    Ref.ModuleName.assertFromString("module"),
                    Ref.DottedName.assertFromString("entity"),
                  ),
                )
              ),
              ImmArray((Option(Ref.Name.assertFromString("something")), Lf.ValueTrue)),
            ),
          )
        ),
        Time.Timestamp.assertFromInstant(ledgerTime),
        workflowId.unwrap,
      ),
    )
  }

  private[this] def withLedgerTime(commands: ApiCommands, let: Instant): ApiCommands =
    commands.copy(
      commands = commands.commands.copy(
        ledgerEffectiveTime = Time.Timestamp.assertFromInstant(let)
      )
    )

  import ValueValidator.validateValue

  private def unexpectedError = sys.error("unexpected error")

  class Fixture(testedFactory: Boolean => CommandsValidator) {
    def testRequestFailure(
        testedRequest: CommandsValidator => Either[StatusRuntimeException, _],
        expectedCodeV1: Code,
        expectedDescriptionV1: String,
        expectedCodeV2: Code,
        expectedDescriptionV2: String,
    ): Assertion = {
      requestMustFailWith(
        request = testedRequest(testedFactory(false)),
        code = expectedCodeV1,
        description = expectedDescriptionV1,
      )
      requestMustFailWith(
        request = testedRequest(testedFactory(true)),
        code = expectedCodeV2,
        description = expectedDescriptionV2,
      )
    }

    def tested(enabledSelfServiceErrorCodes: Boolean): CommandsValidator = {
      testedFactory(enabledSelfServiceErrorCodes)
    }
  }

  private val errorCodesVersionSwitcher_mock = mock[ErrorCodesVersionSwitcher]
  val testedValidator = new CommandsValidator(ledgerId, errorCodesVersionSwitcher_mock)

  private val fixture = new Fixture(selfServiceErrorCodesEnabled =>
    new CommandsValidator(ledgerId, new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled))
  )

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "reject requests with empty submissionId" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withSubmissionId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: submission_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: submission_id",
        )
      }

      "reject requests with empty commands" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withCommands(Seq.empty),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: commands",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: commands",
        )
      }

      "not allow missing ledgerId" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withLedgerId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: ledger_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: ledger_id",
        )
      }

      "tolerate a missing workflowId" in {

        testedValidator.validateCommands(
          api.commands.withWorkflowId(""),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(
          internal.emptyCommands.copy(
            workflowId = None,
            commands = internal.emptyCommands.commands.copy(commandsReference = ""),
          )
        )
      }

      "not allow missing applicationId" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withApplicationId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: application_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: application_id",
        )
      }

      "not allow missing commandId" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withCommandId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: command_id",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: command_id",
        )
      }

      "not allow missing submitter" in {

        fixture.testRequestFailure(
          _.validateCommands(
            api.commands.withParty(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = """Missing field: party or act_as""",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: party or act_as",
        )
      }

      "correctly read and deduplicate multiple submitters" in {

        val result = testedValidator
          .validateCommands(
            api.commands
              .withParty("alice")
              .addActAs("bob")
              .addReadAs("alice")
              .addReadAs("charlie")
              .addReadAs("bob"),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          )
        inside(result) { case Right(cmd) =>
          // actAs parties are gathered from "party" and "readAs" fields
          cmd.actAs shouldEqual Set("alice", "bob")
          // readAs should exclude all parties that are already actAs parties
          cmd.readAs shouldEqual Set("charlie")
        }
      }

      "tolerate a single submitter specified in the actAs fields" in {

        testedValidator
          .validateCommands(
            api.commands.withParty("").addActAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ) shouldEqual Right(internal.emptyCommands)
      }

      "tolerate a single submitter specified in party, actAs, and readAs fields" in {

        testedValidator
          .validateCommands(
            api.commands.withParty(api.submitter).addActAs(api.submitter).addReadAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ) shouldEqual Right(internal.emptyCommands)
      }

      "advance ledger time if minLedgerTimeAbs is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)

        testedValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeAbs = Some(TimestampConversion.fromInstant(minLedgerTimeAbs))
          ),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "advance ledger time if minLedgerTimeRel is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)

        testedValidator.validateCommands(
          api.commands.copy(
            minLedgerTimeRel = Some(DurationConversion.toProto(internal.timeDelta))
          ),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(withLedgerTime(internal.emptyCommands, minLedgerTimeAbs))
      }

      "transform valid deduplication into correct internal structure" in {
        val deduplicationDuration = Duration.of(10, 0)
        forAll(
          Table[DeduplicationPeriodProto, DeduplicationPeriod](
            ("input proto deduplication", "valid model deduplication"),
            DeduplicationPeriodProto.DeduplicationTime(deduplicationDuration) -> DeduplicationPeriod
              .DeduplicationDuration(JDuration.ofSeconds(10)),
            DeduplicationPeriodProto.DeduplicationDuration(
              deduplicationDuration
            ) -> DeduplicationPeriod
              .DeduplicationDuration(JDuration.ofSeconds(10)),
            DeduplicationPeriodProto.Empty -> DeduplicationPeriod.DeduplicationDuration(
              internal.maxDeduplicationDuration
            ),
          )
        ) {
          case (
                sentDeduplication: DeduplicationPeriodProto,
                expectedDeduplication: DeduplicationPeriod,
              ) =>
            val result = testedValidator.validateCommands(
              api.commands.copy(deduplicationPeriod = sentDeduplication),
              internal.ledgerTime,
              internal.submittedAt,
              Some(internal.maxDeduplicationDuration),
            )
            inside(result) { case Right(valid) =>
              valid.deduplicationPeriod shouldBe (expectedDeduplication)
            }
        }
      }

      "not allow negative deduplication duration" in {
        forAll(
          Table(
            "deduplication period",
            DeduplicationPeriodProto.DeduplicationTime(Duration.of(-1, 0)),
            DeduplicationPeriodProto.DeduplicationDuration(Duration.of(-1, 0)),
          )
        ) { deduplication =>
          fixture.testRequestFailure(
            _.validateCommands(
              api.commands.copy(deduplicationPeriod = deduplication),
              internal.ledgerTime,
              internal.submittedAt,
              Some(internal.maxDeduplicationDuration),
            ),
            expectedCodeV1 = INVALID_ARGUMENT,
            expectedDescriptionV1 = "Invalid field deduplication_period: Duration must be positive",
            expectedCodeV2 = INVALID_ARGUMENT,
            expectedDescriptionV2 =
              "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field deduplication_period: Duration must be positive",
          )
        }
      }

      "not allow deduplication duration exceeding maximum deduplication duration" in {
        val durationSecondsExceedingMax =
          internal.maxDeduplicationDuration.plusSeconds(1).getSeconds
        forAll(
          Table(
            "deduplication period",
            DeduplicationPeriodProto.DeduplicationTime(Duration.of(durationSecondsExceedingMax, 0)),
            DeduplicationPeriodProto.DeduplicationDuration(
              Duration.of(durationSecondsExceedingMax, 0)
            ),
          )
        ) { deduplication =>
          fixture.testRequestFailure(
            _.validateCommands(
              api.commands
                .copy(deduplicationPeriod = deduplication),
              internal.ledgerTime,
              internal.submittedAt,
              Some(internal.maxDeduplicationDuration),
            ),
            expectedCodeV1 = INVALID_ARGUMENT,
            expectedDescriptionV1 =
              s"Invalid field deduplication_period: The given deduplication duration of ${java.time.Duration
                .ofSeconds(durationSecondsExceedingMax)} exceeds the maximum deduplication time of ${internal.maxDeduplicationDuration}",
            expectedCodeV2 = INVALID_ARGUMENT,
            expectedDescriptionV2 =
              "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field deduplication_period: The given deduplication duration of PT24H1S exceeds the maximum deduplication time of PT24H",
          )
        }
      }

      "default to maximum deduplication duration if deduplication is missing" in {

        testedValidator.validateCommands(
          api.commands.copy(deduplicationPeriod = DeduplicationPeriodProto.Empty),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(
          internal.emptyCommands.copy(
            deduplicationPeriod =
              DeduplicationPeriod.DeduplicationDuration(internal.maxDeduplicationDuration)
          )
        )
      }

      "not allow missing ledger configuration" in {

        fixture.testRequestFailure(
          _.validateCommands(api.commands, internal.ledgerTime, internal.submittedAt, None),
          expectedCodeV1 = UNAVAILABLE,
          expectedDescriptionV1 = "The ledger configuration is not available.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_CONFIGURATION_NOT_FOUND(11,0): The ledger configuration is not available.",
        )
      }
    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Ref.ContractIdString.assertFromString("#coid")

        val input = Value(Sum.ContractId(coid))
        val expected = Lf.ValueContractId(Lf.ContractId.V0(coid))

        validateValue(input) shouldEqual Right(expected)
      }
    }

    "validating party values" should {
      "convert valid party" in {
        validateValue(DomainMocks.values.validApiParty) shouldEqual Right(
          DomainMocks.values.validLfParty
        )
      }

      "reject non valid party" in {
        requestMustFailWith(
          validateValue(DomainMocks.values.invalidApiParty),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg,
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
              Lf.ValueNumeric(
                Numeric
                  .assertFromBigDecimal(Numeric.Scale.assertFromInt(expectedScale), BigDecimal(s))
              )
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
            "1" + "0" * 27 + "." + "0" * 11 + "1" -> 12,
          )

        forEvery(signs) { sign =>
          forEvery(absoluteValues) { (absoluteValue, _) =>
            val s = sign + absoluteValue
            val input = Value(Sum.Numeric(s))
            requestMustFailWith(
              validateValue(input),
              INVALID_ARGUMENT,
              s"""Invalid argument: Could not read Numeric string "$s"""",
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
              s"""Invalid argument: Could not read Numeric string "$s"""",
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

        forEvery(testCases) { case (long, timestamp) =>
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
          Long.MaxValue,
        )

        forEvery(testCases) { long =>
          val input = Value(Sum.Timestamp(long))
          requestMustFailWith(
            validateValue(input),
            INVALID_ARGUMENT,
            s"Invalid argument: out of bound Timestamp $long",
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

        forEvery(testCases) { case (int, date) =>
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
          Int.MaxValue,
        )

        forEvery(testCases) { int =>
          val input = Value(Sum.Date(int))
          requestMustFailWith(
            validateValue(input),
            INVALID_ARGUMENT,
            s"Invalid argument: out of bound Date $int",
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
              Record(Some(api.identifier), Seq(RecordField(api.label, Some(Value(api.int64)))))
            )
          )
        val expected =
          Lf.ValueRecord(
            Some(DomainMocks.identifier),
            ImmArray(Some(DomainMocks.label) -> DomainMocks.values.int64),
          )
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
          DomainMocks.values.int64,
        )
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
          Lf.ValueList(FrontStack(DomainMocks.values.int64, DomainMocks.values.int64))
        validateValue(list) shouldEqual Right(expected)
      }

      "reject lists containing invalid values" in {
        val input = Value(
          Sum.List(
            ApiList(Seq(DomainMocks.values.validApiParty, DomainMocks.values.invalidApiParty))
          )
        )
        requestMustFailWith(
          validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg,
        )
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
          DomainMocks.values.invalidPartyMsg,
        )
      }
    }

    "validating map values" should {
      "convert empty maps" in {
        val input = Value(Sum.Map(ApiMap(List.empty)))
        val expected = Lf.ValueTextMap(SortedLookupList.Empty)
        validateValue(input) shouldEqual Right(expected)
      }

      "convert valid maps" in {
        val entries = (1 until 5)
          .map { x =>
            Utf8.sha256(x.toString) -> x.toLong
          }
          .to(ImmArray)
        val apiEntries = entries.map { case (k, v) =>
          ApiMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.Map(ApiMap(apiEntries.toSeq)))
        val lfEntries = entries.map { case (k, v) => k -> Lf.ValueInt64(v) }
        val expected =
          Lf.ValueTextMap(SortedLookupList.fromImmArray(lfEntries).getOrElse(unexpectedError))

        validateValue(input) shouldEqual Right(expected)
      }

      "reject maps with repeated keys" in {
        val entries = (1 +: (1 until 5))
          .map { x =>
            Utf8.sha256(x.toString) -> x.toLong
          }
          .to(ImmArray)
        val apiEntries = entries.map { case (k, v) =>
          ApiMap.Entry(k, Some(Value(Sum.Int64(v))))
        }
        val input = Value(Sum.Map(ApiMap(apiEntries.toSeq)))
        requestMustFailWith(
          validateValue(input),
          INVALID_ARGUMENT,
          s"Invalid argument: key ${Utf8.sha256("1")} duplicated when trying to build map",
        )
      }

      "reject maps containing invalid value" in {
        val apiEntries =
          List(
            ApiMap.Entry("1", Some(DomainMocks.values.validApiParty)),
            ApiMap.Entry("2", Some(DomainMocks.values.invalidApiParty)),
          )
        val input = Value(Sum.Map(ApiMap(apiEntries)))
        requestMustFailWith(
          validateValue(input),
          INVALID_ARGUMENT,
          DomainMocks.values.invalidPartyMsg,
        )
      }
    }

  }

}
