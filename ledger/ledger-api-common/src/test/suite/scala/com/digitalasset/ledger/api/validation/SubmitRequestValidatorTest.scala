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
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code.{FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND, UNAVAILABLE}
import org.mockito.MockitoSugar
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
      submissionId = Some(submissionId),
      actAs = Set(DomainMocks.party),
      readAs = Set.empty,
      submittedAt = Time.Timestamp.assertFromInstant(submittedAt),
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

  private def unexpectedError = sys.error("unexpected error")

  private val errorCodesVersionSwitcher_mock = mock[ErrorCodesVersionSwitcher]

  private val commandValidatorFixture = new ValidatorFixture(selfServiceErrorCodesEnabled =>
    new CommandsValidator(ledgerId, new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled))
  )
  private val valueValidatorFixture = new ValidatorFixture(selfServiceErrorCodesEnabled => {
    val errorCodesVersionSwitcher = new ErrorCodesVersionSwitcher(selfServiceErrorCodesEnabled)
    val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
    new ValueValidator(
      errorFactories,
      FieldValidations(errorFactories),
    )
  })

  private val testedCommandValidator =
    new CommandsValidator(ledgerId, errorCodesVersionSwitcher_mock)
  private val testedValueValidator = {
    val errorFactories = ErrorFactories(errorCodesVersionSwitcher_mock)
    new ValueValidator(
      errorFactories,
      FieldValidations(errorFactories),
    )
  }

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "tolerate a missing submissionId" in {
        testedCommandValidator.validateCommands(
          api.commands.withSubmissionId(""),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(internal.emptyCommands.copy(submissionId = None))
      }

      "reject requests with empty commands" in {
        commandValidatorFixture.testRequestFailure(
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

        commandValidatorFixture.testRequestFailure(
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

        testedCommandValidator.validateCommands(
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

        commandValidatorFixture.testRequestFailure(
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

        commandValidatorFixture.testRequestFailure(
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

        commandValidatorFixture.testRequestFailure(
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

        val result = testedCommandValidator
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

        testedCommandValidator
          .validateCommands(
            api.commands.withParty("").addActAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ) shouldEqual Right(internal.emptyCommands)
      }

      "tolerate a single submitter specified in party, actAs, and readAs fields" in {

        testedCommandValidator
          .validateCommands(
            api.commands.withParty(api.submitter).addActAs(api.submitter).addReadAs(api.submitter),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ) shouldEqual Right(internal.emptyCommands)
      }

      "advance ledger time if minLedgerTimeAbs is set" in {
        val minLedgerTimeAbs = internal.ledgerTime.plus(internal.timeDelta)

        testedCommandValidator.validateCommands(
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

        testedCommandValidator.validateCommands(
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
            val result = testedCommandValidator.validateCommands(
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
          commandValidatorFixture.testRequestFailure(
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
          commandValidatorFixture.testRequestFailure(
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
            expectedCodeV2 = FAILED_PRECONDITION,
            expectedDescriptionV2 = s"INVALID_DEDUPLICATION_PERIOD(9,0): The submitted command had an invalid deduplication period: The given deduplication duration of ${java.time.Duration
              .ofSeconds(durationSecondsExceedingMax)} exceeds the maximum deduplication time of ${internal.maxDeduplicationDuration}",
            metadataV2 = Map(
              "max_deduplication_duration" -> internal.maxDeduplicationDuration.toString
            ),
          )
        }
      }

      "default to maximum deduplication duration if deduplication is missing" in {

        testedCommandValidator.validateCommands(
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

        commandValidatorFixture.testRequestFailure(
          _.validateCommands(api.commands, internal.ledgerTime, internal.submittedAt, None),
          expectedCodeV1 = UNAVAILABLE,
          expectedDescriptionV1 = "The ledger configuration is not available.",
          expectedCodeV2 = NOT_FOUND,
          expectedDescriptionV2 =
            "LEDGER_CONFIGURATION_NOT_FOUND(11,0): The ledger configuration could not be retrieved.",
        )
      }
    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Ref.ContractIdString.assertFromString("#coid")

        val input = Value(Sum.ContractId(coid))
        val expected = Lf.ValueContractId(Lf.ContractId.V0(coid))

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }
    }

    "validating party values" should {
      "convert valid party" in {
        testedValueValidator.validateValue(DomainMocks.values.validApiParty) shouldEqual Right(
          DomainMocks.values.validLfParty
        )
      }

      "reject non valid party" in {
        valueValidatorFixture.testRequestFailure(
          _.validateValue(DomainMocks.values.invalidApiParty),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = DomainMocks.values.invalidPartyMsg,
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
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
            testedValueValidator.validateValue(input) shouldEqual Right(expected)
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
            valueValidatorFixture.testRequestFailure(
              _.validateValue(input),
              expectedCodeV1 = INVALID_ARGUMENT,
              expectedDescriptionV1 = s"""Invalid argument: Could not read Numeric string "$s"""",
              expectedCodeV2 = INVALID_ARGUMENT,
              expectedDescriptionV2 =
                s"""INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Could not read Numeric string "$s"""",
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
            valueValidatorFixture.testRequestFailure(
              _.validateValue(input),
              expectedCodeV1 = INVALID_ARGUMENT,
              expectedDescriptionV1 = s"""Invalid argument: Could not read Numeric string "$s"""",
              expectedCodeV2 = INVALID_ARGUMENT,
              expectedDescriptionV2 =
                s"""INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Could not read Numeric string "$s"""",
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
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
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
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
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
          valueValidatorFixture.testRequestFailure(
            _.validateValue(input),
            expectedCodeV1 = INVALID_ARGUMENT,
            expectedDescriptionV1 = s"Invalid argument: out of bound Timestamp $long",
            expectedCodeV2 = INVALID_ARGUMENT,
            expectedDescriptionV2 =
              s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: out of bound Timestamp $long",
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
          testedValueValidator.validateValue(input) shouldEqual Right(expected)
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
          valueValidatorFixture.testRequestFailure(
            _.validateValue(input),
            expectedCodeV1 = INVALID_ARGUMENT,
            expectedDescriptionV1 = s"Invalid argument: out of bound Date $int",
            expectedCodeV2 = INVALID_ARGUMENT,
            expectedDescriptionV2 =
              s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: out of bound Date $int",
          )
        }
      }
    }

    "validating boolean values" should {
      "accept any of them" in {
        testedValueValidator.validateValue(Value(Sum.Bool(true))) shouldEqual Right(Lf.ValueTrue)
        testedValueValidator.validateValue(Value(Sum.Bool(false))) shouldEqual Right(Lf.ValueFalse)
      }
    }

    "validating unit values" should {
      "succeed" in {
        testedValueValidator.validateValue(Value(Sum.Unit(Empty()))) shouldEqual Right(Lf.ValueUnit)
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
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing identifiers in records" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          Lf.ValueRecord(None, ImmArray(Some(DomainMocks.label) -> DomainMocks.values.int64))
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing labels in record fields" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField("", Some(Value(api.int64)))))))
        val expected =
          ValueRecord(None, ImmArray(None -> DomainMocks.values.int64))
        testedValueValidator.validateValue(record) shouldEqual Right(expected)
      }

      "not allow missing record values" in {
        val record =
          Value(Sum.Record(Record(Some(api.identifier), Seq(RecordField(api.label, None)))))
        valueValidatorFixture.testRequestFailure(
          _.validateValue(record),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: value",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
        )
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
        testedValueValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "tolerate missing identifiers" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, Some(Value(api.int64)))))
        val expected =
          Lf.ValueVariant(None, DomainMocks.values.constructor, DomainMocks.values.int64)

        testedValueValidator.validateValue(variant) shouldEqual Right(expected)
      }

      "not allow missing constructor" in {
        val variant = Value(Sum.Variant(Variant(None, "", Some(Value(api.int64)))))
        valueValidatorFixture.testRequestFailure(
          _.validateValue(variant),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: constructor",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: constructor",
        )
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        valueValidatorFixture.testRequestFailure(
          _.validateValue(variant),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = "Missing field: value",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
        )
      }

    }

    "validating list values" should {
      "convert empty lists" in {
        val input = Value(Sum.List(ApiList(List.empty)))
        val expected =
          Lf.ValueNil

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid lists" in {
        val list = Value(Sum.List(ApiList(Seq(Value(api.int64), Value(api.int64)))))
        val expected =
          Lf.ValueList(FrontStack(DomainMocks.values.int64, DomainMocks.values.int64))
        testedValueValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject lists containing invalid values" in {
        val input = Value(
          Sum.List(
            ApiList(Seq(DomainMocks.values.validApiParty, DomainMocks.values.invalidApiParty))
          )
        )
        valueValidatorFixture.testRequestFailure(
          _.validateValue(input),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = DomainMocks.values.invalidPartyMsg,
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
        )
      }
    }

    "validating optional values" should {
      "convert empty optionals" in {
        val input = Value(Sum.Optional(ApiOptional(None)))
        val expected = Lf.ValueNone

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
      }

      "convert valid non-empty optionals" in {
        val list = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.validApiParty))))
        val expected = Lf.ValueOptional(Some(DomainMocks.values.validLfParty))
        testedValueValidator.validateValue(list) shouldEqual Right(expected)
      }

      "reject optional containing invalid values" in {
        val input = Value(Sum.Optional(ApiOptional(Some(DomainMocks.values.invalidApiParty))))
        valueValidatorFixture.testRequestFailure(
          _.validateValue(input),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = DomainMocks.values.invalidPartyMsg,
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
        )
      }
    }

    "validating map values" should {
      "convert empty maps" in {
        val input = Value(Sum.Map(ApiMap(List.empty)))
        val expected = Lf.ValueTextMap(SortedLookupList.Empty)
        testedValueValidator.validateValue(input) shouldEqual Right(expected)
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

        testedValueValidator.validateValue(input) shouldEqual Right(expected)
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
        valueValidatorFixture.testRequestFailure(
          _.validateValue(input),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 =
            s"Invalid argument: key ${Utf8.sha256("1")} duplicated when trying to build map",
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: key 6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b duplicated when trying to build map",
        )
      }

      "reject maps containing invalid value" in {
        val apiEntries =
          List(
            ApiMap.Entry("1", Some(DomainMocks.values.validApiParty)),
            ApiMap.Entry("2", Some(DomainMocks.values.invalidApiParty)),
          )
        val input = Value(Sum.Map(ApiMap(apiEntries)))
        valueValidatorFixture.testRequestFailure(
          _.validateValue(input),
          expectedCodeV1 = INVALID_ARGUMENT,
          expectedDescriptionV1 = DomainMocks.values.invalidPartyMsg,
          expectedCodeV2 = INVALID_ARGUMENT,
          expectedDescriptionV2 =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
        )
      }
    }

  }

}
