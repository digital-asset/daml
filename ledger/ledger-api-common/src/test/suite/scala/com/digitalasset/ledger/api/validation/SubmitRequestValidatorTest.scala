// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import java.time.{Instant, Duration => JDuration}
import com.daml.api.util.{DurationConversion, TimestampConversion}
import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.DomainMocks.{applicationId, commandId, submissionId, workflowId}
import com.daml.ledger.api.domain.{LedgerId, Commands => ApiCommands}
import com.daml.ledger.api.v1.commands.Commands.{DeduplicationPeriod => DeduplicationPeriodProto}
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{List => ApiList, Map => ApiMap, Optional => ApiOptional, _}
import com.daml.ledger.api.{DeduplicationPeriod, DomainMocks}
import com.daml.lf.command.{
  ContractMetadata,
  DisclosedContract,
  ApiCommand => LfCommand,
  ApiCommands => LfCommands,
}
import com.daml.lf.data._
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.{Value => Lf}
import com.daml.platform.error.definitions.LedgerApiErrors
import com.google.protobuf.duration.Duration
import com.google.protobuf.empty.Empty
import io.grpc.Status.Code.{INVALID_ARGUMENT, NOT_FOUND}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.tag._

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class SubmitRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks
    with MockitoSugar
    with ArgumentMatchersSugar {
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
      applicationId = applicationId,
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

    val templateId: Ref.Identifier = Ref.Identifier(
      Ref.PackageId.assertFromString("package"),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("module"),
        Ref.DottedName.assertFromString("entity"),
      ),
    )

    val disclosedContracts: ImmArray[DisclosedContract] = ImmArray(
      DisclosedContract(
        templateId,
        Lf.ContractId.V1.assertFromString("00" + "00" * 32),
        ValueRecord(
          Some(templateId),
          ImmArray.empty,
        ),
        ContractMetadata(Time.Timestamp.now(), None, ImmArray.Empty),
      )
    )

    val emptyCommands = ApiCommands(
      ledgerId = Some(ledgerId),
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
          LfCommand.Create(
            templateId,
            Lf.ValueRecord(
              Option(
                templateId
              ),
              ImmArray((Option(Ref.Name.assertFromString("something")), Lf.ValueTrue)),
            ),
          )
        ),
        Time.Timestamp.assertFromInstant(ledgerTime),
        workflowId.unwrap,
      ),
      disclosedContracts,
    )
  }

  private[this] def withLedgerTime(commands: ApiCommands, let: Instant): ApiCommands =
    commands.copy(
      commands = commands.commands.copy(
        ledgerEffectiveTime = Time.Timestamp.assertFromInstant(let)
      )
    )

  private def unexpectedError = sys.error("unexpected error")

  private val validateDisclosedContractsMock = mock[ValidateDisclosedContracts]

  when(validateDisclosedContractsMock(any[Commands])(any[ContextualizedErrorLogger]))
    .thenReturn(Right(internal.disclosedContracts))

  private val testedCommandValidator =
    new CommandsValidator(ledgerId, validateDisclosedContractsMock)
  private val testedValueValidator = ValueValidator

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "validate a complete request" in {
        testedCommandValidator.validateCommands(
          api.commands,
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(internal.emptyCommands)
      }

      "tolerate a missing submissionId" in {
        testedCommandValidator.validateCommands(
          api.commands.withSubmissionId(""),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(internal.emptyCommands.copy(submissionId = None))
      }

      "reject requests with empty commands" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withCommands(Seq.empty),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: commands",
          metadata = Map.empty,
        )
      }

      "allow missing ledgerId" in {
        testedCommandValidator.validateCommands(
          api.commands.withLedgerId(""),
          internal.ledgerTime,
          internal.submittedAt,
          Some(internal.maxDeduplicationDuration),
        ) shouldEqual Right(internal.emptyCommands.copy(ledgerId = None))
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
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withApplicationId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: application_id",
          metadata = Map.empty,
        )
      }

      "not allow missing commandId" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withCommandId(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: command_id",
          metadata = Map.empty,
        )
      }

      "not allow missing submitter" in {
        requestMustFailWith(
          request = testedCommandValidator.validateCommands(
            api.commands.withParty(""),
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: party or act_as",
          metadata = Map.empty,
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
          requestMustFailWith(
            testedCommandValidator.validateCommands(
              api.commands.copy(deduplicationPeriod = deduplication),
              internal.ledgerTime,
              internal.submittedAt,
              Some(internal.maxDeduplicationDuration),
            ),
            code = INVALID_ARGUMENT,
            description =
              "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field deduplication_period: Duration must be positive",
            metadata = Map.empty,
          )
        }
      }

      "allow deduplication duration exceeding maximum deduplication duration" in {
        val durationSecondsExceedingMax =
          internal.maxDeduplicationDuration.plusSeconds(1)
        forAll(
          Table(
            "deduplication period",
            DeduplicationPeriodProto.DeduplicationTime(
              Duration.of(durationSecondsExceedingMax.getSeconds, 0)
            ),
            DeduplicationPeriodProto.DeduplicationDuration(
              Duration.of(durationSecondsExceedingMax.getSeconds, 0)
            ),
          )
        ) { deduplicationPeriod =>
          val commandsWithDeduplicationDuration = api.commands
            .copy(deduplicationPeriod = deduplicationPeriod)
          testedCommandValidator.validateCommands(
            commandsWithDeduplicationDuration,
            internal.ledgerTime,
            internal.submittedAt,
            Some(internal.maxDeduplicationDuration),
          ) shouldBe Right(
            internal.emptyCommands.copy(
              deduplicationPeriod =
                DeduplicationPeriod.DeduplicationDuration(durationSecondsExceedingMax)
            )
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
        requestMustFailWith(
          request = testedCommandValidator
            .validateCommands(api.commands, internal.ledgerTime, internal.submittedAt, None),
          code = NOT_FOUND,
          description =
            "LEDGER_CONFIGURATION_NOT_FOUND(11,0): The ledger configuration could not be retrieved.",
          metadata = Map.empty,
        )
      }

      "fail when disclosed contracts validation fails" in {
        when(validateDisclosedContractsMock(any[Commands])(any[ContextualizedErrorLogger]))
          .thenReturn(
            Left(
              LedgerApiErrors.RequestValidation.InvalidField
                .Reject("some failed", "some message")
                .asGrpcError
            )
          )
        requestMustFailWith(
          request = testedCommandValidator
            .validateCommands(
              api.commands,
              internal.ledgerTime,
              internal.submittedAt,
              Some(internal.maxDeduplicationDuration),
            ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field some failed: some message",
          metadata = Map.empty,
        )
      }
    }

    "validating contractId values" should {
      "succeed" in {

        val coid = Lf.ContractId.V1.assertFromString("00" + "00" * 32)

        val input = Value(Sum.ContractId(coid.coid))
        val expected = Lf.ValueContractId(coid)

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
        requestMustFailWith(
          request = testedValueValidator.validateValue(DomainMocks.values.invalidApiParty),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
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
            requestMustFailWith(
              request = testedValueValidator.validateValue(input),
              code = INVALID_ARGUMENT,
              description =
                s"""INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Could not read Numeric string "$s"""",
              metadata = Map.empty,
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
              request = testedValueValidator.validateValue(input),
              code = INVALID_ARGUMENT,
              description =
                s"""INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Could not read Numeric string "$s"""",
              metadata = Map.empty,
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
          requestMustFailWith(
            request = testedValueValidator.validateValue(input),
            code = INVALID_ARGUMENT,
            description =
              s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: out of bound Timestamp $long",
            metadata = Map.empty,
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
          requestMustFailWith(
            request = testedValueValidator.validateValue(input),
            code = INVALID_ARGUMENT,
            description =
              s"INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: out of bound Date $int",
            metadata = Map.empty,
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
        requestMustFailWith(
          request = testedValueValidator.validateValue(record),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
          metadata = Map.empty,
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
        requestMustFailWith(
          request = testedValueValidator.validateValue(variant),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: constructor",
          metadata = Map.empty,
        )
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        requestMustFailWith(
          request = testedValueValidator.validateValue(variant),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: value",
          metadata = Map.empty,
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
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
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
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
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
        requestMustFailWith(
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: key 6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b duplicated when trying to build map",
          metadata = Map.empty,
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
          request = testedValueValidator.validateValue(input),
          code = INVALID_ARGUMENT,
          description =
            """INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x40 in Daml-LF Party "p@rty"""",
          metadata = Map.empty,
        )
      }
    }
  }
}
