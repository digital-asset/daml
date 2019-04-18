// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import java.time.Instant

import com.digitalasset.api.util.TimestampConversion
import com.digitalasset.ledger.api.DomainMocks.{applicationId, commandId, workflowId}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{List => ApiList, _}
import com.digitalasset.ledger.api.{DomainMocks, domain}
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import io.grpc.Status.Code.{INVALID_ARGUMENT, NOT_FOUND}
import org.scalatest.WordSpec

import scala.collection.immutable
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandSubmissionRequestValidatorTest extends WordSpec with ValidatorTestUtils {

  val ledgerId = "ledger-id"

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

    val emptyCommands = domain.Commands(
      domain.LedgerId(ledgerId),
      Some(workflowId),
      applicationId,
      commandId,
      DomainMocks.party,
      let,
      mrt,
      immutable.Seq.empty
    )
  }

  val sut = new CommandSubmissionRequestValidator(
    ledgerId,
    new IdentifierResolver(_ => Future.successful(None)))

  def recordFieldWithValue(value: Value) = RecordField("label", Some(value))

  "CommandSubmissionRequestValidator" when {
    "validating command submission requests" should {
      "convert valid requests with empty commands" in {
        sut.validateCommands(api.commands) shouldEqual Right(internal.emptyCommands)
      }

      "not allow missing ledgerId" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.withLedgerId("")),
          NOT_FOUND,
          "Ledger ID '' not found. Actual Ledger ID is 'ledger-id'.")
      }

      "tolerate a missing workflowId" in {
        sut.validateCommands(api.commands.withWorkflowId("")) shouldEqual Right(
          internal.emptyCommands.copy(workflowId = None))
      }

      "not allow missing applicationId" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.withApplicationId("")),
          INVALID_ARGUMENT,
          "Missing field: application_id")
      }

      "not allow missing commandId" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.withCommandId("")),
          INVALID_ARGUMENT,
          "Missing field: command_id")
      }

      "not allow missing submitter" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.withParty("")),
          INVALID_ARGUMENT,
          "Invalid field party: Expected a non-empty string")
      }

      "not allow missing let" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.copy(ledgerEffectiveTime = None)),
          INVALID_ARGUMENT,
          "Missing field: ledger_effective_time")

      }

      "not allow missing mrt" in {
        requestMustFailWith(
          sut.validateCommands(api.commands.copy(maximumRecordTime = None)),
          INVALID_ARGUMENT,
          "Missing field: maximum_record_time")
      }
    }

    "validating record values" should {
      "convert valid records" in {
        val record =
          Value(
            Sum.Record(
              Record(Some(api.identifier), Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          domain.Value.RecordValue(
            Some(DomainMocks.identifier),
            immutable.Seq(domain.RecordField(Some(DomainMocks.label), DomainMocks.values.int64)))
        sut.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing identifiers in records" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField(api.label, Some(Value(api.int64)))))))
        val expected =
          domain.Value.RecordValue(
            None,
            immutable.Seq(domain.RecordField(Some(DomainMocks.label), DomainMocks.values.int64)))
        sut.validateValue(record) shouldEqual Right(expected)
      }

      "tolerate missing labels in record fields" in {
        val record =
          Value(Sum.Record(Record(None, Seq(RecordField("", Some(Value(api.int64)))))))
        val expected =
          domain.Value
            .RecordValue(None, immutable.Seq(domain.RecordField(None, DomainMocks.values.int64)))
        sut.validateValue(record) shouldEqual Right(expected)
      }

      "not allow missing record values" in {
        val record =
          Value(Sum.Record(Record(Some(api.identifier), Seq(RecordField(api.label, None)))))
        requestMustFailWith(sut.validateValue(record), INVALID_ARGUMENT, "Missing field: value")
      }

    }

    "validating variant values" should {

      "convert valid variants" in {
        val variant =
          Value(Sum.Variant(Variant(Some(api.identifier), api.constructor, Some(Value(api.int64)))))
        val expected = domain.Value.VariantValue(
          Some(DomainMocks.identifier),
          DomainMocks.values.constructor,
          DomainMocks.values.int64)
        sut.validateValue(variant) shouldEqual Right(expected)
      }

      "tolerate missing identifiers" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, Some(Value(api.int64)))))
        val expected =
          domain.Value.VariantValue(None, DomainMocks.values.constructor, DomainMocks.values.int64)

        sut.validateValue(variant) shouldEqual Right(expected)
      }

      "not allow missing constructor" in {
        val variant = Value(Sum.Variant(Variant(None, "", Some(Value(api.int64)))))
        requestMustFailWith(
          sut.validateValue(variant),
          INVALID_ARGUMENT,
          "Missing field: constructor")
      }

      "not allow missing values" in {
        val variant = Value(Sum.Variant(Variant(None, api.constructor, None)))
        requestMustFailWith(sut.validateValue(variant), INVALID_ARGUMENT, "Missing field: value")
      }

    }

    "validating list values" should {
      "convert valid lists" in {
        val list = Value(Sum.List(ApiList(Seq(Value(api.int64), Value(api.int64)))))
        val expected =
          domain.Value.ListValue(immutable.Seq(DomainMocks.values.int64, DomainMocks.values.int64))
        sut.validateValue(list) shouldEqual Right(expected)
      }
    }
  }
}
