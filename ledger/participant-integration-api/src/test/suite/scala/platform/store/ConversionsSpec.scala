// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.RejectionReason
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ValueText
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.Conversions._
import io.grpc.Status
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class ConversionsSpec extends AsyncWordSpec with Matchers {
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(
      ContextualizedLogger.get(getClass),
      LoggingContext.ForTesting,
      None,
    )

  "converting rejection reasons" should {
    "convert an 'Inconsistent' rejection reason" in {
      assertConversion(domain.RejectionReason.Inconsistent("This was not very consistent."))(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage = "Inconsistent: This was not very consistent.",
        v2expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        v2expectedMessage = "INCONSISTENT(9,0): Inconsistent: This was not very consistent.",
      )
    }

    "convert a 'ContractsNotFound' rejection reason" in {
      assertConversion(domain.RejectionReason.ContractsNotFound(Set("cId_1", "cId_2")))(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage = "Inconsistent: Could not lookup contracts: [cId_1, cId_2]",
        v2expectedCode = Status.Code.NOT_FOUND.value(),
        v2expectedMessage = "CONTRACTS_NOT_FOUND(11,0): Unknown contracts: [cId_1, cId_2]",
      )
    }

    "convert an 'InconsistentContractKeys' rejection reason" in {
      val cId = "#cId1"
      assertConversion(
        domain.RejectionReason
          .InconsistentContractKeys(Some(Value.ContractId.assertFromString(cId)), None)
      )(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage =
          s"Inconsistent: Contract key lookup with different results: expected [Some(ContractId($cId))], actual [$None]",
        v2expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        v2expectedMessage =
          s"INCONSISTENT_CONTRACT_KEY(9,0): Contract key lookup with different results: expected [Some(ContractId($cId))], actual [$None]",
      )
    }

    "convert a 'DuplicateContractKey' rejection reason" in {
      val key = GlobalKey.assertBuild(
        Ref.Identifier.assertFromString("some:template:value"),
        ValueText("value"),
      )
      assertConversion(domain.RejectionReason.DuplicateContractKey(key))(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage = "Inconsistent: DuplicateKey: contract key is not unique",
        v2expectedCode = Status.Code.ALREADY_EXISTS.value(),
        v2expectedMessage = "DUPLICATE_CONTRACT_KEY(10,0): DuplicateKey: contract key is not unique",
      )
    }

    "convert a 'Disputed' rejection reason" in {
      assertConversion(domain.RejectionReason.Disputed("I dispute that."))(
        v1expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1expectedMessage = "Disputed: I dispute that.",
        v2expectedCode = Status.Code.INTERNAL.value(),
        v2expectedMessage =
          "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      )
    }

    "convert an 'OutOfQuota' rejection reason" in {
      assertConversion(domain.RejectionReason.OutOfQuota("Insert coins to continue."))(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage = "Resources exhausted: Insert coins to continue.",
        v2expectedCode = Status.Code.ABORTED.value(),
        v2expectedMessage = "OUT_OF_QUOTA(2,0): Insert coins to continue.",
      )
    }

    "convert a 'PartiesNotKnownOnLedger' rejection reason" in {
      assertConversion(domain.RejectionReason.PartiesNotKnownOnLedger(Set("Alice")))(
        v1expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1expectedMessage = "Parties not known on ledger: [Alice]",
        v2expectedCode = Status.Code.NOT_FOUND.value(),
        v2expectedMessage = "PARTY_NOT_KNOWN_ON_LEDGER(11,0): Parties not known on ledger: [Alice]",
      )
    }

    "convert a 'PartyNotKnownOnLedger' rejection reason" in {
      assertConversion(domain.RejectionReason.PartyNotKnownOnLedger("reason"))(
        v1expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1expectedMessage = "Parties not known on ledger: reason",
        v2expectedCode = Status.Code.NOT_FOUND.value(),
        v2expectedMessage = "PARTY_NOT_KNOWN_ON_LEDGER(11,0): Party not known on ledger: reason",
      )
    }

    "convert a 'SubmitterCannotActViaParticipant' rejection reason" in {
      assertConversion(domain.RejectionReason.SubmitterCannotActViaParticipant("Wrong box."))(
        v1expectedCode = Status.Code.PERMISSION_DENIED.value(),
        v1expectedMessage = "Submitted cannot act via participant: Wrong box.",
        v2expectedCode = Status.Code.PERMISSION_DENIED.value(),
        v2expectedMessage =
          "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      )
    }

    "convert an 'InvalidLedgerTime' rejection reason" in {
      assertConversion(domain.RejectionReason.InvalidLedgerTime("Too late."))(
        v1expectedCode = Status.Code.ABORTED.value(),
        v1expectedMessage = "Invalid ledger time: Too late.",
        v2expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        v2expectedMessage = "INVALID_LEDGER_TIME(9,0): Invalid ledger time: Too late.",
      )
    }
  }

  private def assertConversion(actualRejectionReason: RejectionReason)(
      v1expectedCode: Int,
      v1expectedMessage: String,
      v2expectedCode: Int,
      v2expectedMessage: String,
  ): Assertion = {
    val errorFactoriesV1 = ErrorFactories(
      new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = false)
    )
    val errorFactoriesV2 = ErrorFactories(
      new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = true)
    )

    val convertedV1 = actualRejectionReason.toParticipantStateRejectionReason(errorFactoriesV1)
    convertedV1.code shouldBe v1expectedCode
    convertedV1.message shouldBe v1expectedMessage

    val convertedV2 = actualRejectionReason.toParticipantStateRejectionReason(errorFactoriesV2)
    convertedV2.code shouldBe v2expectedCode
    convertedV2.message shouldBe v2expectedMessage
  }
}
