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
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage = "Inconsistent: This was not very consistent.",
        v2_expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "INCONSISTENT(9,0): Inconsistent: This was not very consistent.; details=This was not very consistent., category=9",
      )
    }

    "convert an 'ContractsNotFound' rejection reason" in {
      assertConversion(domain.RejectionReason.ContractsNotFound(Set("cId_1", "cId_2")))(
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage = "Inconsistent: Could not lookup contracts: [cId_1, cId_2]",
        v2_expectedCode = Status.Code.NOT_FOUND.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "CONTRACTS_NOT_FOUND(11,0): Unknown contracts: [cId_1, cId_2]; notFoundContractIds=Set(cId_1, cId_2), category=11",
      )
    }

    "convert an 'InconsistentContractKeys' rejection reason" in {
      assertConversion(
        domain.RejectionReason
          .InconsistentContractKeys(Some(Value.ContractId.assertFromString("#cId1")), None)
      )(
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage =
          s"Inconsistent: Contract key lookup with different results: expected [Some(ContractId(#cId1))], actual [None]",
        v2_expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "INCONSISTENT_CONTRACT_KEY(9,0): Contract key lookup with different results: expected [Some(ContractId(#cId1))], actual [None]; category=9",
      )
    }

    "convert an 'DuplicateContractKey' rejection reason" in {
      val key = GlobalKey.assertBuild(
        Ref.Identifier.assertFromString(s"some:template:value"),
        ValueText("value"),
      )
      assertConversion(domain.RejectionReason.DuplicateContractKey(key))(
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage = s"Inconsistent: DuplicateKey: contract key is not unique",
        v2_expectedCode = Status.Code.ALREADY_EXISTS.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "DUPLICATE_CONTRACT_KEY(10,0): DuplicateKey: contract key is not unique; category=10",
      )
    }

    "convert an 'Disputed' rejection reason" in {
      assertConversion(domain.RejectionReason.Disputed("I dispute that."))(
        v1_expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1_expectedMessage = "Disputed: I dispute that.",
        v2_expectedCode = Status.Code.INTERNAL.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      )
    }

    "convert an 'OutOfQuota' rejection reason" in {
      assertConversion(domain.RejectionReason.OutOfQuota("Insert coins to continue."))(
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage = "Resources exhausted: Insert coins to continue.",
        v2_expectedCode = Status.Code.ABORTED.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage = "OUT_OF_QUOTA(2,0): Insert coins to continue.; category=2",
      )
    }

    "convert an 'PartiesNotKnownOnLedger' rejection reason" in {
      assertConversion(domain.RejectionReason.PartiesNotKnownOnLedger(Set("Alice")))(
        v1_expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1_expectedMessage = "Parties not known on ledger: [Alice]",
        v2_expectedCode = Status.Code.NOT_FOUND.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "PARTY_NOT_KNOWN_ON_LEDGER(11,0): Parties not known on ledger: [Alice]; parties$1=Set(Alice), category=11",
      )
    }

    "convert an 'PartyNotKnownOnLedger' rejection reason" in {
      assertConversion(domain.RejectionReason.PartyNotKnownOnLedger("reason"))(
        v1_expectedCode = Status.Code.INVALID_ARGUMENT.value(),
        v1_expectedMessage = "Parties not known on ledger: reason",
        v2_expectedCode = Status.Code.NOT_FOUND.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "PARTY_NOT_KNOWN_ON_LEDGER(11,0): Party not known on ledger: reason; category=11",
      )
    }

    "convert an 'SubmitterCannotActViaParticipant' rejection reason" in {
      assertConversion(domain.RejectionReason.SubmitterCannotActViaParticipant("Wrong box."))(
        v1_expectedCode = Status.Code.PERMISSION_DENIED.value(),
        v1_expectedMessage = "Submitted cannot act via participant: Wrong box.",
        v2_expectedCode = Status.Code.PERMISSION_DENIED.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      )
    }

    "convert an 'InvalidLedgerTime' rejection reason" in {
      assertConversion(domain.RejectionReason.InvalidLedgerTime("Too late."))(
        v1_expectedCode = Status.Code.ABORTED.value(),
        v1_expectedMessage = "Invalid ledger time: Too late.",
        v2_expectedCode = Status.Code.FAILED_PRECONDITION.value(),
        // TODO error codes: remove context from end of message (see [[com.daml.error.definitions.TransactionError.rpcStatus]])
        v2_expectedMessage =
          "INVALID_LEDGER_TIME(9,0): Invalid ledger time: Too late.; details=Too late., category=9",
      )
    }
  }

  private def assertConversion(actualRejectionReason: RejectionReason)(
      v1_expectedCode: Int,
      v1_expectedMessage: String,
      v2_expectedCode: Int,
      v2_expectedMessage: String,
  ): Assertion = {
    val errorFactoriesV1 = ErrorFactories(new ErrorCodesVersionSwitcher(false))
    val errorFactoriesV2 = ErrorFactories(new ErrorCodesVersionSwitcher(true))

    val convertedV1 = actualRejectionReason.toParticipantStateRejectionReason(errorFactoriesV1)
    convertedV1.code shouldBe v1_expectedCode
    convertedV1.message shouldBe v1_expectedMessage

    val convertedV2 = actualRejectionReason.toParticipantStateRejectionReason(errorFactoriesV2)
    convertedV2.code shouldBe v2_expectedCode
    convertedV2.message shouldBe v2_expectedMessage
  }
}
