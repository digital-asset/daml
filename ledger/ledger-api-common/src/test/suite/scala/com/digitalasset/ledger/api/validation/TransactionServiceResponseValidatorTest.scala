// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.ledger.api.validation.TransactionServiceResponseValidatorTest._
import com.daml.lf.data.Ref.{IdString, LedgerString}
import io.grpc.StatusRuntimeException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TransactionServiceResponseValidatorTest extends AsyncWordSpec with Matchers {
  private val ledgerId = "someLedgerId"
  private val ledgerEnd = LedgerOffset.Absolute(LedgerString.assertFromString("o1"))
  private val fetchLedgerEnd =
    Map(ledgerId -> Future.successful(ledgerEnd))
      .withDefault(_ => throw new RuntimeException("Unexpected ledger id"))

  private val validator =
    new TransactionServiceResponseValidator(LedgerId(ledgerId), fetchLedgerEnd)

  private val offsetOrdering: Ordering[LedgerOffset.Absolute] =
    Ordering.by[LedgerOffset.Absolute, String](_.value)

  "TransactionServiceResponseValidator" when {
    "validating transaction responses" should {

      "return a validated response" in {
        val response = DummyResponse("o1")
        validator
          .validate[DummyResponse](response, _.offset, offsetOrdering)
          .map(_ shouldBe response)
      }

      "return a failed future if transaction offset is past ledger end" in {
        recoverToSucceededIf[StatusRuntimeException] {
          validator.validate[DummyResponse](DummyResponse("o2"), _.offset, offsetOrdering)
        }
      }
    }
  }
}

object TransactionServiceResponseValidatorTest {
  private case class DummyResponse(private val _offset: String) {
    val offset: IdString.LedgerString = LedgerString.assertFromString(_offset)
  }
}
