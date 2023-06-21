// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.ledger.api.auth.CustomDamlJWTPayload
import com.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.OneAnd

final class JsonPartyValidation extends AnyWordSpec with Matchers {

  import com.daml.lf.engine.script.v1.ledgerinteraction.JsonLedgerClient._

  private def token(actAs: List[String], readAs: List[String]): CustomDamlJWTPayload =
    CustomDamlJWTPayload(
      ledgerId = None,
      participantId = None,
      applicationId = None,
      exp = None,
      admin = false,
      actAs = actAs,
      readAs = readAs,
    )

  private val alice = Ref.Party.assertFromString("Alice")
  private val bob = Ref.Party.assertFromString("Bob")

  "validateSubmitParties" should {

    "handle a single actAs party" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set.empty,
        token(List(alice), List()),
      ) shouldBe Right(None)
    }
    "fail for no actAs party" in {
      validateSubmitParties(OneAnd(alice, Set.empty), Set.empty, token(List(), List())) shouldBe a[
        Left[_, _]
      ]
    }
    "handle duplicate actAs in token" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set.empty,
        token(List(alice, alice), List()),
      ) shouldBe Right(None)
    }
    "handle duplicate actAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set(alice)),
        Set.empty,
        token(List(alice), List()),
      ) shouldBe Right(None)
    }
    "fail for missing readAs party" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(bob),
        token(List(alice), List()),
      ) shouldBe a[Left[_, _]]
    }
    "handle single readAs party" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(bob),
        token(List(alice), List(bob)),
      ) shouldBe Right(None)
    }
    "ignore party that is in readAs and actAs in token" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(),
        token(List(alice), List(alice)),
      ) shouldBe Right(None)
    }
    "ignore party that is in readAs and actAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(alice),
        token(List(alice), List()),
      ) shouldBe Right(None)
    }
    "fail if party is in actAs in submit but only in readAs in token" in {
      validateSubmitParties(OneAnd(alice, Set.empty), Set(), token(List(), List(alice))) shouldBe a[
        Left[_, _]
      ]
    }
    "handle duplicate readAs in token" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(bob),
        token(List(alice), List(bob, bob)),
      ) shouldBe Right(None)
    }
    "handle duplicate readAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(bob, bob),
        token(List(alice), List(bob)),
      ) shouldBe Right(None)
    }
    "return explicit party specification for constrained actAs" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set.empty,
        token(List(alice, bob), List()),
      ) shouldBe Right(Some(SubmitParties(OneAnd(alice, Set.empty), Set.empty)))
    }
    "return explicit party specification for constrained readAs" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set.empty,
        token(List(alice), List(bob)),
      ) shouldBe Right(Some(SubmitParties(OneAnd(alice, Set.empty), Set.empty)))
    }
  }

  "validateTokenParties" should {
    "handle a single actAs party" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(alice), List()),
      ) shouldBe Right(None)
    }
    "handle a single readAs party" in {
      validateTokenParties(OneAnd(alice, Set.empty), "", token(List(), List(alice))) shouldBe Right(
        None
      )
    }
    "handle duplicate actAs" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(alice, alice), List()),
      ) shouldBe Right(None)
    }
    "handle duplicate readAs" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(), List(alice, alice)),
      ) shouldBe Right(None)
    }
    "fail for missing readAs party" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(), List()),
      ) shouldBe a[Left[_, _]]
    }
    "handle party that is in readAs and actAs" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(alice), List(alice)),
      ) shouldBe Right(None)
    }
    "return explicit party specification for constrained parties" in {
      validateTokenParties(
        OneAnd(alice, Set.empty),
        "",
        token(List(alice, bob), List()),
      ) shouldBe Right(Some(QueryParties(OneAnd(alice, Set.empty))))
    }
  }
}
