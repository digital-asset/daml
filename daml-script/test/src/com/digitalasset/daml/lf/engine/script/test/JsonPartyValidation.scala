// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.ledger.api.auth.AuthServiceJWTPayload
import com.daml.lf.data.Ref
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.engine.script.JsonLedgerClient
import scalaz.OneAnd

final class JsonPartyValidation extends AnyWordSpec with Matchers {

  import JsonLedgerClient._

  private def token(actAs: List[String], readAs: List[String]): AuthServiceJWTPayload =
    AuthServiceJWTPayload(
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
      ) shouldBe Right(())
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
      ) shouldBe Right(())
    }
    "handle duplicate actAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set(alice)),
        Set.empty,
        token(List(alice), List()),
      ) shouldBe Right(())
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
      ) shouldBe Right(())
    }
    "ignore party that is in readAs and actAs in token" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(),
        token(List(alice), List(alice)),
      ) shouldBe Right(())
    }
    "ignore party that is in readAs and actAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(alice),
        token(List(alice), List()),
      ) shouldBe Right(())
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
      ) shouldBe Right(())
    }
    "handle duplicate readAs in submit" in {
      validateSubmitParties(
        OneAnd(alice, Set.empty),
        Set(bob, bob),
        token(List(alice), List(bob)),
      ) shouldBe Right(())
    }
  }
}
