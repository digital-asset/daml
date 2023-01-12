// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.lf.data.Ref.LedgerString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IdentityProviderIdSpec extends AnyWordSpec with Matchers {

  "IdentityProviderId.Default" in {
    IdentityProviderId.Default.toDb shouldBe None
    IdentityProviderId.Default.toRequestString shouldBe ""
  }

  "IdentityProviderId.Id" in {
    IdentityProviderId.Id(LedgerString.assertFromString("a123")).toDb shouldBe Some(
      IdentityProviderId.Id(LedgerString.assertFromString("a123"))
    )
    IdentityProviderId.Id(LedgerString.assertFromString("a123")).toRequestString shouldBe "a123"
  }

  "IdentityProviderId.Id.fromString" in {
    IdentityProviderId.Id.fromString("") shouldBe Left("Daml-LF Ledger String is empty")

    IdentityProviderId.Id.fromString("a" * 256) shouldBe Left(
      "Daml-LF Ledger String is too long (max: 255)"
    )

    IdentityProviderId.Id.fromString("a123") shouldBe Right(
      IdentityProviderId.Id(
        LedgerString.assertFromString("a123")
      )
    )
  }

  "IdentityProviderId.Id.assertFromString" in {
    assertThrows[IllegalArgumentException] {
      IdentityProviderId.Id.assertFromString("")
    }

    assertThrows[IllegalArgumentException] {
      IdentityProviderId.Id.assertFromString("a" * 256)
    }

    IdentityProviderId.Id.assertFromString("a123") shouldBe
      IdentityProviderId.Id(
        LedgerString.assertFromString("a123")
      )
  }

  "IdentityProviderId.apply" in {
    IdentityProviderId("") shouldBe IdentityProviderId.Default
    IdentityProviderId("a123") shouldBe IdentityProviderId.Id.assertFromString("a123")
  }

  "IdentityProviderId.fromString" in {
    IdentityProviderId.fromString("") shouldBe Right(IdentityProviderId.Default)
    IdentityProviderId.fromString("a123") shouldBe Right(
      IdentityProviderId.Id.assertFromString("a123")
    )
    IdentityProviderId.fromString("a" * 256) shouldBe Left(
      "Daml-LF Ledger String is too long (max: 255)"
    )
  }

  "IdentityProviderId.fromDb" in {
    IdentityProviderId.fromDb(None) shouldBe IdentityProviderId.Default
    IdentityProviderId.fromDb(
      Some(IdentityProviderId.Id.assertFromString("a123"))
    ) shouldBe IdentityProviderId.Id.assertFromString("a123")
  }
}

object IdentityProviderIdSpec {}
