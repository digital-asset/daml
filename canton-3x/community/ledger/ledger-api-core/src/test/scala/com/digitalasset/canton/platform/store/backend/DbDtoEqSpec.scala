// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DbDtoEqSpec extends AnyWordSpec with Matchers {

  import DbDtoEq.*

  "DbDtoEq" should {

    "compare DbDto when used with `decided` keyword" in {

      val dto0 = DbDto.ConfigurationEntry(
        ledger_offset = "",
        recorded_at = 0,
        submission_id = "",
        typ = "",
        configuration = Array[Byte](),
        rejection_reason = None,
      )

      val dto1 = dto0.copy()
      val dto2 = dto0.copy()

      dto0 should equal(dto0) // Works due to object equality shortcut
      dto1 shouldNot equal(dto2) // As equality is overridden to be false with DbDto
      dto1 should equal(dto2)(decided by DbDtoEq)
      List(dto1) should equal(List(dto2))(decided by DbDtoSeqEq)

    }

  }

}
