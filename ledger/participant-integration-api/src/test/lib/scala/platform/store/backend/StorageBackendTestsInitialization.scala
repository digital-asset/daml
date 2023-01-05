// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.lf.data.Ref
import com.daml.platform.common.MismatchException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitialization extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initialization)"

  it should "correctly handle repeated initialization" in {
    val ledgerId = LedgerId("ledger")
    val participantId = ParticipantId(Ref.ParticipantId.assertFromString("participant"))
    val otherLedgerId = LedgerId("otherLedger")
    val otherParticipantId = ParticipantId(Ref.ParticipantId.assertFromString("otherParticipant"))

    executeSql(
      backend.parameter.initializeParameters(
        ParameterStorageBackend.IdentityParams(
          ledgerId = ledgerId,
          participantId = participantId,
        )
      )
    )
    val error1 = intercept[RuntimeException](
      executeSql(
        backend.parameter.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = otherLedgerId,
            participantId = participantId,
          )
        )
      )
    )
    val error2 = intercept[RuntimeException](
      executeSql(
        backend.parameter.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = ledgerId,
            participantId = otherParticipantId,
          )
        )
      )
    )
    val error3 = intercept[RuntimeException](
      executeSql(
        backend.parameter.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = otherLedgerId,
            participantId = otherParticipantId,
          )
        )
      )
    )
    executeSql(
      backend.parameter.initializeParameters(
        ParameterStorageBackend.IdentityParams(
          ledgerId = ledgerId,
          participantId = participantId,
        )
      )
    )

    error1 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
    error2 shouldBe MismatchException.ParticipantId(participantId, otherParticipantId)
    error3 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
  }
}
