// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.lf.data.Ref
import com.daml.platform.common.MismatchException
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitialization extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  private val parameterStorageBackend: ParameterStorageBackend =
    backendFactory.createParameterStorageBackend

  behavior of "StorageBackend (initialization)"

  it should "correctly handle repeated initialization" in {
    val ledgerId = LedgerId("ledger")
    val participantId = ParticipantId(Ref.ParticipantId.assertFromString("participant"))
    val otherLedgerId = LedgerId("otherLedger")
    val otherParticipantId = ParticipantId(Ref.ParticipantId.assertFromString("otherParticipant"))

    for {
      _ <- executeSql(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = ledgerId,
            participantId = participantId,
          )
        )
      )
      error1 <- executeSql(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = otherLedgerId,
            participantId = participantId,
          )
        )
      ).failed
      error2 <- executeSql(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = ledgerId,
            participantId = otherParticipantId,
          )
        )
      ).failed
      error3 <- executeSql(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = otherLedgerId,
            participantId = otherParticipantId,
          )
        )
      ).failed
      _ <- executeSql(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = ledgerId,
            participantId = participantId,
          )
        )
      )
    } yield {
      error1 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
      error2 shouldBe MismatchException.ParticipantId(participantId, otherParticipantId)
      error3 shouldBe MismatchException.LedgerId(ledgerId, otherLedgerId)
    }
  }
}
