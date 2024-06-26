// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.platform.store.backend.common.MismatchException
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsInitialization extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initialization)"

  it should "correctly handle repeated initialization" in {
    val participantId = ParticipantId(Ref.ParticipantId.assertFromString("participant"))
    val otherParticipantId = ParticipantId(Ref.ParticipantId.assertFromString("otherParticipant"))

    loggerFactory.assertLogs(
      within = {
        executeSql(
          backend.parameter.initializeParameters(
            ParameterStorageBackend.IdentityParams(
              participantId = participantId
            ),
            loggerFactory,
          )
        )
        val error = intercept[RuntimeException](
          executeSql(
            backend.parameter.initializeParameters(
              ParameterStorageBackend.IdentityParams(
                participantId = otherParticipantId
              ),
              loggerFactory,
            )
          )
        )
        executeSql(
          backend.parameter.initializeParameters(
            ParameterStorageBackend.IdentityParams(
              participantId = participantId
            ),
            loggerFactory,
          )
        )

        error.asInstanceOf[MismatchException.ParticipantId].existing shouldBe participantId
        error.asInstanceOf[MismatchException.ParticipantId].provided shouldBe otherParticipantId
      },
      assertions = _.errorMessage should include(
        "Found existing database with mismatching participantId: existing 'participant', provided 'otherParticipant'"
      ),
    )
  }
}
