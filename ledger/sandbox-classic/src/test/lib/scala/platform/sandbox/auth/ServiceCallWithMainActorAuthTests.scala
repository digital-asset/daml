// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.time.Duration
import java.util.UUID

trait ServiceCallWithMainActorAuthTests extends SecuredServiceCallAuthTests {

  protected val mainActor: String = UUID.randomUUID.toString

  private val signedIncorrectly =
    Option(customTokenToHeader(readWriteToken(mainActor), UUID.randomUUID.toString))

  it should "deny calls authorized to read/write as the wrong party" in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "deny calls authorized to read-only as the wrong party" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with an invalid signature" in {
    expectUnauthenticated(serviceCallWithToken(signedIncorrectly))
  }

  protected val canReadAsMainActor =
    Option(customTokenToHeader(readOnlyToken(mainActor)))
  protected val canReadAsMainActorExpired =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(mainActor))))
  protected val canReadAsMainActorExpiresTomorrow =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(1), readOnlyToken(mainActor))))

  protected val canActAsMainActor =
    Option(customTokenToHeader(readWriteToken(mainActor)))
  protected val canActAsMainActorExpired =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(-1), readWriteToken(mainActor))))
  protected val canActAsMainActorExpiresTomorrow =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(1), readWriteToken(mainActor))))

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsMainActorActualLedgerId =
    Option(customTokenToHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomLedgerId =
    Option(customTokenToHeader(forLedgerId(UUID.randomUUID.toString, readOnlyToken(mainActor))))
  protected val canReadAsMainActorActualParticipantId =
    Option(customTokenToHeader(forParticipantId("sandbox-participant", readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomParticipantId =
    Option(
      customTokenToHeader(forParticipantId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
    )
  protected val canReadAsMainActorActualApplicationId =
    Option(customTokenToHeader(forApplicationId(serviceCallName, readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomApplicationId =
    Option(
      customTokenToHeader(forApplicationId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
    )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canActAsMainActorActualLedgerId =
    Option(customTokenToHeader(forLedgerId(unwrappedLedgerId, readWriteToken(mainActor))))
  protected val canActAsMainActorRandomLedgerId =
    Option(customTokenToHeader(forLedgerId(UUID.randomUUID.toString, readWriteToken(mainActor))))
  protected val canActAsMainActorActualParticipantId =
    Option(customTokenToHeader(forParticipantId("sandbox-participant", readWriteToken(mainActor))))
  protected val canActAsMainActorRandomParticipantId =
    Option(
      customTokenToHeader(forParticipantId(UUID.randomUUID.toString, readWriteToken(mainActor)))
    )
  protected val canActAsMainActorActualApplicationId =
    Option(customTokenToHeader(forApplicationId(serviceCallName, readWriteToken(mainActor))))
  protected val canActAsMainActorRandomApplicationId =
    Option(
      customTokenToHeader(forApplicationId(UUID.randomUUID.toString, readWriteToken(mainActor)))
    )

}
