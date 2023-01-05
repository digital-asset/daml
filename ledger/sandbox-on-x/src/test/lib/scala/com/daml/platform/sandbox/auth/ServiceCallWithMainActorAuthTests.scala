// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.time.Duration
import java.util.UUID

trait ServiceCallWithMainActorAuthTests extends SecuredServiceCallAuthTests {

  final protected val mainActor: String = UUID.randomUUID.toString

  private val signedIncorrectly =
    Option(toHeader(readWriteToken(mainActor), UUID.randomUUID.toString))

  it should "deny calls authorized to read/write as the wrong party" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Present a JWT with an unknown party authorized to read/write"
      )
    ) in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "deny calls authorized to read-only as the wrong party" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a JWT with an unknown party authorized to read-only")
    ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with an invalid signature" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT signed by an unknown secret")
  ) in {
    expectUnauthenticated(serviceCallWithToken(signedIncorrectly))
  }

  protected val canReadAsMainActor =
    Option(toHeader(readOnlyToken(mainActor)))
  protected val canReadAsMainActorExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(mainActor))))
  protected val canReadAsMainActorExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readOnlyToken(mainActor))))

  protected val canActAsMainActor =
    Option(toHeader(readWriteToken(mainActor)))
  protected val canActAsMainActorExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readWriteToken(mainActor))))
  protected val canActAsMainActorExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readWriteToken(mainActor))))

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsMainActorActualLedgerId =
    Option(toHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomLedgerId =
    Option(toHeader(forLedgerId(UUID.randomUUID.toString, readOnlyToken(mainActor))))
  protected val canReadAsMainActorActualParticipantId =
    Option(toHeader(forParticipantId("sandbox-participant", readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomParticipantId =
    Option(
      toHeader(forParticipantId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
    )
  protected val canReadAsMainActorActualApplicationId =
    Option(toHeader(forApplicationId(serviceCallName, readOnlyToken(mainActor))))
  protected val canReadAsMainActorRandomApplicationId =
    Option(
      toHeader(forApplicationId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
    )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canActAsMainActorActualLedgerId =
    Option(toHeader(forLedgerId(unwrappedLedgerId, readWriteToken(mainActor))))
  protected val canActAsMainActorRandomLedgerId =
    Option(toHeader(forLedgerId(UUID.randomUUID.toString, readWriteToken(mainActor))))
  protected val canActAsMainActorActualParticipantId =
    Option(toHeader(forParticipantId("sandbox-participant", readWriteToken(mainActor))))
  protected val canActAsMainActorRandomParticipantId =
    Option(
      toHeader(forParticipantId(UUID.randomUUID.toString, readWriteToken(mainActor)))
    )
  protected val canActAsMainActorActualApplicationId =
    Option(toHeader(forApplicationId(serviceCallName, readWriteToken(mainActor))))
  protected val canActAsMainActorRandomApplicationId =
    Option(
      toHeader(forApplicationId(UUID.randomUUID.toString, readWriteToken(mainActor)))
    )

}
