// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.time.Duration
import java.util.UUID

trait ServiceCallWithMainActorAuthTests extends SecuredServiceCallAuthTests {

  final protected val mainActor: String = UUID.randomUUID.toString

  private val signedIncorrectly =
    ServiceCallContext(Option(toHeader(readWriteToken(mainActor), UUID.randomUUID.toString)))

  it should "deny calls authorized to read/write as the wrong party" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Present a JWT with an unknown party authorized to read/write"
      )
    ) in {
    expectPermissionDenied(serviceCall(canActAsRandomParty))
  }
  it should "deny calls authorized to read-only as the wrong party" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a JWT with an unknown party authorized to read-only")
    ) in {
    expectPermissionDenied(serviceCall(canReadAsRandomParty))
  }
  it should "deny calls with an invalid signature" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT signed by an unknown secret")
  ) in {
    expectUnauthenticated(serviceCall(signedIncorrectly))
  }

  protected val canReadAsMainActor =
    ServiceCallContext(Option(toHeader(readOnlyToken(mainActor))))
  protected val canReadAsMainActorExpired =
    ServiceCallContext(Option(toHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(mainActor)))))
  protected val canReadAsMainActorExpiresTomorrow =
    ServiceCallContext(Option(toHeader(expiringIn(Duration.ofDays(1), readOnlyToken(mainActor)))))

  protected val canActAsMainActor =
    ServiceCallContext(Option(toHeader(readWriteToken(mainActor))))
  protected val canActAsMainActorExpired =
    ServiceCallContext(Option(toHeader(expiringIn(Duration.ofDays(-1), readWriteToken(mainActor)))))
  protected val canActAsMainActorExpiresTomorrow =
    ServiceCallContext(Option(toHeader(expiringIn(Duration.ofDays(1), readWriteToken(mainActor)))))

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsMainActorActualLedgerId =
    ServiceCallContext(Option(toHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(mainActor)))))
  protected val canReadAsMainActorRandomLedgerId =
    ServiceCallContext(
      Option(toHeader(forLedgerId(UUID.randomUUID.toString, readOnlyToken(mainActor))))
    )
  protected val canReadAsMainActorActualParticipantId =
    ServiceCallContext(
      Option(toHeader(forParticipantId("sandbox-participant", readOnlyToken(mainActor))))
    )
  protected val canReadAsMainActorRandomParticipantId =
    ServiceCallContext(
      Option(
        toHeader(forParticipantId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
      )
    )
  protected val canReadAsMainActorActualApplicationId =
    ServiceCallContext(
      Option(toHeader(forApplicationId(serviceCallName, readOnlyToken(mainActor))))
    )
  protected val canReadAsMainActorRandomApplicationId =
    ServiceCallContext(
      Option(
        toHeader(forApplicationId(UUID.randomUUID.toString, readOnlyToken(mainActor)))
      )
    )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canActAsMainActorActualLedgerId =
    ServiceCallContext(Option(toHeader(forLedgerId(unwrappedLedgerId, readWriteToken(mainActor)))))
  protected val canActAsMainActorRandomLedgerId =
    ServiceCallContext(
      Option(toHeader(forLedgerId(UUID.randomUUID.toString, readWriteToken(mainActor))))
    )
  protected val canActAsMainActorActualParticipantId =
    ServiceCallContext(
      Option(toHeader(forParticipantId("sandbox-participant", readWriteToken(mainActor))))
    )
  protected val canActAsMainActorRandomParticipantId =
    ServiceCallContext(
      Option(
        toHeader(forParticipantId(UUID.randomUUID.toString, readWriteToken(mainActor)))
      )
    )
  protected val canActAsMainActorActualApplicationId =
    ServiceCallContext(
      Option(toHeader(forApplicationId(serviceCallName, readWriteToken(mainActor))))
    )
  protected val canActAsMainActorRandomApplicationId =
    ServiceCallContext(
      Option(
        toHeader(forApplicationId(UUID.randomUUID.toString, readWriteToken(mainActor)))
      )
    )

}
