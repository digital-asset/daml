// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import scala.concurrent.Future

trait ReadWriteServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  protected def serviceCallWithMainActorUser(
      userPrefix: String,
      right: proto.Right.Kind,
  ): Future[Any] =
    createUserByAdmin(userPrefix + mainActor, rights = Vector(proto.Right(right)))
      .flatMap { case (_, token) =>
        serviceCall(ServiceCallContext(token, includeApplicationId = false))
      }

  protected override def prerequisiteParties: List[String] = List(mainActor)

  it should "deny calls with an expired read/write token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired read/write JWT")
  ) in {
    expectUnauthenticated(serviceCall(canActAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with explicitly non-expired read/write token"
    ) in {
    expectSuccess(serviceCall(canActAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with read/write token without expiration"
    ) in {
    expectSuccess(serviceCall(canActAsMainActor))
  }
  it should "allow calls with user token that can-act-as main actor" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with a user token that can-act-as main actor"
    ) in {
    expectSuccess(
      serviceCallWithMainActorUser("u1", proto.Right.Kind.CanActAs(proto.Right.CanActAs(mainActor)))
    )
  }
  it should "deny calls with user token that can-read-as main actor" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a user JWT that can-read-as main actor")
    ) in {
    expectPermissionDenied(
      serviceCallWithMainActorUser(
        "u2",
        proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(mainActor)),
      )
    )
  }
  it should "deny calls with 'participant_admin' user token" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a 'participant_admin' user JWT")
  ) in {
    expectPermissionDenied(serviceCall(canReadAsAdminStandardJWT))
  }
  it should "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user JWT")
    ) in {
    expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
  }
  it should "deny calls with explicitly non-expired read-only token" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a explicitly non-expired read-only JWT")
    ) in {
    expectPermissionDenied(serviceCall(canReadAsMainActorExpiresTomorrow))
  }
  it should "deny calls with read-only token without expiration" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a read-only JWT without expiration")
  ) in {
    expectPermissionDenied(serviceCall(canReadAsMainActor))
  }
  it should "allow calls with the correct ledger ID" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with the correct ledger ID"
    ) in {
    expectSuccess(serviceCall(canActAsMainActorActualLedgerId))
  }
  it should "deny calls with a random ledger ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown ledger ID")
  ) in {
    expectPermissionDenied(serviceCall(canActAsMainActorRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with the correct participant ID"
    ) in {
    expectSuccess(serviceCall(canActAsMainActorActualParticipantId))
  }
  it should "deny calls with a random participant ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown participant ID")
  ) in {
    expectPermissionDenied(serviceCall(canActAsMainActorRandomParticipantId))
  }
  it should "allow calls with the correct application ID" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with the correct application ID"
    ) in {
    expectSuccess(serviceCall(canActAsMainActorActualApplicationId))
  }
  it should "deny calls with a random application ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown application ID")
  ) in {
    expectPermissionDenied(serviceCall(canActAsMainActorRandomApplicationId))
  }
  it should "allow calls with an application ID present in the message and a token with an empty application ID" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with an application ID present in the message and a token with an empty application ID"
    ) in {
    expectSuccess(
      serviceCall(canActAsMainActorActualApplicationId.copy(includeApplicationId = false))
    )
  }
  it should "deny calls with an application ID present in the message and a token without an application id" taggedAs securityAsset
    .setAttack(
      attackInvalidArgument(threat =
        "Present a JWT without application ID but call does not contain application ID"
      )
    ) in {
    expectInvalidArgument(serviceCall(canActAsMainActor.copy(includeApplicationId = false)))
  }
}
