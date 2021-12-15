// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => proto}

import scala.concurrent.Future

trait ReadWriteServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  protected def serviceCallWithMainActorUser(
      userPrefix: String,
      right: proto.Right.Kind,
  ): Future[Any] =
    createUserByAdmin(userPrefix + mainActor, Vector(proto.Right(right)))
      .flatMap { case (_, token) => serviceCallWithoutApplicationId(token) }

  it should "deny calls with an expired read/write token" in {
    expectUnauthenticated(serviceCallWithToken(canActAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" in {
    expectSuccess(serviceCallWithToken(canActAsMainActor))
  }
  it should "allow calls with user token that can-act-as main actor" in {
    expectSuccess(
      serviceCallWithMainActorUser("u1", proto.Right.Kind.CanActAs(proto.Right.CanActAs(mainActor)))
    )
  }
  it should "deny calls with user token that can-read-as main actor" in {
    expectPermissionDenied(
      serviceCallWithMainActorUser(
        "u2",
        proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(mainActor)),
      )
    )
  }
  it should "deny calls with 'participant_admin' user token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminStandardJWT))
  }
  it should "deny calls with non-expired 'unknown_user' user token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }
  it should "deny calls with explicitly non-expired read-only token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActorExpiresTomorrow))
  }
  it should "deny calls with read-only token without expiration" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsMainActor))
  }

  it should "allow calls with the correct ledger ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomParticipantId))
  }
  it should "allow calls with the correct application ID" in {
    expectSuccess(serviceCallWithToken(canActAsMainActorActualApplicationId))
  }
  it should "deny calls with a random application ID" in {
    expectPermissionDenied(serviceCallWithToken(canActAsMainActorRandomApplicationId))
  }
  it should "allow calls with an empty application ID for a token with an application id" in {
    expectSuccess(serviceCallWithoutApplicationId(canActAsMainActorActualApplicationId))
  }
  it should "deny calls with an empty application ID for a token without an application id" in {
    expectInvalidArgument(serviceCallWithoutApplicationId(canActAsMainActor))
  }
}
