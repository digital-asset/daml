// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.Assertion

import scala.concurrent.Future

trait ReadOnlyServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  /** Allows to override what is regarded as a successful response, e.g. lookup queries for
    * commands can return a NOT_FOUND, which is fine because the result is not PERMISSION_DENIED
    */
  def successfulBehavior: Future[Any] => Future[Assertion] = expectSuccess(_: Future[Any])

  /** Flag to switch of a particular kind of test for technical reasons. See the use sites for details. */
  protected val testCanReadAsMainActor: Boolean = true

  protected override def prerequisiteParties: List[String] = List(mainActor)

  protected def serviceCallWithMainActorUser(
      userPrefix: String,
      rights: Vector[proto.Right.Kind],
  ): Future[Any] =
    createUserByAdmin(userPrefix + mainActor, rights = rights.map(proto.Right(_)))
      .flatMap { case (_, context) => serviceCall(context) }

  it should "deny calls with an expired read-only token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired read-only JWT")
  ) in {
    expectUnauthenticated(serviceCall(canReadAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read-only token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with explicitly non-expired read-only token"
    ) in {
    successfulBehavior(serviceCall(canReadAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read-only token without expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with read-only token without expiration"
    ) in {
    successfulBehavior(serviceCall(canReadAsMainActor))
  }
  it should "allow calls with user token that can-read-as main actor" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with user token that can-read-as main actor"
    ) in {
    // The completion stream tests are structured as submit-command-then-consume-completions, which requires read-write
    // rights. The tests for custom claim tokens provide the read-write tokens implicitly. That is not possible for user tokens.
    // We thus disable this test via an override in the completion stream tests.
    assume(testCanReadAsMainActor)
    successfulBehavior(
      serviceCallWithMainActorUser(
        "u1",
        Vector(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(mainActor))),
      )
    )
  }
  it should "deny calls with 'participant_admin' user token" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a 'participant_admin' user JWT")
  ) in {
    expectPermissionDenied(
      serviceCall(canReadAsAdminStandardJWT.copy(includeApplicationId = false))
    )
  }
  it should "deny calls with user token that cannot read as main actor" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Present a user JWT with permission cannot read as main actor"
      )
    ) in {
    expectPermissionDenied(serviceCallWithMainActorUser("u2", Vector.empty))
  }
  it should "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user JWT")
    ) in {
    expectPermissionDenied(
      serviceCall(canReadAsUnknownUserStandardJWT.copy(includeApplicationId = false))
    )
  }

  it should "deny calls with an expired read/write token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired read/write JWT")
  ) in {
    expectUnauthenticated(serviceCall(canActAsMainActorExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with non-expired read/write token"
    ) in {
    successfulBehavior(serviceCall(canActAsMainActorExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with read/write token without expiration"
    ) in {
    successfulBehavior(serviceCall(canActAsMainActor))
  }
  it should "allow calls with user token that can-act-as main actor" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with user token that can-act-as main actor"
    ) in {
    successfulBehavior(
      serviceCallWithMainActorUser(
        "u3",
        Vector(proto.Right.Kind.CanActAs(proto.Right.CanActAs(mainActor))),
      )
    )
  }

  it should "allow calls with the correct ledger ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can make a call with the correct ledger ID"
  ) in {
    successfulBehavior(serviceCall(canReadAsMainActorActualLedgerId))
  }
  it should "deny calls with a random ledger ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown ledger ID")
  ) in {
    expectPermissionDenied(serviceCall(canReadAsMainActorRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can makes a call with the correct participant ID"
  ) in {
    successfulBehavior(serviceCall(canReadAsMainActorActualParticipantId))
  }
  it should "deny calls with a random participant ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown participant ID")
  ) in {
    expectPermissionDenied(serviceCall(canReadAsMainActorRandomParticipantId))
  }
}
