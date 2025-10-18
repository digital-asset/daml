// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTPayload
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future

trait ServiceCallWithMainActorAuthTests extends SecuredServiceCallAuthTests {

  final protected val mainActor: String = "mainActor-" + UUID.randomUUID.toString
  protected val mainActorReadUser: String = "readAs-" + mainActor
  protected val mainActorActUser: String = "actAs-" + mainActor
  protected val mainActorExecuteUser: String = "executeAs-" + mainActor

  // the party id is not determined before allocating the party, so it needs to be passed as an argument after
  // retrieving the actual id via the console environment
  protected def getMainActorId(implicit env: TestConsoleEnvironment): String =
    getPartyId(mainActor)

  protected def getRandomPartyId(implicit env: TestConsoleEnvironment): String =
    getPartyId(randomParty)

  protected override def prerequisiteParties: List[String] =
    List(mainActor) ++ super.prerequisiteParties
  protected override def prerequisiteUsers: List[PrerequisiteUser] = List(
    PrerequisiteUser(mainActorReadUser, readAsParties = List(mainActor)),
    PrerequisiteUser(mainActorActUser, actAsParties = List(mainActor)),
    PrerequisiteUser(mainActorExecuteUser, executeAsParties = List(mainActor)),
  ) ++ super.prerequisiteUsers

  protected def serviceCallWithMainActorUser(
      userPrefix: String,
      rights: Vector[proto.Right.Kind],
      tokenModifier: StandardJWTPayload => StandardJWTPayload = identity,
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    createUserByAdmin(
      userPrefix + mainActor,
      rights = rights.map(proto.Right(_)),
      tokenModifier = tokenModifier,
    )
      .flatMap { case (_, context) => serviceCall(context) }
  }

  serviceCallName should {
    "deny calls authorized to read/write as the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read/write"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canActAsRandomParty))
    }

    "deny calls authorized to read-only as the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read-only"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canReadAsRandomParty))
    }
    "deny calls with an invalid signature when expecting a token for the main party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a JWT signed by an unknown secret")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(
          serviceCall(
            ServiceCallContext(
              Option(toHeader(standardToken(mainActorActUser), UUID.randomUUID.toString))
            )
          )
        )
      }
    }
  }

  protected def canReadAsMainActor =
    ServiceCallContext(Option(toHeader(standardToken(mainActorReadUser))))
  protected def canReadAsMainActorExpired =
    ServiceCallContext(
      Option(toHeader(expiringIn(Duration.ofDays(-1), standardToken(mainActorReadUser))))
    )
  protected def canReadAsMainActorExpiresInAnHour =
    ServiceCallContext(
      Option(toHeader(expiringIn(Duration.ofHours(1), standardToken(mainActorReadUser))))
    )

  protected def canActAsMainActor =
    ServiceCallContext(Option(toHeader(standardToken(mainActorActUser))))
  protected def canActAsMainActorExpired =
    ServiceCallContext(
      Option(toHeader(expiringIn(Duration.ofDays(-1), standardToken(mainActorActUser))))
    )
  protected def canActAsMainActorExpiresInAnHour =
    ServiceCallContext(
      Option(toHeader(expiringIn(Duration.ofHours(1), standardToken(mainActorActUser))))
    )

  protected def canExecuteAsMainActor =
    ServiceCallContext(Option(toHeader(standardToken(mainActorExecuteUser))))
}
