// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTPayload
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule

import java.util.UUID
import scala.concurrent.Future

/** Trait for services that use multiple actAs and readAs parties. These tests only test for
  * variations in the authorized parties. They do not test for variations in expiration time, ledger
  * ID, or participant ID. It is expected that [[SyncServiceCallAuthTests]] are run on the same
  * service.
  */
trait MultiPartyServiceCallAuthTests extends SecuredServiceCallAuthTests {

  // Parties specified in the API request
  sealed case class RequestSubmitters(actAs: Seq[String], readAs: Seq[String])

  // Parties specified in the access token
  sealed case class TokenParties(actAs: List[String], readAs: List[String])

  private[this] val actorsCount: Int = 3
  private[this] val readersCount: Int = 3

  private[this] val actAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  private[this] val readAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)
  private[this] val submitters: RequestSubmitters = RequestSubmitters(actAs, readAs)

  private[this] val singleParty = UUID.randomUUID.toString

  private[this] val randomActAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  private[this] val randomReadAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)

  private[this] val singlePartyReadUser = "readAs-" + singleParty
  private[this] val singlePartyActUser = "actAs-" + singleParty
  private[this] val noPartyUser = "noPartyUser-" + UUID.randomUUID.toString
  private[this] val superUser = "superUser-" + UUID.randomUUID.toString
  private[this] val superActorUser = "superActorUser-" + UUID.randomUUID.toString
  private[this] val superReaderUser = "superReaderUser-" + UUID.randomUUID.toString
  private[this] val superUserMinusActor = "superUserMinusActor-" + UUID.randomUUID.toString
  private[this] val superUserMinusReader = "superUserMinusReader-" + UUID.randomUUID.toString
  private[this] val superUserSwapped = "superUserSwapped-" + UUID.randomUUID.toString
  private[this] val randomUser = "randomUser-" + UUID.randomUUID.toString
  private[this] val omnipotentUser = "omnipotentUser-" + UUID.randomUUID.toString

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    serviceCall(context, submitters)

  def serviceCall(context: ServiceCallContext, requestSubmitters: RequestSubmitters)(implicit
      env: TestConsoleEnvironment
  ): Future[Any]

  private[this] def serviceCallFor(
      token: StandardJWTPayload,
      requestSubmitters: RequestSubmitters,
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    val context = ServiceCallContext(
      Option(
        toHeader(token)
      )
    )
    serviceCall(context, requestSubmitters)
  }

  protected override def prerequisiteParties: List[String] =
    super.prerequisiteParties ++ actAs ++ readAs ++ randomActAs ++ randomReadAs :+ singleParty
  protected override def prerequisiteUsers: List[PrerequisiteUser] = List(
    PrerequisiteUser(singlePartyReadUser, readAsParties = List(singleParty)),
    PrerequisiteUser(singlePartyActUser, actAsParties = List(singleParty)),
    PrerequisiteUser(superUser, actAsParties = actAs, readAsParties = readAs),
    PrerequisiteUser(superActorUser, actAsParties = actAs ++ readAs),
    PrerequisiteUser(superReaderUser, readAsParties = actAs ++ readAs),
    PrerequisiteUser(
      superUserMinusActor,
      actAsParties = actAs.dropRight(1),
      readAsParties = readAs,
    ),
    PrerequisiteUser(
      superUserMinusReader,
      actAsParties = actAs,
      readAsParties = readAs.dropRight(1),
    ),
    PrerequisiteUser(superUserSwapped, actAsParties = readAs, readAsParties = actAs),
    PrerequisiteUser(randomUser, actAsParties = randomActAs, readAsParties = randomReadAs),
    PrerequisiteUser(
      omnipotentUser,
      actAsParties = actAs ++ randomActAs,
      readAsParties = readAs ++ randomReadAs,
    ),
  ) ++ super.prerequisiteUsers

  // Notes for multi-party submissions:
  // - ActAs parties from the request require ActAs claims in the token
  // - ReadAs parties from the request require ReadAs or ActAs claims in the token

  serviceCallName should {
    "allow single-party calls authorized to exactly the submitter (actAs)" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a single-party calls authorized to exactly the submitter (actAs)"
      ) in { implicit env =>
      import env.*
      val singlePartyId = getPartyId(singleParty)
      expectSuccess(
        serviceCallFor(
          standardToken(singlePartyActUser),
          RequestSubmitters(List(singlePartyId), List.empty),
        )
      )
    }

    "allow single-party calls authorized to exactly the submitter (actAs, and readAs)" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a single-party calls authorized to exactly the submitter (actAs, and readAs)"
      ) in { implicit env =>
      import env.*
      val singlePartyId = getPartyId(singleParty)
      expectSuccess(
        serviceCallFor(
          standardToken(singlePartyActUser),
          RequestSubmitters(List(singlePartyId), List(singlePartyId)),
        )
      )
    }

    "deny single-party calls authorized to no parties (actAs)" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a single-party call authorized to no parties (actAs)"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(
          serviceCallFor(
            standardToken(noPartyUser),
            RequestSubmitters(List(getPartyId(singleParty)), List.empty),
          )
        )
      }
    }

    "deny single-party calls authorized in read-only mode (actAs)" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a single-party call authorized in read-only mode (actAs)"
        )
      ) in { implicit env =>
      import env.*
      val singlePartyId = getPartyId(singleParty)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(singlePartyReadUser),
          RequestSubmitters(List(singlePartyId), List.empty),
        )
      )
    }

    "deny single-party calls authorized to a random party (actAs)" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a single-party call authorized to a random party (actAs)"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCallFor(
          standardToken(randomPartyActUser),
          RequestSubmitters(List(getPartyId(singleParty)), List.empty),
        )
      )
    }

    "allow multi-party calls authorized to exactly the required parties" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a multi-party calls authorized to exactly the required parties"
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectSuccess(
        serviceCallFor(
          standardToken(superUser),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "allow multi-party calls authorized to a superset of the required parties" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a multi-party calls authorized to a superset of the required parties"
      ) in { implicit env =>
      import env.*
      val actAsIds = (actAs ++ randomActAs).map(getPartyId)
      val readAsIds = (readAs ++ randomReadAs).map(getPartyId)
      expectSuccess(
        serviceCallFor(
          standardToken(omnipotentUser),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "allow multi-party calls with all parties authorized in read-write mode" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a multi-party calls with all parties authorized in read-write mode"
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectSuccess(
        serviceCallFor(
          standardToken(superActorUser),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "allow multi-party calls with actAs parties duplicated in the readAs field" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a multi-party call with actAs parties duplicated in the readAs field"
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectSuccess(
        serviceCallFor(
          standardToken(omnipotentUser),
          RequestSubmitters(actAsIds, actAsIds ++ readAsIds),
        )
      )
    }

    "deny multi-party calls authorized to no parties" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Execute a multi-party call authorized to no parties")
    ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(
          serviceCallFor(
            standardToken(noPartyUser),
            RequestSubmitters(actAsIds, readAsIds),
          )
        )
      }
    }

    "deny multi-party calls authorized to random parties" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Execute a multi-party call authorized to random parties")
    ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(randomUser),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "deny multi-party calls with all parties authorized in read-only mode" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a multi-party call with all parties authorized in read-only mode"
        )
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(superReaderUser),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "deny multi-party calls with one missing actor authorization" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a multi-party call with one missing actor authorization"
        )
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(superUserMinusActor),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "deny multi-party calls with one missing reader authorization" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a multi-party call with one missing reader authorization"
        )
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(superUserMinusReader),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }

    "deny multi-party calls authorized to swapped actAs/readAs parties" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Execute a multi-party call authorized to swapped actAs/readAs parties"
        )
      ) in { implicit env =>
      import env.*
      val actAsIds = actAs.map(getPartyId)
      val readAsIds = readAs.map(getPartyId)
      expectPermissionDenied(
        serviceCallFor(
          standardToken(superUserSwapped),
          RequestSubmitters(actAsIds, readAsIds),
        )
      )
    }
  }
}
