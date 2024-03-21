// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID
import scala.concurrent.Future

/** Trait for services that use multiple actAs and readAs parties.
  * These tests only test for variations in the authorized parties.
  * They do not test for variations in expiration time, ledger ID, or participant ID.
  * It is expected that [[ReadWriteServiceCallAuthTests]] are run on the same service.
  */
trait MultiPartyServiceCallAuthTests extends SecuredServiceCallAuthTests {

  // Parties specified in the API request
  sealed case class RequestSubmitters(party: String, actAs: Seq[String], readAs: Seq[String])

  // Parties specified in the access token
  sealed case class TokenParties(actAs: List[String], readAs: List[String])

  private[this] val actorsCount: Int = 3
  private[this] val readersCount: Int = 3

  private[this] val actAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  private[this] val readAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)
  private[this] val submitters: RequestSubmitters = RequestSubmitters("", actAs, readAs)

  private[this] val singleParty = UUID.randomUUID.toString

  private[this] val randomActAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  private[this] val randomReadAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    serviceCallWithToken(token, submitters)

  def serviceCallWithToken(token: Option[String], requestSubmitters: RequestSubmitters): Future[Any]

  private[this] def serviceCallFor(
      tokenParties: TokenParties,
      requestSubmitters: RequestSubmitters,
  ): Future[Any] = {
    val token = Option(
      toHeader(multiPartyToken(tokenParties.actAs, tokenParties.readAs))
    )
    serviceCallWithToken(token, requestSubmitters)
  }

  // Notes for multi-party submissions:
  // - ActAs parties can be specified through a "party" field, and/or an "actAs" field.
  //   The contents of those fields are merged.
  // - ActAs parties from the request require ActAs claims in the token
  // - ReadAs parties from the request require ReadAs or ActAs claims in the token

  it should "allow single-party calls authorized to exactly the submitter (party)" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a single-party calls authorized to exactly the submitter (party)"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(List(singleParty), List.empty),
        RequestSubmitters(singleParty, List.empty, List.empty),
      )
    )
  }

  it should "allow single-party calls authorized to exactly the submitter (actAs)" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a single-party calls authorized to exactly the submitter (actAs)"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(List(singleParty), List.empty),
        RequestSubmitters("", List(singleParty), List.empty),
      )
    )
  }

  it should "allow single-party calls authorized to exactly the submitter (party, actAs, and readAs)" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a single-party calls authorized to exactly the submitter (party, actAs, and readAs)"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(List(singleParty), List.empty),
        RequestSubmitters(singleParty, List(singleParty), List(singleParty)),
      )
    )
  }

  it should "allow single-party calls authorized to a superset of the required parties (party)" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a single-party calls authorized to a superset of the required parties (party)"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(randomActAs :+ singleParty, randomReadAs),
        RequestSubmitters(singleParty, List.empty, List.empty),
      )
    )
  }

  it should "deny single-party calls authorized to no parties (party)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized to no parties (party)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, List.empty),
        RequestSubmitters(singleParty, List.empty, List.empty),
      )
    )
  }

  it should "deny single-party calls authorized to no parties (actAs)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized to no parties (actAs)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, List.empty),
        RequestSubmitters("", List(singleParty), List.empty),
      )
    )
  }

  it should "deny single-party calls authorized in read-only mode (party)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized in read-only mode (party)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, List(singleParty)),
        RequestSubmitters(singleParty, List.empty, List.empty),
      )
    )
  }

  it should "deny single-party calls authorized in read-only mode (actAs)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized in read-only mode (actAs)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, List(singleParty)),
        RequestSubmitters("", List(singleParty), List.empty),
      )
    )
  }

  it should "deny single-party calls authorized to a random party (party)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized to a random party (party)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List(randomParty), List.empty),
        RequestSubmitters(singleParty, List.empty, List.empty),
      )
    )
  }

  it should "deny single-party calls authorized to a random party (actAs)" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a single-party call authorized to a random party (actAs)"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List(randomParty), List.empty),
        RequestSubmitters("", List(singleParty), List.empty),
      )
    )
  }

  it should "allow multi-party calls authorized to exactly the required parties" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a multi-party calls authorized to exactly the required parties"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(actAs, readAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "allow multi-party calls authorized to a superset of the required parties" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a multi-party calls authorized to a superset of the required parties"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(randomActAs ++ actAs, randomReadAs ++ readAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "allow multi-party calls with all parties authorized in read-write mode" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a multi-party calls with all parties authorized in read-write mode"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(actAs ++ readAs, List.empty),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "allow multi-party calls with actAs parties spread across party and actAs fields" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a multi-party calls with actAs parties spread across party and actAs fields"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(actAs, readAs),
        RequestSubmitters(actAs.head, actAs.tail, readAs),
      )
    )
  }

  it should "allow multi-party calls with actAs parties duplicated in the readAs field" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a multi-party call with actAs parties duplicated in the readAs field"
    ) in {
    expectSuccess(
      serviceCallFor(
        TokenParties(actAs, readAs),
        RequestSubmitters("", actAs, actAs ++ readAs),
      )
    )
  }

  it should "deny multi-party calls authorized to no parties" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Execute a multi-party call authorized to no parties")
  ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, List.empty),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "deny multi-party calls authorized to random parties" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Execute a multi-party call authorized to random parties")
  ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(randomActAs, randomReadAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "deny multi-party calls with all parties authorized in read-only mode" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a multi-party call with all parties authorized in read-only mode"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(List.empty, actAs ++ readAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "deny multi-party calls with one missing actor authorization" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a multi-party call with one missing actor authorization"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(actAs.dropRight(1), readAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "deny multi-party calls with one missing reader authorization" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a multi-party call with one missing reader authorization"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(actAs, readAs.dropRight(1)),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }

  it should "deny multi-party calls authorized to swapped actAs/readAs parties" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Execute a multi-party call authorized to swapped actAs/readAs parties"
      )
    ) in {
    expectPermissionDenied(
      serviceCallFor(
        TokenParties(readAs, actAs),
        RequestSubmitters("", actAs, readAs),
      )
    )
  }
}
