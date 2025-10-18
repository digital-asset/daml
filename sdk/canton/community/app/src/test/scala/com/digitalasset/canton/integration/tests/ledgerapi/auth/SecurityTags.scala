// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTPayload
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{
  CantonFixture,
  CreatesParties,
  CreatesUsers,
}
import org.scalatest.BeforeAndAfterAll

import java.time.Duration
import java.util.UUID

trait SecurityTags extends CreatesUsers with CreatesParties with BeforeAndAfterAll {
  self: CantonFixture =>
  def serviceCallName: String

  val securityAsset: SecurityTest =
    SecurityTest(property = Authorization, asset = s"User Endpoint $serviceCallName")

  val adminSecurityAsset: SecurityTest =
    SecurityTest(property = Authorization, asset = s"Admin Endpoint $serviceCallName")

  def attackPermissionDenied(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with PERMISSION_DENIED to $serviceCallName",
  )

  def attackInvalidArgument(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with INVALID_ARGUMENT to $serviceCallName",
  )

  def attackUnauthenticated(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with UNAUTHENTICATED to $serviceCallName",
  )

  def attackUnknownResource(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with NOT_FOUND to $serviceCallName",
  )

  def streamAttack(threat: String): Attack = Attack(
    actor = s"Ledger API stream client calling $serviceCallName",
    threat = threat,
    mitigation = s"Break the stream of $serviceCallName",
  )

  protected val randomParty: String = "randomParty-" + UUID.randomUUID.toString
  protected val randomPartyReadUser: String = "readAs-" + randomParty
  protected val randomPartyActUser: String = "actAs-" + randomParty
  protected val readAsAnyPartyUser: String = "readAsAnyParty-" + UUID.randomUUID.toString
  protected val executeAsAnyPartyUser: String = "executeAsAnyParty-" + UUID.randomUUID.toString

  protected def prerequisiteParties: List[String] = List(randomParty)
  protected def prerequisiteUsers: List[PrerequisiteUser] = List(
    PrerequisiteUser(randomPartyReadUser, readAsParties = List(randomParty)),
    PrerequisiteUser(randomPartyActUser, actAsParties = List(randomParty)),
    PrerequisiteUser(readAsAnyPartyUser, readAsAnyParty = true),
    PrerequisiteUser(executeAsAnyPartyUser, executeAsAnyParty = true),
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(canBeAnAdmin.token, prerequisiteParties)(
      directExecutionContext
    )
    createPrerequisiteUsers(canBeAnAdmin.token, prerequisiteUsers)(
      directExecutionContext
    )
  }
  protected def unknownUserTokenStandardJWT: StandardJWTPayload = standardToken("unknown_user")
  protected def invalidUserTokenStandardJWT: StandardJWTPayload = standardToken("??invalid_user??")

  protected def noToken: ServiceCallContext = ServiceCallContext(None)
  protected def canActAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(standardToken(randomPartyActUser)))
  )
  protected def canActAsRandomPartyExpired: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(-1), standardToken(randomPartyActUser)))
    )
  )
  protected def canActAsRandomPartyExpiresInAnHour: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofHours(1), standardToken(randomPartyActUser)))
    )
  )

  protected def canReadAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(standardToken(randomPartyReadUser)))
  )
  protected def canReadAsRandomPartyExpired: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(-1), standardToken(randomPartyReadUser)))
    )
  )
  protected def canReadAsRandomPartyExpiresInAnHour: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofHours(1), standardToken(randomPartyReadUser)))
    )
  )

  protected def canReadAsAdminExpired: ServiceCallContext = ServiceCallContext(
    Option(toHeader(expiringIn(Duration.ofDays(-1), adminToken)))
  )
  protected def canReadAsAdminExpiresInAnHour: ServiceCallContext = ServiceCallContext(
    Option(toHeader(expiringIn(Duration.ofHours(1), adminToken)))
  )

  protected def canReadAsAnyParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(standardToken(readAsAnyPartyUser)))
  )
  protected def canReadAsAnyPartyExpired: ServiceCallContext = ServiceCallContext(
    Option(toHeader(expiringIn(Duration.ofDays(-1), standardToken(readAsAnyPartyUser))))
  )
  protected def canReadAsAnyPartyExpiresInAnHour =
    ServiceCallContext(
      Option(toHeader(expiringIn(Duration.ofHours(1), standardToken(readAsAnyPartyUser))))
    )

  // Standard tokens for user authentication
  protected def canBeAnAdmin: ServiceCallContext = ServiceCallContext(
    Option(toHeader(adminToken))
  )
  protected def canReadAsUnknownUserStandardJWT: ServiceCallContext = ServiceCallContext(
    Option(toHeader(unknownUserTokenStandardJWT))
  )
  protected def canReadAsInvalidUserStandardJWT: ServiceCallContext = ServiceCallContext(
    Option(toHeader(invalidUserTokenStandardJWT))
  )

  protected def canBeAUser(userId: String, issuer: Option[String]) = ServiceCallContext(
    Option(toHeader(standardToken(userId, issuer = issuer)))
  )

  protected def canExecuteAsAnyParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(standardToken(executeAsAnyPartyUser)))
  )
}
