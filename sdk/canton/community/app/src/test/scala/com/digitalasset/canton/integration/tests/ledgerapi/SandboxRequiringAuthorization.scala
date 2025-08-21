// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.AuthCallCredentials
import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  DecodedJwt,
  JwtSigner,
  KeyUtils,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin.user_management_service.User
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{
  CantonFixture,
  CantonFixtureAbstract,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import io.grpc.stub.AbstractStub

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait SandboxRequiringAuthorizationFuns {
  self: CantonFixtureAbstract =>

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  private def jwtHeaderRSA(keyId: String) =
    s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""

  protected val jwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)

  protected val participantAdmin = UserManagementStore.DefaultParticipantAdminUserId

  val defaultScope: String = AuthServiceJWTCodec.scopeLedgerApiFull

  protected def emptyToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    userId = "",
    participantId = None,
    exp = None,
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )

  protected def standardToken(
      userId: String,
      expiresIn: Option[Duration] = None,
      participantId: Option[String] = None,
      issuer: Option[String] = None,
  ): StandardJWTPayload =
    StandardJWTPayload(
      issuer = issuer,
      participantId = participantId,
      userId = userId,
      exp = expiresIn.map(delta => Instant.now().plusNanos(delta.toNanos)),
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = Some(defaultScope),
    )

  protected def randomUserId(): String = UUID.randomUUID().toString

  protected def adminToken: StandardJWTPayload = standardToken(
    participantAdmin
  )

  protected def expiringIn(t: Duration, p: StandardJWTPayload): StandardJWTPayload =
    p.copy(exp = Option(Instant.now().plusNanos(t.toNanos)))

  protected def forIssuer(id: String, p: StandardJWTPayload): StandardJWTPayload =
    p.copy(issuer = Some(id))

  protected def forParticipantId(id: String, p: StandardJWTPayload): StandardJWTPayload =
    p.copy(participantId = Some(id))

  protected def forUserId(id: String, p: StandardJWTPayload): StandardJWTPayload =
    p.copy(userId = id)

  protected def toHeader(
      payload: AuthServiceJWTPayload,
      secret: String = jwtSecret.unwrap,
      enforceFormat: Option[StandardJWTTokenFormat] = None,
  ): String =
    signed(payload, secret, enforceFormat)

  protected def toHeaderRSA(
      keyId: String,
      payload: AuthServiceJWTPayload,
      privateKey: RSAPrivateKey,
      enforceFormat: Option[StandardJWTTokenFormat] = None,
  ): String =
    signedRSA(keyId, payload, privateKey, enforceFormat)

  private def signed(
      payload: AuthServiceJWTPayload,
      secret: String,
      enforceFormat: Option[StandardJWTTokenFormat],
  ): String =
    JwtSigner.HMAC256
      .sign(
        DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload, enforceFormat)),
        secret,
      )
      .getOrElse(sys.error("Failed to generate token"))
      .value

  private def signedRSA(
      keyId: String,
      payload: AuthServiceJWTPayload,
      privateKey: RSAPrivateKey,
      enforceFormat: Option[StandardJWTTokenFormat],
  ): String =
    JwtSigner.RSA256
      .sign(
        DecodedJwt(
          jwtHeaderRSA(keyId),
          AuthServiceJWTCodec.compactPrint(payload, enforceFormat),
        ),
        privateKey,
      )
      .getOrElse(sys.error("Failed to generate token"))
      .value

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(AuthCallCredentials.authorizingStub(stub, _))

  protected def createUserByAdmin(
      userId: String,
      identityProviderId: String = "",
      rights: Vector[proto.Right] = Vector.empty,
      tokenModifier: StandardJWTPayload => StandardJWTPayload = identity,
      tokenIssuer: Option[String] = None,
      secret: Option[String] = None,
      primaryParty: String = "",
  )(implicit ec: ExecutionContext): Future[(proto.User, ServiceCallContext)] = {
    val userToken = Option(
      toHeader(
        tokenModifier(standardToken(userId, issuer = tokenIssuer)),
        secret = secret.getOrElse(jwtSecret.unwrap),
      )
    )
    createUser(userId, userToken, identityProviderId, rights, primaryParty)
  }

  protected def createUserByAdminRSA(
      userId: String,
      identityProviderId: String = "",
      rights: Vector[proto.Right] = Vector.empty,
      tokenIssuer: Option[String] = None,
      privateKey: RSAPrivateKey,
      primaryParty: String = "",
      keyId: String = "",
  )(implicit ec: ExecutionContext): Future[(User, ServiceCallContext)] = {
    val userToken = Option(
      toHeaderRSA(
        keyId = if (keyId.nonEmpty) keyId else userId,
        payload = standardToken(userId, issuer = tokenIssuer),
        privateKey = privateKey,
      )
    )
    createUser(userId, userToken, identityProviderId, rights, primaryParty)
  }

  def createUser(
      userId: String,
      userToken: Option[String],
      identityProviderId: String,
      rights: Vector[proto.Right],
      primaryParty: String,
  )(implicit ec: ExecutionContext): Future[(User, ServiceCallContext)] = {
    val user = proto.User(
      id = userId,
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = identityProviderId,
      primaryParty = primaryParty,
      isDeactivated = false,
    )
    val req = proto.CreateUserRequest(Some(user), rights)
    for {
      res <- stub(proto.UserManagementServiceGrpc.stub(channel), asAdmin.token)
        .createUser(req)
      createdUser = res.user.getOrElse(
        throw new RuntimeException("User was not found in CreateUserResponse")
      )
    } yield (createdUser, ServiceCallContext(userToken, None, identityProviderId))
  }

  protected def asAdmin: ServiceCallContext = ServiceCallContext(
    Option(toHeader(adminToken))
  )

}

trait SandboxRequiringAuthorization extends SandboxRequiringAuthorizationFuns {
  self: CantonFixture =>

}

trait KeyPairs {

  case class KeyPair(id: String, publicKey: RSAPublicKey, privateKey: RSAPrivateKey)

  protected val privateKeyParticipantAdmin = newRsaKeyPair().privateKey

  protected val key1 = newRsaKeyPair()
  protected val key2 = newRsaKeyPair()
  protected val key3 = newRsaKeyPair()

  val jwks: String = KeyUtils.generateJwks(
    Map(
      key1.id -> key1.publicKey,
      key2.id -> key2.publicKey,
    )
  )
  val altJwks: String = KeyUtils.generateJwks(
    Map(
      key3.id -> key3.publicKey
    )
  )

  protected def newKeyPair(algorithm: String, keySize: Int): java.security.KeyPair = {
    val generator = KeyPairGenerator.getInstance(algorithm)
    generator.initialize(keySize)
    generator.generateKeyPair()
  }

  protected def newRsaKeyPair(): KeyPair = {
    val keyPair = newKeyPair("RSA", 2048)
    val publicKey = keyPair.getPublic.asInstanceOf[RSAPublicKey]
    val privateKey = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
    val id = UUID.randomUUID().toString
    KeyPair(id, publicKey, privateKey)
  }
}
