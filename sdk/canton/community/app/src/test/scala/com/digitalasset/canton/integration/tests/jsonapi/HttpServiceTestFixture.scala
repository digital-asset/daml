// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.AuthServiceJWTCodec.JsonImplicits.*
import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  DecodedJwt,
  Jwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{
  PemFile,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.http.UserId
import com.digitalasset.canton.http.util.NewBoolean
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.Ref
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import scalaz.syntax.tag.*

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
object HttpServiceTestFixture {

  val loggerFactory: NamedLoggerFactory = SuppressingLogger(getClass)
  val logger = loggerFactory.getLogger(getClass)

  lazy val staticPkgIdAccount: Ref.PackageRef = {
    import com.digitalasset.daml.lf.{archive, typesig}
    val darFile = JarResourceUtils.resourceFile("Account-3.4.0.dar")
    val dar = archive.UniversalArchiveReader.assertReadFile(darFile)
    Ref.PackageRef.assertFromString(typesig.PackageSignature.read(dar.main)._2.packageId)
  }

  object UseTls extends NewBoolean.Named {
    val Tls: UseTls = True
    val NoTls: UseTls = False
  }
  type UseTls = UseTls.T

  private val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) =
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      PemFile(
        ExistingFile.tryCreate(
          "base/daml-tls/src/test/resources/test-certificates/" + src
        )
      )
    }

  final val serverTlsConfig = TlsServerConfig(serverCrt, serverPem, Some(caCrt))
  final val clientTlsConfig =
    TlsClientConfig(
      trustCollectionFile = Some(caCrt),
      clientCert = Some(
        TlsClientCertificate(certChainFile = clientCrt, privateKeyFile = clientPem)
      ),
    )

  val userId: UserId = UserId("test")

  def jwtForUser(
      user: Option[String]
  ): Jwt = {
    val standardJwtPayload: AuthServiceJWTPayload =
      StandardJWTPayload(
        issuer = None,
        userId = user.getOrElse(userId.unwrap),
        participantId = None,
        exp = None,
        format = StandardJWTTokenFormat.Scope,
        audiences = List.empty,
        scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
      )

    val payloadString: String = standardJwtPayload.asJson.noSpaces

    JwtSigner.HMAC256
      .sign(
        DecodedJwt(
          """{"alg": "HS256", "typ": "JWT"}""",
          payloadString,
        ),
        "secret",
      )
      .fold(
        e => throw new IllegalArgumentException(s"cannot sign a JWT: ${e.toString}"),
        identity,
      )
  }

  def headersWithUserAuth(
      user: Option[String]
  ): List[HttpHeader] = authorizationHeader(jwtForUser(user))

  def authorizationHeader(token: Jwt): List[HttpHeader] =
    List(Authorization(OAuth2BearerToken(token.value)))

  def getResponseDataString(resp: HttpResponse, debug: Boolean = false)(implicit
      mat: Materializer,
      ec: ExecutionContext,
  ): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  def getResponseDataBytes(resp: HttpResponse)(implicit
      mat: Materializer
  ): Future[ByteString] =
    resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a)
}
