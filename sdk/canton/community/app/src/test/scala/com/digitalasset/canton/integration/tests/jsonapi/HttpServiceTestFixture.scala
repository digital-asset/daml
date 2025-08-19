// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.ledger.api.v2.value as v
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{
  PemFile,
  TlsClientCertificate,
  TlsClientConfig,
  TlsServerConfig,
}
import com.digitalasset.canton.http.json.v1.{LedgerReader, PackageService, V1Routes}
import com.digitalasset.canton.http.json.{ApiJsonDecoder, ApiJsonEncoder}
import com.digitalasset.canton.http.util.ClientUtil.boxedRecord
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.http.util.{FutureUtil, NewBoolean}
import com.digitalasset.canton.http.{ContractTypeId, CreateCommand, ExerciseCommand, Party, UserId}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString
import scalaz.syntax.tag.*
import spray.json.*

import java.time.Instant
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
object HttpServiceTestFixture {

  val loggerFactory = SuppressingLogger(getClass)
  val logger = loggerFactory.getLogger(getClass)

  lazy val staticPkgIdAccount: Ref.PackageRef = {
    import com.digitalasset.daml.lf.{archive, typesig}
    val darFile = JarResourceUtils.resourceFile("Account-3.4.0.dar")
    val dar = archive.UniversalArchiveReader.assertReadFile(darFile)
    Ref.PackageRef.assertFromString(typesig.PackageSignature.read(dar.main)._2.packageId)
  }

  def jsonCodecs(
      client: DamlLedgerClient,
      token: Option[Jwt],
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[(ApiJsonEncoder, ApiJsonDecoder)] = {
    val loadCache = LedgerReader.LoadCache.freshCache()
    val packageService =
      new PackageService(
        reloadPackageStoreIfChanged =
          V1Routes.doLoad(client.packageService, LedgerReader(loggerFactory), loadCache),
        loggerFactory = SuppressingLogger(getClass),
      )
    packageService
      .reload(
        token.getOrElse(Jwt("we use a dummy because there is no token in these tests."))
      )
      .flatMap(x => FutureUtil.toFuture(x))
      .map(_ => V1Routes.buildJsonCodecs(packageService))
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
    val payload: JsValue = {
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
      standardJwtPayload.toJson
    }
    JwtSigner.HMAC256
      .sign(
        DecodedJwt(
          """{"alg": "HS256", "typ": "JWT"}""",
          payload.prettyPrint,
        ),
        "secret",
      )
      .fold(
        e => throw new IllegalArgumentException(s"cannot sign a JWT: ${e.prettyPrint}"),
        identity,
      )
  }

  def headersWithUserAuth(
      user: Option[String]
  ): List[HttpHeader] = authorizationHeader(jwtForUser(user))

  def authorizationHeader(token: Jwt): List[HttpHeader] =
    List(Authorization(OAuth2BearerToken(token.value)))

  def archiveCommand[Ref](reference: Ref): ExerciseCommand[Nothing, v.Value, Ref] = {
    val arg: v.Record = v.Record()
    val choice = lar.Choice("Archive")
    ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
  }

  def accountCreateCommand(
      owner: Party,
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.roundInstantToMicros(Instant.now),
  ): CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg] = {
    val templateId = ContractTypeId.Template(staticPkgIdAccount, "Account", "Account")
    val timeValue = v.Value(time)
    val enabledVariantValue =
      v.Value(v.Value.Sum.Variant(v.Variant(None, "Enabled", Some(timeValue))))
    val arg = v.Record(
      fields = List(
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party(owner.unwrap)))),
        v.RecordField("number", Some(v.Value(v.Value.Sum.Text(number)))),
        v.RecordField("status", Some(enabledVariantValue)),
      )
    )

    CreateCommand(templateId, arg, None)
  }

  def sharedAccountCreateCommand(
      owners: Seq[Party],
      number: String,
      time: v.Value.Sum.Timestamp = TimestampConversion.roundInstantToMicros(Instant.now),
  ): CreateCommand[v.Record, ContractTypeId.Template.RequiredPkg] = {
    val templateId = ContractTypeId.Template(staticPkgIdAccount, "Account", "SharedAccount")
    val timeValue = v.Value(time)
    val ownersEnc = v.Value(
      v.Value.Sum.List(v.List(Party.unsubst(owners).map(o => v.Value(v.Value.Sum.Party(o)))))
    )
    val enabledVariantValue =
      v.Value(v.Value.Sum.Variant(v.Variant(None, "Enabled", Some(timeValue))))
    val arg = v.Record(
      fields = List(
        v.RecordField("owners", Some(ownersEnc)),
        v.RecordField("number", Some(v.Value(v.Value.Sum.Text(number)))),
        v.RecordField("status", Some(enabledVariantValue)),
      )
    )

    CreateCommand(templateId, arg, None)
  }

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
