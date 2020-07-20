// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

object AuthServiceDomain extends DefaultJsonProtocol {
  // OAuth2 bearer token for requests to authentication service
  case class AuthServiceToken(token: String)
  implicit val authServiceTokenFormat = jsonFormat1(AuthServiceToken)

  case class Nonce(nonce: String)
  implicit val nonceFormat = jsonFormat1(Nonce)

  case class CredentialId(credId: String)
  implicit val credentialIdFormat = jsonFormat1(CredentialId)

  case class ServiceAccount(creds: List[CredentialId], nonce: String, serviceAccount: String)
  implicit val serviceAccountFormat = jsonFormat3(ServiceAccount)

  case class ServiceAccountList(serviceAccounts: List[ServiceAccount])
  implicit val serviceAccountListFormat = jsonFormat1(ServiceAccountList)

  case class Credential(
      cred: String,
      credId: String,
      nonce: String,
      validFrom: String,
      validTo: String
  )
  implicit val credentialFormat = jsonFormat5(Credential)

  case class LedgerAccessToken(token: String)
  implicit val ledgerAccessTokenFormat = jsonFormat1(LedgerAccessToken)
}

class AuthServiceClient(authServiceBaseUri: Uri)(
    implicit system: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext) {
  import AuthServiceDomain._

  private val http: HttpExt = Http(system)
  private val saSecure = Path("/sa/secure")
  private val saLogin = Path("/sa/login")

  def authorize(username: String, password: String): Future[AuthServiceToken] = {
    val authorizeUri = authServiceBaseUri.withPath(saSecure./("authorize"))
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = authorizeUri,
      headers = List(Authorization(BasicHttpCredentials(username, password)))
    )
    for {
      authResponse <- http.singleRequest(request)
      authServiceToken <- Unmarshal(authResponse).to[AuthServiceToken]
    } yield authServiceToken
  }

  def requestServiceAccount(
      authServiceToken: AuthServiceToken,
      ledgerId: String): Future[Boolean] = {
    val uri = authServiceBaseUri.withPath(saSecure./("request")./(ledgerId))
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val nonce = Nonce(UUID.randomUUID.toString)
    val entity = HttpEntity(ContentTypes.`application/json`, nonce.toJson.toString)
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
      entity
    )
    http.singleRequest(req).map(_.status.isSuccess)
  }

  def listServiceAccounts(authServiceToken: AuthServiceToken): Future[ServiceAccountList] = {
    val uri = authServiceBaseUri.withPath(saSecure)
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri,
      headers = List(authHeader),
    )
    http.singleRequest(req).flatMap(Unmarshal(_).to[ServiceAccountList])
  }

  def getServiceAccount(authServiceToken: AuthServiceToken): Future[Option[ServiceAccount]] =
    try {
      RetryStrategy.constant(attempts = 3, waitTime = 4.seconds) { (_, _) =>
        for {
          ServiceAccountList(sa :: _) <- listServiceAccounts(authServiceToken)
        } yield Some(sa)
      }
    } catch {
      case e: Throwable => Future(None)
    }

  def requestCredential(
      authServiceToken: AuthServiceToken,
      serviceAccountId: String): Future[Boolean] = {
    val uri = authServiceBaseUri.withPath(saSecure./(serviceAccountId)./("credRequest"))
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
    )
    http.singleRequest(req).map(_.status.isSuccess)
  }

  def getNewCredentialId(
      authServiceToken: AuthServiceToken,
      serviceAccountId: String): Future[Option[CredentialId]] =
    getServiceAccount(authServiceToken) flatMap {
      case None => Future(None)
      case Some(sa) =>
        val numCreds = sa.creds.length
        requestCredential(authServiceToken, sa.serviceAccount) flatMap { reqSuccess =>
          if (!reqSuccess) Future(None)
          else
            try {
              RetryStrategy.constant(attempts = 3, waitTime = 4.seconds) { (_, _) =>
                for {
                  ServiceAccountList(sa :: _) <- listServiceAccounts(authServiceToken)
                  if sa.creds.length > numCreds
                } yield Some(sa.creds.head)
              }
            } catch {
              case e: Throwable => Future(None)
            }
        }
    }

  def getCredential(
      authServiceToken: AuthServiceToken,
      credentialId: CredentialId): Future[Credential] = {
    val uri = authServiceBaseUri.withPath(saSecure./("cred")./(credentialId.credId))
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri,
      headers = List(authHeader),
    )
    http.singleRequest(req).flatMap(Unmarshal(_).to[Credential])
  }

  def login(credential: Credential): Future[LedgerAccessToken] = {
    val uri = authServiceBaseUri.withPath(saLogin)
    val authHeader = Authorization(BasicHttpCredentials(credential.credId, credential.cred))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
    )
    http.singleRequest(req).flatMap(Unmarshal(_).to[LedgerAccessToken])
  }
}

object AuthServiceClient {
  def apply(authServiceBaseUri: Uri)(
      implicit system: ActorSystem,
      materializer: Materializer,
      ec: ExecutionContext,
  ): AuthServiceClient = {
    new AuthServiceClient(authServiceBaseUri)
  }
}
