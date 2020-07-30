// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.{NoSuchElementException, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  StatusCodes,
  Uri
}
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

  // Send an HTTP request and convert an error response into a failed future.
  // Run the given function on a successful response only.
  private def runRequest[A](req: HttpRequest)(ifOk: HttpResponse => Future[A]): Future[A] =
    http.singleRequest(req) flatMap { resp =>
      resp.status match {
        case StatusCodes.OK => ifOk(resp)
        case errorCode =>
          val errorMessage =
            s"""${req.method.value} request to ${req.uri} failed with status code ${errorCode.value}.
          |Response body: ${resp.entity.dataBytes}""".stripMargin
          Future.failed(new RuntimeException(errorMessage))
      }
    }

  // Used to identify a failure condition in a retry strategy
  private val notFound: PartialFunction[Throwable, Boolean] = {
    case e if e.isInstanceOf[NoSuchElementException] => true
  }

  def authorize(username: String, password: String): Future[AuthServiceToken] = {
    val uri = authServiceBaseUri.withPath(saSecure./("authorize"))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(Authorization(BasicHttpCredentials(username, password)))
    )
    runRequest(req)(Unmarshal(_).to[AuthServiceToken])
  }

  def requestServiceAccount(authServiceToken: AuthServiceToken, ledgerId: String): Future[Unit] = {
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
    runRequest(req)(_ => Future(()))
  }

  def listServiceAccounts(authServiceToken: AuthServiceToken): Future[ServiceAccountList] = {
    val uri = authServiceBaseUri.withPath(saSecure)
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri,
      headers = List(authHeader),
    )
    runRequest(req)(Unmarshal(_).to[ServiceAccountList])
  }

  def getServiceAccount(authServiceToken: AuthServiceToken): Future[ServiceAccount] =
    RetryStrategy.constant(attempts = Some(3), waitTime = 4.seconds)(notFound) { (_, _) =>
      listServiceAccounts(authServiceToken).map(_.serviceAccounts.head)
    }

  def requestCredential(
      authServiceToken: AuthServiceToken,
      serviceAccountId: String): Future[Unit] = {
    val uri = authServiceBaseUri.withPath(saSecure./(serviceAccountId)./("credRequest"))
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
    )
    runRequest(req)(_ => Future(()))
  }

  def getNewCredentialId(authServiceToken: AuthServiceToken): Future[CredentialId] =
    for {
      sa <- getServiceAccount(authServiceToken)
      initialNumCreds = sa.creds.length
      () <- requestCredential(authServiceToken, sa.serviceAccount)
      newCred <- RetryStrategy.constant(attempts = Some(3), waitTime = 10.seconds)(notFound) {
        (_, _) =>
          for {
            sa <- getServiceAccount(authServiceToken)
            _ = if (sa.creds.length <= 0) throw new NoSuchElementException
          } yield sa.creds.head // new credential is added to the front of the list
      }
    } yield newCred

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
    runRequest(req)(Unmarshal(_).to[Credential])
  }

  def login(credential: Credential): Future[LedgerAccessToken] = {
    val uri = authServiceBaseUri.withPath(saLogin)
    val authHeader = Authorization(BasicHttpCredentials(credential.credId, credential.cred))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
    )
    runRequest(req)(Unmarshal(_).to[LedgerAccessToken])
  }

  def getLedgerToken(
      username: String,
      password: String,
      ledgerId: String): Future[LedgerAccessToken] =
    for {
      token <- authorize(username, password)
      () <- requestServiceAccount(token, ledgerId)
      sa <- getServiceAccount(token)
      () <- requestCredential(token, sa.serviceAccount)
      credId <- getNewCredentialId(token)
      cred <- getCredential(token, credId)
      accessTok <- login(cred)
    } yield accessTok
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
