// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast.Package
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.util.ByteString
import akka.stream.scaladsl.{FileIO, Sink, Source}
import java.io.File
import java.util.UUID
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import scala.concurrent.{Await}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.syntax.show._
import spray.json._

import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter, InclusiveFilters}
import com.daml.ledger.client.LedgerClient
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}

class ServiceTest extends AsyncFlatSpec with Eventually with Matchers {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(1, Seconds)))

  val darPath = requiredResource("triggers/service/test-model.dar")
  val encodedDar =
    DarReader().readArchiveFromFile(darPath).get
  val dar = encodedDar.map {
    case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
  }
  val testPkgId = dar.main._1

  def submitCmd(client: LedgerClient, party: String, cmd: Command) = {
    val req = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = testId,
          ledgerId = client.ledgerId.unwrap,
          commandId = UUID.randomUUID.toString,
          commands = Seq(cmd)
        )))
    client.commandServiceClient.submitAndWait(req)
  }

  def testId: String = this.getClass.getSimpleName
  implicit val system: ActorSystem = ActorSystem(testId)
  implicit val esf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(system)
  implicit val ec: ExecutionContext = system.dispatcher

  protected def jwt(party: String): Jwt = {
    val decodedJwt = DecodedJwt(
      """{"alg": "HS256", "typ": "JWT"}""",
      s"""{"https://daml.com/ledger-api": {"ledgerId": "${testId: String}", "applicationId": "${testId: String}", "actAs": ["${party}"]}}"""
    )
    JwtSigner.HMAC256
      .sign(decodedJwt, "secret")
      .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  protected def headersWithAuth(party: String) = authorizationHeader(jwt(party))

  protected def authorizationHeader(token: Jwt): List[Authorization] =
    List(Authorization(OAuth2BearerToken(token.value)))

  def withHttpService[A](triggerDar: Option[Dar[(PackageId, Package)]])
    : ((Uri, LedgerClient) => Future[A]) => Future[A] =
    TriggerServiceFixture
      .withTriggerService[A](testId, List(darPath), triggerDar)

  def startTrigger(uri: Uri, id: String, party: String): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/v1/start")),
      headers = headersWithAuth(party),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"identifier": "$id"}"""
      )
    )
    Http().singleRequest(req)
  }

  def listTriggers(uri: Uri, party: String): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/list")),
      headers = headersWithAuth(party),
    )
    Http().singleRequest(req)
  }

  def stopTrigger(uri: Uri, id: String, party: String): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      headers = headersWithAuth(s"${party}"),
      uri = uri.withPath(Uri.Path(s"/v1/stop/$id")),
    )
    Http().singleRequest(req)
  }

  def uploadDar(uri: Uri, file: File): Future[HttpResponse] = {
    val fileContentsSource: Source[ByteString, Any] = FileIO.fromPath(file.toPath)
    val multipartForm = Multipart.FormData(
      Multipart.FormData.BodyPart(
        "dar",
        HttpEntity.IndefiniteLength(ContentTypes.`application/octet-stream`, fileContentsSource),
        Map("filename" -> file.toString)))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path(s"/v1/upload_dar")),
      entity = multipartForm.toEntity
    )
    Http().singleRequest(req)
  }

  def responseBodyToString(resp: HttpResponse): Future[String] = {
    resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  }

  // Check the response was successful and extract the "result" field.
  def parseResult(resp: HttpResponse): Future[JsValue] = {
    for {
      _ <- assert(resp.status.isSuccess)
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      Some(result) = fields.get("result")
    } yield result
  }

  def parseTriggerId(resp: HttpResponse): Future[String] = {
    for {
      JsObject(fields) <- parseResult(resp)
      Some(JsString(triggerId)) = fields.get("triggerId")
    } yield triggerId
  }

  def parseTriggerIds(resp: HttpResponse): Future[Vector[String]] = {
    for {
      JsObject(fields) <- parseResult(resp)
      Some(JsArray(ids)) = fields.get("triggerIds")
      triggerIds = ids map {
        case JsString(id) => id
        case _ => fail("""Non-string element of "triggerIds" field""")
      }
    } yield triggerIds
  }

  it should "fail to start non-existent trigger" in withHttpService(Some(dar)) {
    (uri: Uri, client) =>
      val expectedError = StatusCodes.UnprocessableEntity
      for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:foobar", "Alice")
        _ <- resp.status should equal(expectedError)
        // Check the "status" and "errors" fields
        body <- responseBodyToString(resp)
        JsObject(fields) = body.parseJson
        _ <- fields.get("status") should equal(Some(JsNumber(expectedError.intValue)))
        _ <- fields.get("errors") should equal(
          Some(JsArray(JsString("Could not find name foobar in module TestTrigger"))))
      } yield succeed
  }

  it should "start a trigger after uploading it" in withHttpService(None) { (uri: Uri, client) =>
    for {
      resp <- uploadDar(uri, darPath)
      JsObject(fields) <- parseResult(resp)
      Some(JsString(mainPackageId)) = fields.get("mainPackageId")
      _ <- mainPackageId should not be empty

      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
      triggerId <- parseTriggerId(resp)

      resp <- listTriggers(uri, "Alice")
      result <- parseTriggerIds(resp)
      _ <- result should equal(Vector(triggerId))

      resp <- stopTrigger(uri, triggerId, "Alice")
      stoppedTriggerId <- parseTriggerId(resp)
      _ <- stoppedTriggerId should equal(triggerId)
    } yield succeed
  }

  it should "start multiple triggers and list them by party" in withHttpService(Some(dar)) {
    (uri: Uri, client) =>
      for {
        // no triggers running initially
        resp <- listTriggers(uri, "Alice")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
        // start trigger for Alice
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        resp <- listTriggers(uri, "Alice")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector(aliceTrigger))
        // start trigger for Bob
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Bob")
        bobTrigger1 <- parseTriggerId(resp)
        resp <- listTriggers(uri, "Bob")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector(bobTrigger1))
        // start another trigger for Bob
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Bob")
        bobTrigger2 <- parseTriggerId(resp)
        resp <- listTriggers(uri, "Bob")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector(bobTrigger1, bobTrigger2))
        // stop Alice's trigger
        resp <- stopTrigger(uri, aliceTrigger, "Alice")
        _ <- assert(resp.status.isSuccess)
        resp <- listTriggers(uri, "Alice")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
        resp <- listTriggers(uri, "Bob")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector(bobTrigger1, bobTrigger2))
        // stop Bob's triggers
        resp <- stopTrigger(uri, bobTrigger1, "Bob")
        _ <- assert(resp.status.isSuccess)
        resp <- stopTrigger(uri, bobTrigger2, "Bob")
        _ <- assert(resp.status.isSuccess)
        resp <- listTriggers(uri, "Bob")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
      } yield succeed
  }

  it should "should enable a trigger on http request" in withHttpService(Some(dar)) {
    (uri: Uri, client) =>
      for {
        // Start the trigger
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        triggerId <- parseTriggerId(resp)

        // Trigger is running, create an A contract
        _ <- {
          val cmd = Command().withCreate(
            CreateCommand(
              templateId = Some(Identifier(testPkgId, "TestTrigger", "A")),
              createArguments = Some(
                Record(
                  None,
                  Seq(
                    RecordField(value = Some(Value().withParty("Alice"))),
                    RecordField(value = Some(Value().withInt64(42)))))),
            ))
          submitCmd(client, "Alice", cmd)
        }
        // Query ACS until we see a B contract
        // format: off
        _ <- Future {
          val filter = TransactionFilter(List(("Alice", Filters(Some(InclusiveFilters(Seq(Identifier(testPkgId, "TestTrigger", "B"))))))).toMap)
          eventually {
            val acs = client.activeContractSetClient.getActiveContracts(filter).runWith(Sink.seq)
              .map(acsPages => acsPages.flatMap(_.activeContracts))
            // Once we switch to scalatest 3.1, we should no longer need the Await.result here since eventually
            // handles Future results.
            val r = Await.result(acs, Duration.Inf)
            assert(r.length == 1)
          }
        }
        // format: on
        resp <- stopTrigger(uri, triggerId, "Alice")
        _ <- assert(resp.status.isSuccess)
      } yield succeed
  }
}
