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

import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._
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
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.testing.postgresql.PostgresAroundSuite
import eu.rekawek.toxiproxy._

class ServiceTest extends AsyncFlatSpec with Eventually with Matchers with PostgresAroundSuite {

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

  def withHttpService[A](
      triggerDar: Option[Dar[(PackageId, Package)]],
      jdbcConfig: Option[JdbcConfig] = None)
    : ((Uri, LedgerClient, Proxy) => Future[A]) => Future[A] =
    TriggerServiceFixture
      .withTriggerService[A](testId, List(darPath), triggerDar, jdbcConfig)

  def startTrigger(uri: Uri, id: String, party: String): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/v1/start")),
      headers = headersWithAuth(party),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"triggerName": "$id"}"""
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

  def triggerStatus(uri: Uri, id: String): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/status/$id")),
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

  def assertTriggerIds(
      uri: Uri,
      party: String,
      pred: (Vector[String]) => Boolean): Future[Assertion] = {
    eventually {
      val actualTriggerIds = Await.result(for {
        resp <- listTriggers(uri, party)
        result <- parseTriggerIds(resp)
      } yield result, Duration.Inf)
      assert(pred(actualTriggerIds))
    }
  }

  def parseTriggerStatus(resp: HttpResponse): Future[Vector[String]] = {
    for {
      JsObject(fields) <- parseResult(resp)
      Some(JsArray(list)) = fields.get("logs")
      statusMsgs = list map {
        case JsArray(Vector(JsString(_), JsString(msg))) => msg
        case _ => fail("""Unexpected format in the "logs" field""")
      }
    } yield statusMsgs
  }

  def assertTriggerStatus(
      uri: Uri,
      id: String,
      pred: (Vector[String]) => Boolean): Future[Assertion] = {
    eventually {
      val actualTriggerStatus = Await.result(for {
        resp <- triggerStatus(uri, id)
        result <- parseTriggerStatus(resp)
      } yield result, Duration.Inf)
      assert(pred(actualTriggerStatus))
    }
  }

  it should "initialize database" in {
    connectToPostgresqlServer()
    createNewDatabase()
    val testJdbcConfig = JdbcConfig(postgresDatabase.url, "operator", "password")
    assert(ServiceMain.initDatabase(testJdbcConfig).isRight)
    dropDatabase()
    disconnectFromPostgresqlServer()
    succeed
  }

  it should "add running triggers to the database" in {
    connectToPostgresqlServer()
    createNewDatabase()
    val testJdbcConfig = JdbcConfig(postgresDatabase.url, "operator", "password")
    assert(ServiceMain.initDatabase(testJdbcConfig).isRight)
    Await.result(
      withHttpService(Some(dar), Some(testJdbcConfig)) {
        (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
          for {
            // Initially no triggers started for Alice
            _ <- assertTriggerIds(uri, "Alice", _ == Vector())
            // Start a trigger for Alice and check it appears in list.
            resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
            trigger1 <- parseTriggerId(resp)
            _ <- assertTriggerIds(uri, "Alice", _ == Vector(trigger1))
            // Do the same for a second trigger.
            resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
            trigger2 <- parseTriggerId(resp)
            expected = Vector(trigger1, trigger2).sorted
            _ <- assertTriggerIds(uri, "Alice", _ == expected)
          } yield succeed
      },
      Duration.Inf
    )
    dropDatabase()
    disconnectFromPostgresqlServer()
    succeed
  }

  it should "fail to start non-existent trigger" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
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

  it should "start a trigger after uploading it" in withHttpService(None) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- uploadDar(uri, darPath)
        JsObject(fields) <- parseResult(resp)
        Some(JsString(mainPackageId)) = fields.get("mainPackageId")
        _ <- mainPackageId should not be empty
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        triggerId <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, "Alice", _ == Vector(triggerId))
        resp <- stopTrigger(uri, triggerId, "Alice")
        stoppedTriggerId <- parseTriggerId(resp)
        _ <- stoppedTriggerId should equal(triggerId)
      } yield succeed
  }

  it should "start multiple triggers and list them by party" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- listTriggers(uri, "Alice")
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
        // Start trigger for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, "Alice", _ == Vector(aliceTrigger))
        // Start trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Bob")
        bobTrigger1 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, "Bob", _ == Vector(bobTrigger1))
        // Start another trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Bob")
        bobTrigger2 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, "Bob", _ == Vector(bobTrigger1, bobTrigger2))
        // Stop Alice's trigger.
        resp <- stopTrigger(uri, aliceTrigger, "Alice")
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, "Alice", _.isEmpty)
        _ <- assertTriggerIds(uri, "Bob", _ == Vector(bobTrigger1, bobTrigger2))
        // Stop Bob's triggers.
        resp <- stopTrigger(uri, bobTrigger1, "Bob")
        _ <- assert(resp.status.isSuccess)
        resp <- stopTrigger(uri, bobTrigger2, "Bob")
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, "Bob", _.isEmpty)
      } yield succeed
  }

  it should "should enable a trigger on http request" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
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

  it should "fail to start a trigger if a ledger client can't be obtained" in withHttpService(
    Some(dar)) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    // Disable the proxy. This means that the service won't be able to
    // get a ledger connection.
    ledgerProxy.disable()
    try {
      // Request a trigger be started and setup an assertion that
      // completes with success when the running trigger table becomes
      // non-empty.
      val runningTriggersNotEmpty = for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        triggerId <- parseTriggerId(resp)
      } yield (assertTriggerIds(uri, "Alice", _.nonEmpty))
      // Wait a good while (10s) on the assertion to become true. If it
      // does, indicate the test has failed (because, since the trigger
      // can't be initialized for the lack of a viable ledger client
      // connection, it should not make its way to the running triggers
      // table).
      Await.ready(awaitable = runningTriggersNotEmpty, atMost = 10.seconds)
      fail("Timeout expected")
    } catch {
      // If the assertion times-out the test has succeeded (look to
      // the log; you'll see messages indicating that the trigger
      // failed to initialize and was stopped).
      case _: TimeoutException => succeed
    } finally {
      // This isn't strictly neccessary here (since each test gets its
      // own fixture) but it's jolly decent of us to do it anyway.
      ledgerProxy.enable()
    }
  }

  it should "stop a failing trigger that can't be restarted" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      // Simulate the ledger becoming unrecoverably unavailable due to
      // network connectivity loss. The stop strategy means the running
      // trigger will be terminated.
      for {
        // Request a trigger be started for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        // Proceed when it's confirmed to be running.
        _ <- assertTriggerIds(uri, "Alice", _ == Vector(aliceTrigger))
        // Simulate unrecoverable network connectivity loss.
        _ <- Future { ledgerProxy.disable() }
        // Confirm that the running trigger ends up stopped and that
        // its history matches our expectations.
        _ <- assertTriggerIds(uri, "Alice", _.isEmpty)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _ ==
            Vector(
              "starting",
              "running",
              "stopped: runtime failure",
              "starting",
              "stopped: initialization failure"))
      } yield succeed
  }

  it should "restart a failing trigger if possible" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      // Simulate the ledger briefly being unavailable due to network
      // connectivity loss. Our restart strategy means that the running
      // trigger gets restarted.
      for {
        // Request a trigger be started for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        // Proceed when it is confirmed to be running.
        _ <- assertTriggerIds(uri, "Alice", _ == Vector(aliceTrigger))
        // Simulate brief network connectivity loss. This will cause the
        // running trigger's flow graph to complete with failure. Don't
        // wait around to restore the network or the restart strategy
        // will in turn lead to the stop strategy killing the trigger
        // due to the lack of ability to initialize the restarted
        // trigger.
        _ <- Future { ledgerProxy.disable() }
        _ <- Future { ledgerProxy.enable() }
        // To conclude, check that the trigger survived the network
        // outage and that its history indicates it went through a
        // restart to do so.
        _ <- assertTriggerIds(uri, "Alice", _ == Vector(aliceTrigger))
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _ ==
            Vector(
              "starting",
              "running",
              "stopped: runtime failure",
              "starting",
              "running"
            ))
      } yield succeed
  }

  it should "stop a trigger when the user script fails init" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:ErrorTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _ ==
            Vector(
              "starting",
              "stopped: initialization failure",
            ))
      } yield succeed
  }

  it should "restart triggers with errors in user script" in withHttpService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:LowLevelErrorTrigger:trigger", "Alice")
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _ ==
            Vector(
              "starting",
              "running",
              "stopped: runtime failure",
              "starting",
              "running",
              "stopped: runtime failure",
              "starting",
              "running",
              "stopped: runtime failure",
              "starting",
              "running",
              "stopped: runtime failure"
            )
        )
        _ <- assertTriggerIds(uri, "Alice", _.isEmpty)
      } yield succeed
  }

}
