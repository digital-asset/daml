// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast.Package
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
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
import com.daml.testing.postgresql.PostgresAroundAll
import eu.rekawek.toxiproxy._

abstract class AbstractTriggerServiceTest extends AsyncFlatSpec with Eventually with Matchers {

  // Abstract member for testing with and without a database
  def jdbcConfig: Option[JdbcConfig]

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(1, Seconds)))

  private val darPath = requiredResource("triggers/service/test-model.dar")
  private val encodedDar =
    DarReader().readArchiveFromFile(darPath).get
  private val dar = encodedDar.map {
    case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
  }
  private val testPkgId = dar.main._1

  private def submitCmd(client: LedgerClient, party: String, cmd: Command) = {
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

  private case class User(userName: String, password: String)
  private val alice = User("Alice", "&alC2l3SDS*V")
  private val bob = User("Bob", "7GR8G@InIO&v")

  protected def headersWithAuth(user: User): List[Authorization] = {
    user match {
      case User(party, password) => List(Authorization(BasicHttpCredentials(party, password)))
    }
  }

  def withTriggerService[A](dar: Option[Dar[(PackageId, Package)]])
    : ((Uri, LedgerClient, Proxy) => Future[A]) => Future[A] =
    TriggerServiceFixture.withTriggerService(testId, List(darPath), dar, jdbcConfig)

  def startTrigger(uri: Uri, triggerName: String, party: User): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/v1/start")),
      headers = headersWithAuth(party),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"triggerName": "$triggerName"}"""
      )
    )
    Http().singleRequest(req)
  }

  def listTriggers(uri: Uri, party: User): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/list")),
      headers = headersWithAuth(party),
    )
    Http().singleRequest(req)
  }

  def triggerStatus(uri: Uri, triggerInstance: UUID): Future[HttpResponse] = {
    val id = triggerInstance.toString
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/status/$id")),
    )
    Http().singleRequest(req)
  }

  def stopTrigger(uri: Uri, triggerInstance: UUID, party: User): Future[HttpResponse] = {
    val id = triggerInstance.toString
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      headers = headersWithAuth(party),
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

  def parseTriggerId(resp: HttpResponse): Future[UUID] = {
    for {
      JsObject(fields) <- parseResult(resp)
      Some(JsString(triggerId)) = fields.get("triggerId")
    } yield UUID.fromString(triggerId)
  }

  def parseTriggerIds(resp: HttpResponse): Future[Vector[UUID]] = {
    for {
      JsObject(fields) <- parseResult(resp)
      Some(JsArray(ids)) = fields.get("triggerIds")
      triggerIds = ids map {
        case JsString(id) => UUID.fromString(id)
        case _ => fail("""Non-string element of "triggerIds" field""")
      }
    } yield triggerIds
  }

  def assertTriggerIds(uri: Uri, party: User, pred: Vector[UUID] => Boolean): Future[Assertion] = {
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
      triggerInstance: UUID,
      pred: (Vector[String]) => Boolean): Future[Assertion] = {
    eventually {
      val actualTriggerStatus = Await.result(for {
        resp <- triggerStatus(uri, triggerInstance)
        result <- parseTriggerStatus(resp)
      } yield result, Duration.Inf)
      assert(pred(actualTriggerStatus))
    }
  }

  it should "start up and shut down server" in
    withTriggerService(Some(dar)) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      Future(succeed)
    }

  it should "fail to start non-existent trigger" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      val expectedError = StatusCodes.UnprocessableEntity
      for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:foobar", alice)
        _ <- resp.status should equal(expectedError)
        // Check the "status" and "errors" fields
        body <- responseBodyToString(resp)
        JsObject(fields) = body.parseJson
        _ <- fields.get("status") should equal(Some(JsNumber(expectedError.intValue)))
        _ <- fields.get("errors") should equal(
          Some(JsArray(JsString("Could not find name foobar in module TestTrigger"))))
      } yield succeed
  }

  it should "start a trigger after uploading it" in withTriggerService(None) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- uploadDar(uri, darPath)
        JsObject(fields) <- parseResult(resp)
        Some(JsString(mainPackageId)) = fields.get("mainPackageId")
        _ <- mainPackageId should not be empty
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        triggerId <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, _ == Vector(triggerId))
        resp <- stopTrigger(uri, triggerId, alice)
        stoppedTriggerId <- parseTriggerId(resp)
        _ <- stoppedTriggerId should equal(triggerId)
      } yield succeed
  }

  it should "start multiple triggers and list them by party" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- listTriggers(uri, alice)
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
        // Start trigger for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, _ == Vector(aliceTrigger))
        // Start trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", bob)
        bobTrigger1 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, bob, _ == Vector(bobTrigger1))
        // Start another trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", bob)
        bobTrigger2 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, bob, _ == Vector(bobTrigger1, bobTrigger2).sorted)
        // Stop Alice's trigger.
        resp <- stopTrigger(uri, aliceTrigger, alice)
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, alice, _.isEmpty)
        _ <- assertTriggerIds(uri, bob, _ == Vector(bobTrigger1, bobTrigger2).sorted)
        // Stop Bob's triggers.
        resp <- stopTrigger(uri, bobTrigger1, bob)
        _ <- assert(resp.status.isSuccess)
        resp <- stopTrigger(uri, bobTrigger2, bob)
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, bob, _.isEmpty)
      } yield succeed
  }

  it should "should enable a trigger on http request" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        // Start the trigger
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
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
        resp <- stopTrigger(uri, triggerId, alice)
        _ <- assert(resp.status.isSuccess)
      } yield succeed
  }

  it should "fail to start a trigger if a ledger client can't be obtained" in withTriggerService(
    Some(dar)) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    // Disable the proxy. This means that the service won't be able to
    // get a ledger connection.
    ledgerProxy.disable()
    try {
      // Request a trigger be started and setup an assertion that
      // completes with success when the running trigger table becomes
      // non-empty.
      val runningTriggersNotEmpty = for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        triggerId <- parseTriggerId(resp)
      } yield (assertTriggerIds(uri, alice, _.nonEmpty))
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

  it should "stop a failing trigger that can't be restarted" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      // Simulate the ledger becoming unrecoverably unavailable due to
      // network connectivity loss. The stop strategy means the running
      // trigger will be terminated.
      for {
        // Request a trigger be started for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        // Proceed when it's confirmed to be running.
        _ <- assertTriggerIds(uri, alice, _ == Vector(aliceTrigger))
        // Simulate unrecoverable network connectivity loss.
        _ <- Future { ledgerProxy.disable() }
        // Confirm that the running trigger ends up stopped and that
        // its history matches our expectations.
        _ <- assertTriggerIds(uri, alice, _.isEmpty)
        _ <- assertTriggerStatus(uri, aliceTrigger, _.last == "stopped: initialization failure")
      } yield succeed
  }

  it should "restart a trigger failing due to a dropped connection" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      // Simulate the ledger briefly being unavailable due to network
      // connectivity loss. Our restart strategy means that the running
      // trigger gets restarted.
      for {
        // Request a trigger be started for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        // Proceed when it is confirmed to be running.
        _ <- assertTriggerIds(uri, alice, _ == Vector(aliceTrigger))
        // Simulate brief network connectivity loss.
        _ <- Future { ledgerProxy.disable() }
        _ <- Future { ledgerProxy.enable() }
        // Check that the trigger survived the outage and that its
        // history shows it went through a restart.
        _ <- assertTriggerIds(uri, alice, _ == Vector(aliceTrigger))
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          triggerStatus => {
            triggerStatus.count(_ == "stopped: runtime failure") == 1 &&
            triggerStatus.last == "running"
          }
        )
      } yield succeed
  }

  it should "restart triggers with script init errors" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:ErrorTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          triggerStatus => {
            triggerStatus.count(_ == "starting") ==
              ServiceConfig.DefaultMaxFailureNumberOfRetries + 1 &&
            triggerStatus.last == "stopped: initialization failure"
          }
        )
        _ <- assertTriggerIds(uri, alice, _.isEmpty)
      } yield succeed
  }

  it should "restart triggers with script update errors" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:LowLevelErrorTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          triggerStatus => {
            triggerStatus
              .count(_ == "running") == ServiceConfig.DefaultMaxFailureNumberOfRetries + 1 &&
            triggerStatus.last == "stopped: runtime failure"
          }
        )
        _ <- assertTriggerIds(uri, alice, _.isEmpty)
      } yield succeed
  }

  it should "give an 'unauthorized' response for a stop request without an authorization header" in withTriggerService(
    None) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    val uuid: String = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      uri = uri.withPath(Uri.Path(s"/v1/stop/$uuid")),
    )
    for {
      resp <- Http().singleRequest(req)
      _ <- resp.status should equal(StatusCodes.Unauthorized)
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      _ <- fields.get("status") should equal(Some(JsNumber(StatusCodes.Unauthorized.intValue)))
      _ <- fields.get("errors") should equal(
        Some(JsArray(JsString("missing Authorization header with Basic Token"))))
    } yield succeed
  }

  it should "give an 'unauthorized' response for a start request with an invalid party identifier" in withTriggerService(
    Some(dar)) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    for {
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", User("Alice-!", "&alC2l3SDS*V"))
      _ <- resp.status should equal(StatusCodes.Unauthorized)
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      _ <- fields.get("status") should equal(Some(JsNumber(StatusCodes.Unauthorized.intValue)))
      _ <- fields.get("errors") should equal(
        Some(JsArray(JsString("invalid party identifier 'Alice-!'"))))
    } yield succeed
  }

  it should "give a 'not found' response for a stop request with an unparseable UUID" in withTriggerService(
    None) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    val uuid: String = "No More Mr Nice Guy"
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      uri = uri.withPath(Uri.Path(s"/v1/stop/$uuid")),
    )
    for {
      resp <- Http().singleRequest(req)
      _ <- resp.status should equal(StatusCodes.NotFound)
    } yield succeed
  }

  it should "give a 'not found' response for a stop request on an unknown UUID" in withTriggerService(
    None) { (uri: Uri, client: LedgerClient, ledgerProxy: Proxy) =>
    val uuid = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    for {
      resp <- stopTrigger(uri, uuid, alice)
      _ <- resp.status should equal(StatusCodes.NotFound)
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      _ <- fields.get("status") should equal(Some(JsNumber(StatusCodes.NotFound.intValue)))
      _ <- fields.get("errors") should equal(
        Some(JsArray(JsString(s"No trigger running with id $uuid"))))
    } yield succeed
  }
}

// Tests for in-memory mode only go here
class TriggerServiceTestInMem extends AbstractTriggerServiceTest {

  override def jdbcConfig: Option[JdbcConfig] = None

}

// Tests for database mode only go here
class TriggerServiceTestWithDb extends AbstractTriggerServiceTest with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)

  // Lazy because the postgresDatabase is only available once the tests start
  private lazy val jdbcConfig_ = JdbcConfig(postgresDatabase.url, "operator", "password")
}
