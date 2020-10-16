// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.data.Ref._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.ByteString
import akka.stream.scaladsl.{FileIO, Sink, Source}
import java.io.File
import java.util.UUID

import org.scalactic.source
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scalaz.Tag
import scalaz.syntax.tag._
import spray.json._
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.dao.DbTriggerDao
import com.daml.testing.postgresql.PostgresAroundAll
import com.typesafe.scalalogging.StrictLogging
import eu.rekawek.toxiproxy._

import scala.collection.concurrent.TrieMap
import scala.util.Success

/**
  * A test-fixture that persists cookies between http requests for each test-case.
  */
trait HttpCookies extends BeforeAndAfterEach { this: Suite =>
  private val cookieJar = TrieMap[String, String]()

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally cookieJar.clear()
  }

  /**
    * Adds a Cookie header for the currently stored cookies and performs the given http request.
    */
  def httpRequest(request: HttpRequest)(
      implicit system: ActorSystem,
      ec: ExecutionContext): Future[HttpResponse] = {
    Http()
      .singleRequest {
        if (cookieJar.nonEmpty) {
          val cookies = headers.Cookie(values = cookieJar.to[Seq]: _*)
          request.addHeader(cookies)
        } else {
          request
        }
      }
      .andThen {
        case Success(resp) =>
          resp.headers.foreach {
            case headers.`Set-Cookie`(cookie) =>
              cookieJar.update(cookie.name, cookie.value)
            case _ =>
          }
      }
  }

  /**
    * Same as [[httpRequest]] but will follow redirections.
    */
  def httpRequestFollow(request: HttpRequest, maxRedirections: Int = 10)(
      implicit system: ActorSystem,
      ec: ExecutionContext): Future[HttpResponse] = {
    httpRequest(request).flatMap {
      case resp @ HttpResponse(StatusCodes.Redirection(_), _, _, _) =>
        if (maxRedirections == 0) {
          throw new RuntimeException("Too many redirections")
        } else {
          val uri = resp.header[headers.Location].get.uri
          httpRequestFollow(HttpRequest(uri = uri), maxRedirections - 1)
        }
      case resp => Future(resp)
    }
  }
}

abstract class AbstractTriggerServiceTest
    extends AsyncFlatSpec
    with HttpCookies
    with Eventually
    with Matchers
    with StrictLogging {

  import AbstractTriggerServiceTest.CompatAssertion

  // Abstract member for testing with and without a database
  def jdbcConfig: Option[JdbcConfig]

  // Abstract member for testing with and without authentication/authorization
  def authTestConfig: Option[AuthTestConfig]

  // Default retry config for `eventually`
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(1, Seconds)))

  protected val darPath = requiredResource("triggers/service/test-model.dar")

  // Encoded dar used in service initialization
  protected val dar = DarReader().readArchiveFromFile(darPath).get
  protected val testPkgId = dar.main._1

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

  protected val alice: Party = Tag("Alice")
  protected val bob: Party = Tag("Bob")

  def withTriggerService[A](encodedDar: Option[Dar[(PackageId, DamlLf.ArchivePayload)]])(
      testFn: (Uri, LedgerClient, Proxy) => Future[A])(implicit pos: source.Position): Future[A] =
    TriggerServiceFixture.withTriggerService(
      testId,
      List(darPath),
      encodedDar,
      jdbcConfig,
      authTestConfig)(testFn)

  def startTrigger(uri: Uri, triggerName: String, party: Party): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/v1/start")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"triggerName": "$triggerName", "party": "$party"}"""
      )
    )
    httpRequestFollow(req)
  }

  def listTriggers(uri: Uri, party: Party): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/list")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"party": "$party"}"""
      )
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

  def stopTrigger(uri: Uri, triggerInstance: UUID, party: Party): Future[HttpResponse] = {
    // silence unused warning, we probably need this parameter again when we
    // support auth.
    val _ = party
    val id = triggerInstance.toString
    val req = HttpRequest(
      method = HttpMethods.DELETE,
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

  def assertTriggerIds(uri: Uri, party: Party, expected: Vector[UUID]): Future[Assertion] =
    for {
      resp <- listTriggers(uri, party)
      result <- parseTriggerIds(resp)
    } yield assert(result == expected)

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

  def assertTriggerStatus[A](
      uri: Uri,
      triggerInstance: UUID,
      pred: Vector[String] => A,
      timeoutSeconds: Long = 15)(implicit A: CompatAssertion[A]): Future[Assertion] = {
    implicit val patienceConfig: PatienceConfig =
      PatienceConfig(
        timeout = scaled(Span(timeoutSeconds, Seconds)),
        interval = scaled(Span(1, Seconds)))
    eventually {
      val actualTriggerStatus = Await.result(for {
        resp <- triggerStatus(uri, triggerInstance)
        result <- parseTriggerStatus(resp)
      } yield result, Duration.Inf)
      A(pred(actualTriggerStatus))
    }
  }

  it should "start up and shut down server" in
    withTriggerService(Some(dar)) { (_, _, _) =>
      Future(succeed)
    }

  it should "allow repeated uploads of the same packages" in
    withTriggerService(Some(dar)) { (uri: Uri, _, _) =>
      for {
        resp <- uploadDar(uri, darPath) // same dar as in initialization
        _ <- parseResult(resp)
        resp <- uploadDar(uri, darPath) // same dar again
        _ <- parseResult(resp)
      } yield succeed
    }

  it should "fail to start non-existent trigger" in withTriggerService(Some(dar)) {
    (uri: Uri, _, _) =>
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

  it should "start a trigger after uploading it" in withTriggerService(None) { (uri: Uri, _, _) =>
    for {
      resp <- uploadDar(uri, darPath)
      JsObject(fields) <- parseResult(resp)
      Some(JsString(mainPackageId)) = fields.get("mainPackageId")
      _ <- mainPackageId should not be empty
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      triggerId <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(triggerId))
      resp <- stopTrigger(uri, triggerId, alice)
      stoppedTriggerId <- parseTriggerId(resp)
      _ <- stoppedTriggerId should equal(triggerId)
    } yield succeed
  }

  it should "start multiple triggers and list them by party" in withTriggerService(Some(dar)) {
    (uri: Uri, _, _) =>
      for {
        resp <- listTriggers(uri, alice)
        result <- parseTriggerIds(resp)
        _ <- result should equal(Vector())
        // Start trigger for Alice.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
        // Start trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", bob)
        bobTrigger1 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, bob, Vector(bobTrigger1))
        // Start another trigger for Bob.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", bob)
        bobTrigger2 <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, bob, Vector(bobTrigger1, bobTrigger2).sorted)
        // Stop Alice's trigger.
        resp <- stopTrigger(uri, aliceTrigger, alice)
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, alice, Vector())
        _ <- assertTriggerIds(uri, bob, Vector(bobTrigger1, bobTrigger2).sorted)
        // Stop Bob's triggers.
        resp <- stopTrigger(uri, bobTrigger1, bob)
        _ <- assert(resp.status.isSuccess)
        resp <- stopTrigger(uri, bobTrigger2, bob)
        _ <- assert(resp.status.isSuccess)
        _ <- assertTriggerIds(uri, bob, Vector())
      } yield succeed
  }

  it should "should enable a trigger on http request" in withTriggerService(Some(dar)) {
    (uri: Uri, client: LedgerClient, _) =>
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

  it should "restart trigger on initialization failure due to failed connection" in withTriggerService(
    Some(dar)) { (uri: Uri, _, ledgerProxy: Proxy) =>
    for {
      // Simulate a failed ledger connection which will prevent triggers from initializing.
      _ <- Future(ledgerProxy.disable())
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      // The start request should succeed and an entry should be added to the running trigger store,
      // even though the trigger will not be able to start.
      aliceTrigger <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
      // Check the log for an initialization failure.
      _ <- assertTriggerStatus(uri, aliceTrigger, _.contains("stopped: initialization failure"))
      // Finally establish the connection and check that the trigger eventually starts.
      _ <- Future(ledgerProxy.enable())
      _ <- assertTriggerStatus(uri, aliceTrigger, _.last == "running")
    } yield succeed
  }

  it should "restart trigger on run-time failure due to dropped connection" in withTriggerService(
    Some(dar)) { (uri: Uri, _, ledgerProxy: Proxy) =>
    // Simulate the ledger being briefly unavailable due to network connectivity loss.
    // We continually restart the trigger until the connection returns.
    for {
      // Request a trigger be started for Alice.
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      aliceTrigger <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
      // Proceed when it's confirmed to be running.
      _ <- assertTriggerStatus(uri, aliceTrigger, _.last == "running")
      // Simulate brief network connectivity loss and observe the trigger fail.
      _ <- Future(ledgerProxy.disable())
      _ <- assertTriggerStatus(uri, aliceTrigger, _.contains("stopped: runtime failure"))
      // Finally check the trigger is restarted after the connection returns.
      _ <- Future(ledgerProxy.enable())
      _ <- assertTriggerStatus(uri, aliceTrigger, _.last == "running")
    } yield succeed
  }

  it should "restart triggers with initialization errors" in withTriggerService(Some(dar)) {
    (uri: Uri, _, _) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:ErrorTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
        // We will attempt to restart the trigger indefinitely.
        // Just check that we see a few failures and restart attempts.
        // This relies on a small minimum restart interval as the interval doubles after each
        // failure.
        _ <- assertTriggerStatus(uri, aliceTrigger, _.count(_ == "starting") > 2)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _.count(_ == "stopped: initialization failure") > 2)
      } yield succeed
  }

  it should "restart triggers with update errors" in withTriggerService(Some(dar)) {
    (uri: Uri, _, _) =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:LowLevelErrorTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
        // We will attempt to restart the trigger indefinitely.
        // Just check that we see a few failures and restart attempts.
        // This relies on a small minimum restart interval as the interval doubles after each
        // failure.
        _ <- assertTriggerStatus(uri, aliceTrigger, _.count(_ == "starting") should be > 2)
        _ <- assertTriggerStatus(
          uri,
          aliceTrigger,
          _.count(_ == "stopped: runtime failure") should be > 2)
      } yield succeed
  }

  it should "give a 'not found' response for a stop request with an unparseable UUID" in withTriggerService(
    None) { (uri: Uri, _, _) =>
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
    None) { (uri: Uri, _, _) =>
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

object AbstractTriggerServiceTest {
  import org.scalactic.Prettifier, org.scalactic.source.Position
  import Assertions.{assert, assertionsHelper}

  sealed trait CompatAssertion[-A] {
    def apply(a: A): Assertion
  }
  object CompatAssertion {
    private def mk[A](f: A => Assertion) = new CompatAssertion[A] {
      override def apply(a: A) = f(a)
    }
    implicit val id: CompatAssertion[Assertion] = mk(a => a)
    implicit def bool(implicit pretty: Prettifier, pos: Position): CompatAssertion[Boolean] =
      mk(assert(_)(pretty, pos))
  }
}

// Tests for in-memory mode only go here
class TriggerServiceTestInMem extends AbstractTriggerServiceTest {

  override def jdbcConfig: Option[JdbcConfig] = None
  override def authTestConfig: Option[AuthTestConfig] = None

}

// Tests for database mode only go here
class TriggerServiceTestWithDb
    extends AbstractTriggerServiceTest
    with BeforeAndAfterEach
    with PostgresAroundAll {

  override def jdbcConfig: Option[JdbcConfig] = Some(jdbcConfig_)
  override def authTestConfig: Option[AuthTestConfig] = None

  // Lazy because the postgresDatabase is only available once the tests start
  private lazy val jdbcConfig_ = JdbcConfig(postgresDatabase.url, "operator", "password")
  private lazy val triggerDao =
    DbTriggerDao(jdbcConfig_, poolSize = dao.Connection.PoolSize.IntegrationTest)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    triggerDao.initialize fold (fail(_), identity)
  }

  override protected def afterEach(): Unit = {
    triggerDao.destroy() fold (fail(_), identity)
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    triggerDao.destroyPermanently() fold (fail(_), identity)
    super.afterAll()
  }

  behavior of "persistent backend"

  it should "recover packages after shutdown" in (for {
    _ <- withTriggerService(None) { (uri: Uri, _, _) =>
      for {
        resp <- uploadDar(uri, darPath)
        _ <- parseResult(resp)
      } yield succeed
    }
    // Once service is shutdown, start a new one and try to use the previously uploaded dar
    _ <- withTriggerService(None) { (uri: Uri, _, _) =>
      for {
        // start trigger defined in previously uploaded dar
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        triggerId <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(triggerId))
      } yield succeed
    }
  } yield succeed)

  it should "restart triggers after shutdown" in (for {
    _ <- withTriggerService(Some(dar)) { (uri: Uri, _, _) =>
      for {
        // Start a trigger in the first run of the service.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        triggerId <- parseTriggerId(resp)
        // The new trigger should be in the running trigger store and eventually running.
        _ <- assertTriggerIds(uri, alice, Vector(triggerId))
        _ <- assertTriggerStatus(uri, triggerId, _.last should ===("running"))
      } yield succeed
    }
    // Once service is shutdown, start a new one and check the previously running trigger is restarted.
    // also tests vacuous DB migration, incidentally
    _ <- withTriggerService(None) { (uri: Uri, _, _) =>
      for {
        // Get the previous trigger instance using a list request
        resp <- listTriggers(uri, alice)
        triggerIds <- parseTriggerIds(resp)
        _ = triggerIds.length should ===(1)
        aliceTrigger = triggerIds.head
        // Currently the logs aren't persisted so we can check that the trigger was restarted by
        // inspecting the new log.
        _ <- assertTriggerStatus(uri, aliceTrigger, _.last should ===("running"), 60)

        // Finally go ahead and stop the trigger.
        _ <- stopTrigger(uri, aliceTrigger, alice)
        _ <- assertTriggerIds(uri, alice, Vector())
        _ <- assertTriggerStatus(uri, aliceTrigger, _.last should ===("stopped: by user request"))
      } yield succeed
    }
  } yield succeed)

}

// Tests for auth mode only go here
class TriggerServiceTestAuth extends AbstractTriggerServiceTest {

  override def jdbcConfig: Option[JdbcConfig] = None
  override def authTestConfig: Option[AuthTestConfig] =
    Some(
      AuthTestConfig(
        jwtSecret = "secret",
        parties = List(alice, bob),
      ))

}
