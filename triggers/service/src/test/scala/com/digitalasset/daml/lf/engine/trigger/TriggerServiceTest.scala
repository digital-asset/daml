// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.lf.archive.DarReader
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.util.ByteString
import akka.stream.scaladsl.{FileIO, Sink, Source}
import java.io.File
import java.util.UUID

import akka.http.scaladsl.model.Uri.Query
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future
import scalaz.Tag
import scalaz.syntax.tag._
import spray.json._
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Boundary
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement
import com.daml.timer.RetryStrategy
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._

// Tests for all trigger service configurations go here
trait AbstractTriggerServiceTest
    extends AsyncFlatSpec
    with HttpCookies
    with TriggerServiceFixture
    with Matchers
    with StrictLogging
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)))

  import AbstractTriggerServiceTest.CompatAssertion

  protected val darPath = requiredResource("triggers/service/test-model.dar")

  // Encoded dar used in service initialization
  protected val dar = DarReader().readArchiveFromFile(darPath).get
  protected val testPkgId = dar.main._1
  override protected val damlPackages: List[File] = List(darPath)

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
  protected override def actorSystemName = testId

  protected val alice: Party = Tag("Alice")
  // This party is used by the test that queries the ACS.
  // To avoid mixing this up with the other tests, we use a separate
  // party.
  protected val aliceAcs: Party = Tag("Alice_acs")
  protected val bob: Party = Tag("Bob")
  protected val eve: Party = Tag("Eve")

  def startTrigger(
      uri: Uri,
      triggerName: String,
      party: Party,
      applicationId: Option[ApplicationId] = None): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/v1/triggers")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"triggerName": "$triggerName", "party": "$party", "applicationId": "${applicationId
          .getOrElse("null")}"}"""
      )
    )
    httpRequestFollow(req)
  }

  def listTriggers(uri: Uri, party: Party): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path(s"/v1/triggers")).withQuery(Query(("party", party.toString))),
    )
    httpRequestFollow(req)
  }

  def stopTrigger(uri: Uri, triggerInstance: UUID, party: Party): Future[HttpResponse] = {
    // silence unused warning, we probably need this parameter again when we
    // support auth.
    val _ = party
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      uri = uri.withPath(Uri.Path(s"/v1/triggers") / (triggerInstance.toString)),
    )
    httpRequestFollow(req)
  }

  def triggerStatus(uri: Uri, triggerInstance: UUID): Future[HttpResponse] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(Uri.Path("/v1/triggers") / (triggerInstance.toString))
    )
    httpRequestFollow(req)
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
      uri = uri.withPath(Uri.Path(s"/v1/packages")),
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
      body <- responseBodyToString(resp)
      _ <- assert(resp.status.isSuccess)
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

  def assertTriggerStatus[A](triggerInstance: UUID, pred: Vector[String] => A)(
      implicit A: CompatAssertion[A]): Future[Assertion] =
    eventually {
      A(pred(getTriggerStatus(triggerInstance).map(_._2)))
    }

  it should "start up and shut down server" in
    withTriggerService(List(dar)) { _ =>
      Future(succeed)
    }

  it should "allow repeated uploads of the same packages" in
    withTriggerService(List(dar)) { uri: Uri =>
      for {
        resp <- uploadDar(uri, darPath) // same dar as in initialization
        _ <- parseResult(resp)
        resp <- uploadDar(uri, darPath) // same dar again
        _ <- parseResult(resp)
      } yield succeed
    }

  it should "fail to start non-existent trigger" in withTriggerService(List(dar)) { uri: Uri =>
    val expectedError = StatusCodes.UnprocessableEntity
    for {
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:foobar", alice)
      _ <- resp.status shouldBe expectedError
      // Check the "status" and "errors" fields
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      _ <- fields.get("status") shouldBe Some(JsNumber(expectedError.intValue))
      _ <- fields.get("errors") shouldBe
        Some(JsArray(JsString("Could not find name foobar in module TestTrigger")))
    } yield succeed
  }

  it should "start a trigger after uploading it" in withTriggerService(Nil) { uri: Uri =>
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
      _ <- stoppedTriggerId shouldBe triggerId
    } yield succeed
  }

  it should "start multiple triggers and list them by party" in withTriggerService(List(dar)) {
    uri: Uri =>
      for {
        resp <- listTriggers(uri, alice)
        result <- parseTriggerIds(resp)
        _ <- result shouldBe Vector()
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

  it should "should enable a trigger on http request" in withTriggerService(List(dar)) { uri: Uri =>
    for {
      client <- sandboxClient(
        ApiTypes.ApplicationId("my-app-id"),
        actAs = List(ApiTypes.Party(aliceAcs.unwrap)))
      filter = TransactionFilter(
        List(
          (
            aliceAcs.unwrap,
            Filters(Some(InclusiveFilters(Seq(Identifier(testPkgId, "TestTrigger", "B"))))))).toMap)
      // Make sure that no contracts exist initially to guard against accidental
      // party reuse.
      acs <- client.activeContractSetClient
        .getActiveContracts(filter)
        .runWith(Sink.seq)
        .map(acsPages => acsPages.flatMap(_.activeContracts))
      _ = acs shouldBe Vector()
      // Start the trigger
      resp <- startTrigger(
        uri,
        s"$testPkgId:TestTrigger:trigger",
        aliceAcs,
        Some(ApplicationId("my-app-id")))
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
                  RecordField(value = Some(Value().withParty(aliceAcs.unwrap))),
                  RecordField(value = Some(Value().withInt64(42)))))),
          ))
        submitCmd(client, aliceAcs.unwrap, cmd)
      }
      // Query ACS until we see a B contract
      _ <- RetryStrategy.constant(5, 1.seconds) { (_, _) =>
        for {
          acs <- client.activeContractSetClient
            .getActiveContracts(filter)
            .runWith(Sink.seq)
            .map(acsPages => acsPages.flatMap(_.activeContracts))
        } yield assert(acs.length == 1)
      }
      // Read completions to make sure we set the right app id.
      r <- client.commandClient
        .completionSource(List(aliceAcs.unwrap), LedgerOffset(Boundary(LEDGER_BEGIN)))
        .collect({
          case CompletionStreamElement.CompletionElement(completion)
              if !completion.transactionId.isEmpty =>
            completion
        })
        .take(1)
        .runWith(Sink.seq)
      _ = r.length shouldBe 1
      status <- triggerStatus(uri, triggerId)
      _ = status.status shouldBe StatusCodes.OK
      body <- responseBodyToString(status)
      _ = body shouldBe s"""{"result":{"party":"Alice_acs","status":"running","triggerId":"$testPkgId:TestTrigger:trigger"},"status":200}"""
      resp <- stopTrigger(uri, triggerId, alice)
      _ <- assert(resp.status.isSuccess)
    } yield succeed
  }

  it should "restart trigger on initialization failure due to failed connection" in withTriggerService(
    List(dar)) { uri: Uri =>
    for {
      // Simulate a failed ledger connection which will prevent triggers from initializing.
      _ <- Future(toxiSandboxProxy.disable())
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      // The start request should succeed and an entry should be added to the running trigger store,
      // even though the trigger will not be able to start.
      aliceTrigger <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
      // Check the log for an initialization failure.
      _ <- assertTriggerStatus(aliceTrigger, _.contains("stopped: initialization failure"))
      // Finally establish the connection and check that the trigger eventually starts.
      _ <- Future(toxiSandboxProxy.enable())
      _ <- assertTriggerStatus(aliceTrigger, _.last == "running")
    } yield succeed
  }

  it should "restart trigger on run-time failure due to dropped connection" in withTriggerService(
    List(dar)) { uri: Uri =>
    // Simulate the ledger being briefly unavailable due to network connectivity loss.
    // We continually restart the trigger until the connection returns.
    for {
      // Request a trigger be started for Alice.
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      aliceTrigger <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
      // Proceed when it's confirmed to be running.
      _ <- assertTriggerStatus(aliceTrigger, _.last == "running")
      // Simulate brief network connectivity loss and observe the trigger fail.
      _ <- Future(toxiSandboxProxy.disable())
      _ <- assertTriggerStatus(aliceTrigger, _.contains("stopped: runtime failure"))
      // Finally check the trigger is restarted after the connection returns.
      _ <- Future(toxiSandboxProxy.enable())
      _ <- assertTriggerStatus(aliceTrigger, _.last == "running")
    } yield succeed
  }

  it should "restart triggers with initialization errors" in withTriggerService(List(dar)) {
    uri: Uri =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:ErrorTrigger:trigger", alice)
        aliceTrigger <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
        // We will attempt to restart the trigger indefinitely.
        // Just check that we see a few failures and restart attempts.
        // This relies on a small minimum restart interval as the interval doubles after each
        // failure.
        _ <- assertTriggerStatus(aliceTrigger, _.count(_ == "starting") > 2)
        _ <- assertTriggerStatus(aliceTrigger, _.count(_ == "stopped: initialization failure") > 2)
      } yield succeed
  }

  it should "restart triggers with update errors" in withTriggerService(List(dar)) { uri: Uri =>
    for {
      resp <- startTrigger(uri, s"$testPkgId:LowLevelErrorTrigger:trigger", alice)
      aliceTrigger <- parseTriggerId(resp)
      _ <- assertTriggerIds(uri, alice, Vector(aliceTrigger))
      // We will attempt to restart the trigger indefinitely.
      // Just check that we see a few failures and restart attempts.
      // This relies on a small minimum restart interval as the interval doubles after each
      // failure.
      _ <- assertTriggerStatus(aliceTrigger, _.count(_ == "starting") should be > 2)
      _ <- assertTriggerStatus(aliceTrigger, _.count(_ == "stopped: runtime failure") should be > 2)
    } yield succeed
  }

  it should "give a 'not found' response for a stop request with an unparseable UUID" in withTriggerService(
    Nil) { uri: Uri =>
    val uuid: String = "No More Mr Nice Guy"
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      uri = uri.withPath(Uri.Path(s"/v1/triggers/$uuid")),
    )
    for {
      resp <- Http().singleRequest(req)
      _ <- resp.status shouldBe StatusCodes.NotFound
    } yield succeed
  }

  it should "give a 'not found' response for a stop request on an unknown UUID" in withTriggerService(
    Nil) { uri: Uri =>
    val uuid = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    for {
      resp <- stopTrigger(uri, uuid, alice)
      _ <- resp.status shouldBe StatusCodes.NotFound
      body <- responseBodyToString(resp)
      JsObject(fields) = body.parseJson
      _ <- fields.get("status") shouldBe Some(JsNumber(StatusCodes.NotFound.intValue))
      _ <- fields.get("errors") shouldBe
        Some(JsArray(JsString(s"No trigger running with id $uuid")))
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

// Tests for in-memory trigger service configurations go here
trait AbstractTriggerServiceTestInMem
    extends AbstractTriggerServiceTest
    with TriggerDaoInMemFixture {}

// Tests for database trigger service configurations go here
trait AbstractTriggerServiceTestWithDb
    extends AbstractTriggerServiceTest
    with TriggerDaoPostgresFixture {

  behavior of "persistent backend"

  it should "recover packages after shutdown" in (for {
    _ <- withTriggerService(Nil) { uri: Uri =>
      for {
        resp <- uploadDar(uri, darPath)
        _ <- parseResult(resp)
      } yield succeed
    }
    // Once service is shutdown, start a new one and try to use the previously uploaded dar
    _ <- withTriggerService(Nil) { uri: Uri =>
      for {
        // start trigger defined in previously uploaded dar
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        triggerId <- parseTriggerId(resp)
        _ <- assertTriggerIds(uri, alice, Vector(triggerId))
      } yield succeed
    }
  } yield succeed)

  it should "restart triggers after shutdown" in (for {
    _ <- withTriggerService(List(dar)) { uri: Uri =>
      for {
        // Start a trigger in the first run of the service.
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        triggerId <- parseTriggerId(resp)
        // The new trigger should be in the running trigger store and eventually running.
        _ <- assertTriggerIds(uri, alice, Vector(triggerId))
        _ <- assertTriggerStatus(triggerId, _.last should ===("running"))
      } yield succeed
    }
    // Once service is shutdown, start a new one and check the previously running trigger is restarted.
    // also tests vacuous DB migration, incidentally
    _ <- withTriggerService(Nil) { uri: Uri =>
      for {
        // Get the previous trigger instance using a list request
        resp <- listTriggers(uri, alice)
        triggerIds <- parseTriggerIds(resp)
        _ = triggerIds.length should ===(1)
        aliceTrigger = triggerIds.head
        // Currently the logs aren't persisted so we can check that the trigger was restarted by
        // inspecting the new log.
        _ <- assertTriggerStatus(aliceTrigger, _.last should ===("running"))

        // Finally go ahead and stop the trigger.
        _ <- stopTrigger(uri, aliceTrigger, alice)
        _ <- assertTriggerIds(uri, alice, Vector())
        _ <- assertTriggerStatus(aliceTrigger, _.last should ===("stopped: by user request"))
      } yield succeed
    }
  } yield succeed)
}

// Tests for non-authenticated trigger service configurations go here
trait AbstractTriggerServiceTestNoAuth extends AbstractTriggerServiceTest with NoAuthFixture {}

// Tests for authenticated trigger service configurations go here
trait AbstractTriggerServiceTestAuthMiddleware
    extends AbstractTriggerServiceTest
    with AuthMiddlewareFixture {

  override protected val authParties = Some(Set(alice, aliceAcs, bob))

  behavior of "authenticated service"

  it should "forbid a non-authorized party to start a trigger" in withTriggerService(List(dar)) {
    uri: Uri =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", eve)
        _ <- resp.status shouldBe StatusCodes.Forbidden
      } yield succeed
  }

  it should "forbid a non-authorized party to list triggers" in withTriggerService(Nil) {
    uri: Uri =>
      for {
        resp <- listTriggers(uri, eve)
        _ <- resp.status shouldBe StatusCodes.Forbidden
      } yield succeed
  }

  it should "forbid a non-authorized party to check the status of a trigger" in withTriggerService(
    List(dar)) { uri: Uri =>
    for {
      resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
      _ <- resp.status shouldBe StatusCodes.OK
      triggerId <- parseTriggerId(resp)
      // emulate access by a different user by revoking access to alice and deleting the current token cookie
      _ = authServer.revokeParty(alice)
      _ = deleteCookies()
      resp <- triggerStatus(uri, triggerId)
      _ <- resp.status shouldBe StatusCodes.Forbidden
    } yield succeed
  }

  it should "forbid a non-authorized party to stop a trigger" in withTriggerService(List(dar)) {
    uri: Uri =>
      for {
        resp <- startTrigger(uri, s"$testPkgId:TestTrigger:trigger", alice)
        _ <- resp.status shouldBe StatusCodes.OK
        triggerId <- parseTriggerId(resp)
        // emulate access by a different user by revoking access to alice and deleting the current token cookie
        _ = authServer.revokeParty(alice)
        _ = deleteCookies()
        resp <- stopTrigger(uri, triggerId, alice)
        _ <- resp.status shouldBe StatusCodes.Forbidden
      } yield succeed
  }
}
