// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.trigger

import com.digitalasset.daml.lf.archive.{Dar, DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast.Package
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
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

import com.digitalasset.daml.bazeltools.BazelRunfiles.requiredResource
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.command_service._
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  TransactionFilter,
  InclusiveFilters
}
import com.digitalasset.ledger.client.LedgerClient

class ServiceTest extends AsyncFlatSpec with Eventually with Matchers {

  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15, Seconds)), interval = scaled(Span(1, Seconds)))

  val darPath = requiredResource("triggers/service/test-model.dar")
  val encodedDar =
    DarReader().readArchiveFromFile(darPath).get
  val dar = encodedDar.map {
    case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
  }

  def submitCmd(client: LedgerClient, party: String, cmd: Command) = {
    val req = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = testId,
          ledgerId = client.ledgerId.unwrap,
          commandId = UUID.randomUUID.toString,
          ledgerEffectiveTime = None,
          maximumRecordTime = None,
          commands = Seq(cmd)
        )))
    client.commandServiceClient.submitAndWait(req)
  }

  def testId: String = this.getClass.getSimpleName
  implicit val system: ActorSystem = ActorSystem(testId)
  implicit val esf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool(testId)(system)
  implicit val ec: ExecutionContext = system.dispatcher

  def withHttpService[A](triggerDar: Option[Dar[(PackageId, Package)]])
    : ((Uri, LedgerClient) => Future[A]) => Future[A] =
    TriggerServiceFixture
      .withTriggerService[A](testId, List(darPath), triggerDar)

  def startTrigger(uri: Uri, id: String, party: String) = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path("/start")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"identifier": "$id", "party": "$party"}"""
      )
    )
    Http().singleRequest(req)
  }

  def stopTrigger(uri: Uri, id: String) = {
    val req = HttpRequest(
      method = HttpMethods.DELETE,
      uri = uri.withPath(Uri.Path(s"/stop/$id")),
    )
    Http().singleRequest(req)
  }

  def uploadDar(uri: Uri, file: File) = {
    val fileContentsSource: Source[ByteString, Any] = FileIO.fromPath(file.toPath)
    val multipartForm = Multipart.FormData(
      Multipart.FormData.BodyPart(
        "dar",
        HttpEntity.IndefiniteLength(ContentTypes.`application/octet-stream`, fileContentsSource),
        Map("filename" -> file.toString)))
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(Uri.Path(s"/upload_dar")),
      entity = multipartForm.toEntity
    )
    Http().singleRequest(req)
  }

  it should "should fail for non-existent trigger" in withHttpService(Some(dar)) {
    (uri: Uri, client) =>
      for {
        resp <- startTrigger(uri, s"${dar.main._1}:TestTrigger:foobar", "Alice")
        body <- {
          assert(resp.status == StatusCodes.UnprocessableEntity)
          resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
        }
      } yield assert(body == "Could not find name foobar in module TestTrigger")
  }

  it should "find a trigger after uploading it" in withHttpService(None) { (uri: Uri, client) =>
    for {
      // attempt to start trigger before uploading which fails.
      resp <- startTrigger(uri, s"${dar.main._1}:TestTrigger:trigger", "Alice")
      _ <- assert(resp.status == StatusCodes.UnprocessableEntity)
      resp <- uploadDar(uri, darPath)
      body <- resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
      _ <- body should startWith("DAR uploaded")
      resp <- startTrigger(uri, s"${dar.main._1}:TestTrigger:trigger", "Alice")
      _ <- assert(resp.status.isSuccess)
      triggerId <- resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
      resp <- stopTrigger(uri, triggerId)
      _ <- assert(resp.status.isSuccess)
    } yield succeed
  }

  it should "should enable a trigger on http request" in withHttpService(Some(dar)) {
    (uri: Uri, client) =>
      // start the trigger
      for {
        resp <- startTrigger(uri, s"${dar.main._1}:TestTrigger:trigger", "Alice")
        triggerId <- {
          assert(resp.status.isSuccess)
          resp.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
        }
        // Trigger is running, create an A contract
        _ <- {
          val cmd = Command().withCreate(
            CreateCommand(
              templateId = Some(Identifier(dar.main._1, "TestTrigger", "A")),
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
        val filter = TransactionFilter(List(("Alice", Filters(Some(InclusiveFilters(Seq(Identifier(dar.main._1, "TestTrigger", "B"))))))).toMap)
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
        resp <- stopTrigger(uri, triggerId)
      } yield (assert(resp.status.isSuccess))
  }
}
