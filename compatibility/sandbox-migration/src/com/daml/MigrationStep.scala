// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.validation.ValueValidator
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction._
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.archive.{DarReader, Decode}
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, Party, QualifiedName}
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import com.daml.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue,
}
import com.daml.platform.server.api.validation.FieldValidations.validateIdentifier
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scopt.Read
import spray.json._

case class Config(
    outputFile: Path,
    dar: Path,
    host: String,
    port: Int,
    proposer: String,
    accepter: String,
    note: String,
)

case class Result(
    oldTProposals: Seq[(ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])],
    newTProposals: Seq[(ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])],
    oldTs: Seq[(ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])],
    newTs: Seq[(ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])],
    oldTransactions: Seq[TransactionResult],
    newTransactions: Seq[TransactionResult],
)

case class TransactionResult(
    transactionId: String,
    events: Seq[EventResult],
)

sealed trait EventResult;
final case class CreatedResult(
    templateName: String,
    contractId: String,
    argument: Value[AbsoluteContractId])
    extends EventResult
final case class ArchivedResult(templateName: String, contractId: String) extends EventResult

object MigrationStep {

  private def toEventResult(e: Event): EventResult = {
    e.event match {
      case Event.Event.Created(created) =>
        CreatedResult(
          validateIdentifier(created.getTemplateId).right.get.qualifiedName.toString,
          created.contractId,
          ValueValidator.validateRecord(created.getCreateArguments).right.get
        )
      case Event.Event.Archived(archived) =>
        ArchivedResult(
          validateIdentifier(archived.getTemplateId).right.get.qualifiedName.toString,
          archived.contractId)
      case Event.Event.Empty => throw new RuntimeException("Invalid empty event")
    }
  }

  private def toTransactionResult(t: Transaction): TransactionResult = {
    TransactionResult(t.transactionId, t.events.map(toEventResult))
  }

  private implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  implicit val ContractIdFormat: JsonFormat[AbsoluteContractId] =
    new JsonFormat[AbsoluteContractId] {
      override def write(obj: AbsoluteContractId) =
        JsString(obj.coid)
      override def read(json: JsValue) = json match {
        case JsString(s) =>
          AbsoluteContractId.assertFromString(s)
        case _ => deserializationError("ContractId must be a string")
      }
    }

  object LfValueCodec
      extends ApiCodecCompressed[AbsoluteContractId](
        encodeDecimalAsString = true,
        encodeInt64AsString = true) {
    object JsonImplicits extends DefaultJsonProtocol {
      implicit object ApiValueJsonFormat extends RootJsonWriter[Value[AbsoluteContractId]] {
        def write(v: Value[AbsoluteContractId]): JsValue = apiValueToJsValue(v)
      }
    }
  }
  object ResultProtocol extends DefaultJsonProtocol {
    implicit object ContractTupleFormat
        extends RootJsonWriter[(ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])] {
      def write(t: (ValueContractId[AbsoluteContractId], Value[AbsoluteContractId])) =
        JsObject(
          Seq(
            ("_1", LfValueCodec.apiValueToJsValue(t._1)),
            ("_2", LfValueCodec.apiValueToJsValue(t._2))).toMap)
    }
    implicit object EventResultFormat extends RootJsonWriter[EventResult] {
      def write(e: EventResult) = e match {
        case CreatedResult(tpl, cid, argument) =>
          JsObject(
            Seq(
              ("type", "created".toJson),
              ("template", tpl.toJson),
              ("cid", cid.toJson),
              ("argument", LfValueCodec.apiValueToJsValue(argument))).toMap)
        case ArchivedResult(tpl, cid) =>
          JsObject(
            Seq(("type", "archived".toJson), ("template", tpl.toJson), ("cid", cid.toJson)).toMap)
      }
    }
    implicit object TransactionResultFormat extends RootJsonWriter[TransactionResult] {
      def write(t: TransactionResult) =
        JsObject(
          Seq(
            ("transactionId", t.transactionId.toJson),
            ("events", JsArray(t.events.map(_.toJson): _*))).toMap)
    }
    implicit object ResultFormat extends RootJsonWriter[Result] {
      def write(r: Result): JsValue =
        JsObject(
          Seq(
            ("oldTProposals", JsArray(r.oldTProposals.map(_.toJson): _*)),
            ("newTProposals", JsArray(r.newTProposals.map(_.toJson): _*)),
            ("oldTs", JsArray(r.oldTs.map(_.toJson): _*)),
            ("newTs", JsArray(r.newTs.map(_.toJson): _*)),
            ("oldTransactions", JsArray(r.oldTransactions.map(_.toJson): _*)),
            ("newTransactions", JsArray(r.newTransactions.map(_.toJson): _*)),
          ).toMap)

    }
  }

  import ResultProtocol._

  private val parser = new scopt.OptionParser[Config]("migration-step") {
    opt[Path]("output")
      .action((path, c) => c.copy(outputFile = path))
      .required()
    opt[Path]("dar")
      .action((dar, c) => c.copy(dar = dar))
      .required()
    opt[String]("host")
      .action((host, c) => c.copy(host = host))
      .required()
    opt[Int]("port")
      .action((port, c) => c.copy(port = port))
      .required()
    opt[String]("proposer")
      .action((proposer, c) => c.copy(proposer = proposer))
      .required()
    opt[String]("accepter")
      .action((accepter, c) => c.copy(accepter = accepter))
      .required()
    opt[String]("note")
      .action((note, c) => c.copy(note = note))
      .required()
  }
  // All parameters are required so we can safely set things to null here.
  private val defaultConfig = Config(null, null, null, 0, null, null, null)
  private val appId = "migration-step"
  def queryACS(client: LedgerClient, party: String, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer)
    : Future[Seq[(ValueContractId[AbsoluteContractId], ValueRecord[AbsoluteContractId])]] = {
    val filter = TransactionFilter(
      List((party, Filters(Some(InclusiveFilters(Seq(toApiIdentifier(templateId))))))).toMap)
    client.activeContractSetClient
      .getActiveContracts(filter, verbose = true)
      .runWith(Sink.seq)
      .map(
        acsResponse =>
          acsResponse
            .flatMap(_.activeContracts)
            .map(createdEv =>
              (
                ValueContractId(AbsoluteContractId.assertFromString(createdEv.contractId)),
                ValueValidator.validateRecord(createdEv.getCreateArguments).right.get)))
  }
  def createProposal(
      client: LedgerClient,
      tplId: Identifier,
      proposer: String,
      accepter: String,
      note: String)(implicit ec: ExecutionContext): Future[ContractId] = {
    val argument = ValueRecord(
      Some(tplId),
      ImmArray(
        (None, ValueParty(Party.assertFromString(proposer))),
        (None, ValueParty(Party.assertFromString(accepter))),
        (None, ValueText(note))))
    val commands = Commands(
      party = proposer,
      commands = List(
        Command().withCreate(
          CreateCommand(
            Some(toApiIdentifier(tplId)),
            Some(lfValueToApiRecord(true, argument).right.get)))),
      ledgerId = client.ledgerId.unwrap,
      applicationId = appId,
      commandId = UUID.randomUUID.toString
    )
    val request = SubmitAndWaitRequest(Some(commands))
    for {
      transaction <- client.commandServiceClient
        .submitAndWaitForTransaction(request)
        .map(_.getTransaction)
    } yield {
      assert(transaction.events.size == 1)
      AbsoluteContractId.assertFromString(transaction.events.head.getCreated.contractId)
    }
  }
  def acceptProposal(client: LedgerClient, tplId: Identifier, accepter: String, cid: ContractId)(
      implicit ec: ExecutionContext): Future[ContractId] = {
    val argument = ValueRecord(None, ImmArray.empty)
    val commands = Commands(
      party = accepter,
      commands = List(
        Command().withExercise(
          ExerciseCommand(
            Some(toApiIdentifier(tplId)),
            cid.asInstanceOf[AbsoluteContractId].coid,
            "Accept",
            Some(lfValueToApiValue(true, argument).right.get)))),
      ledgerId = client.ledgerId.unwrap,
      applicationId = appId,
      commandId = UUID.randomUUID.toString
    )
    val request = SubmitAndWaitRequest(Some(commands))
    for {
      tree <- client.commandServiceClient
        .submitAndWaitForTransactionTree(request)
        .map(_.getTransaction)
    } yield {
      assert(tree.rootEventIds.size == 1)
      AbsoluteContractId.assertFromString(
        tree.eventsById(tree.rootEventIds.head).getExercised.getExerciseResult.getContractId)
    }
  }
  def unilateralArchive(client: LedgerClient, tplId: Identifier, proposer: String, cid: ContractId)(
      implicit ec: ExecutionContext): Future[Unit] = {
    val argument = ValueRecord(None, ImmArray.empty)
    val commands = Commands(
      party = proposer,
      commands = List(
        Command().withExercise(
          ExerciseCommand(
            Some(toApiIdentifier(tplId)),
            cid.asInstanceOf[AbsoluteContractId].coid,
            "UnilateralArchive",
            Some(lfValueToApiValue(true, argument).right.get)))),
      ledgerId = client.ledgerId.unwrap,
      applicationId = appId,
      commandId = UUID.randomUUID.toString
    )
    val request = SubmitAndWaitRequest(Some(commands))
    for {
      _ <- client.commandServiceClient.submitAndWaitForTransaction(request)
    } yield {
      ()
    }
  }
  def getTransactions(client: LedgerClient, templates: Seq[Identifier], party: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[TransactionResult]] = {
    val filter = TransactionFilter(
      List((party, Filters(Some(InclusiveFilters(templates.map(toApiIdentifier)))))).toMap)
    for {
      transactions <- client.transactionClient
        .getTransactions(
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
          Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))),
          filter,
          verbose = true
        )
        .runWith(Sink.seq)
    } yield {
      transactions.map(toTransactionResult)
    }
  }
  def main(args: Array[String]): Unit = {
    parser.parse(args, defaultConfig) match {
      case None => sys.exit(1)
      case Some(config) =>
        val encodedDar =
          DarReader().readArchiveFromFile(config.dar.toFile).get
        val dar = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }
        val clientConfig = LedgerClientConfiguration(
          appId,
          LedgerIdRequirement("", enabled = false),
          CommandClientConfiguration.default,
          None)
        implicit val system: ActorSystem = ActorSystem("migration-step")
        implicit val sequencer: ExecutionSequencerFactory =
          new AkkaExecutionSequencerPool("migration-step")(system)
        implicit val ec: ExecutionContext = system.dispatcher
        val tProposalId = Identifier(dar.main._1, QualifiedName.assertFromString("Model:TProposal"))
        val tId = Identifier(dar.main._1, QualifiedName.assertFromString("Model:T"))
        val f: Future[Result] = for {
          client <- LedgerClient.singleHost(config.host, config.port, clientConfig)
          oldTransactions <- getTransactions(client, Seq(tProposalId, tId), config.proposer)
          oldProposals <- queryACS(client, config.proposer, tProposalId)
          oldAccepted <- queryACS(client, config.proposer, tId)
          proposal0 <- createProposal(
            client,
            tProposalId,
            config.proposer,
            config.accepter,
            config.note)
          proposal1 <- createProposal(
            client,
            tProposalId,
            config.proposer,
            config.accepter,
            config.note)
          t0 <- acceptProposal(client, tProposalId, config.accepter, proposal0)
          t1 <- acceptProposal(client, tProposalId, config.accepter, proposal1)
          _ <- unilateralArchive(client, tId, config.proposer, t0)
          newProposals <- queryACS(client, config.proposer, tProposalId)
          newAccepted <- queryACS(client, config.proposer, tId)
          newTransactions <- getTransactions(client, Seq(tProposalId, tId), config.proposer)
        } yield
          Result(
            oldProposals,
            newProposals,
            oldAccepted,
            newAccepted,
            oldTransactions,
            newTransactions)
        f.onComplete {
          case Success(result) =>
            Files.write(config.outputFile, Seq(result.toJson.prettyPrint).asJava)
            system.terminate()
          case Failure(e) =>
            System.err.println(e)
            system.terminate()
        }
    }
  }
}
