// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import java.io.File
import java.util

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import scalaz.syntax.traverse._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.Dar._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SExpr

case class RunnerConfig(
    darPath: File,
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerParty: String)

object RunnerConfig {
  private val parser = new scopt.OptionParser[RunnerConfig]("trigger-runner") {
    head("trigger-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the trigger")

    opt[String]("trigger-name")
      .required()
      .action((t, c) => c.copy(triggerIdentifier = t))
      .text("Identifier of the trigger that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[String]("ledger-party")
      .required()
      .action((t, c) => c.copy(ledgerParty = t))
      .text("Ledger party")
  }
  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        triggerIdentifier = null,
        ledgerHost = "",
        ledgerPort = 0,
        ledgerParty = ""))
}

// Convert from a Ledger API transaction to an SValue corresponding to a Message from the Daml.Trigger module
case class Converter(fromTransaction: Transaction => SValue)

object Converter {
  // Helper to make constructing an SRecord more convenient
  private def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({ case (n, _) => Name.assertFromString(n) }): _*)
    val args = new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  // Helper to create identifiers pointing to the DAML.Trigger module
  private case class TriggerIds(triggerPackageId: PackageId, triggerModuleName: ModuleName) {
    def getId(n: String): Identifier =
      Identifier(triggerPackageId, QualifiedName(triggerModuleName, DottedName.assertFromString(n)))
  }
  private object TriggerIds {
    def fromDar(dar: Dar[(PackageId, Package)]): TriggerIds = {
      val triggerModuleName = DottedName.assertFromString("Daml.Trigger")
      // We might want to just fix this at compile time at some point
      // once we ship the trigger lib with the SDK.
      val triggerPackageId: PackageId = dar.all
        .find {
          case (pkgId, pkg) => pkg.modules.contains(triggerModuleName)
        }
        .get
        ._1
      TriggerIds(triggerPackageId, triggerModuleName)
    }
  }

  private def fromIdentifier(triggerIds: TriggerIds, id: value.Identifier): SValue = {
    val identifierTy = triggerIds.getId("Identifier")
    record(
      identifierTy,
      ("packageId", SText(id.packageId)),
      ("moduleName", SText(id.moduleName)),
      ("name", SText(id.entityName)))
  }

  private def fromEvent(triggerIds: TriggerIds, ev: Event): SValue = {
    val eventTy = triggerIds.getId("Event")
    val createdTy = triggerIds.getId("Created")
    val archivedTy = triggerIds.getId("Archived")
    ev.event match {
      case Archived(archivedEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("ArchivedEvent"),
          record(
            archivedTy,
            ("eventId", SText(archivedEvent.eventId)),
            ("contractId", SText(archivedEvent.contractId)),
            ("templateId", fromIdentifier(triggerIds, archivedEvent.getTemplateId))
          )
        )
      }
      case Created(createdEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("CreatedEvent"),
          record(
            createdTy,
            ("eventId", SText(createdEvent.eventId)),
            ("contractId", SText(createdEvent.contractId)),
            ("templateId", fromIdentifier(triggerIds, createdEvent.getTemplateId))
          )
        )
      }
      case _ => {
        throw new RuntimeException(s"Expected Archived or Created but got $ev.event")
      }
    }
  }

  private def fromTransaction(triggerIds: TriggerIds, t: Transaction): SValue = {
    val messageTy = triggerIds.getId("Message")
    val transactionTy = triggerIds.getId("Transaction")
    SVariant(
      messageTy,
      Name.assertFromString("MTransaction"),
      record(
        transactionTy,
        ("transactionId", SText(t.transactionId)),
        ("events", SList(FrontStack(t.events.map(ev => fromEvent(triggerIds, ev))))))
    )
  }

  def fromDar(dar: Dar[(PackageId, Package)]): Converter = {
    val triggerIds = TriggerIds.fromDar(dar)
    Converter(fromTransaction(triggerIds, _))
  }
}

case class Runner(triggerSink: Sink[Transaction, Future[SExpr]])

object Runner {
  def fromDar(dar: Dar[(PackageId, Package)], triggerId: Identifier): Runner = {
    val converter = Converter.fromDar(dar)
    val compiledPackages = PureCompiledPackages(dar.all.toMap).right.get
    val compiler = Compiler(compiledPackages.packages)

    val triggerExpr = EVal(triggerId)
    val (tyCon: TypeConName, stateTy) =
      dar.main._2.lookupIdentifier(triggerId.qualifiedName).right.get match {
        case DValue(TApp(TTyCon(tcon), stateTy), _, _, _) => (tcon, stateTy)
        case _ => {
          throw new RuntimeException(s"Identifier does not point to trigger")
        }
      }
    val triggerTy: TypeConApp = TypeConApp(tyCon, ImmArray(stateTy))
    val update = compiler.compile(ERecProj(triggerTy, Name.assertFromString("update"), triggerExpr))
    val initialState =
      compiler.compile(ERecProj(triggerTy, Name.assertFromString("initialState"), triggerExpr))

    val machine = Speedy.Machine.fromSExpr(null, false, compiledPackages)
    val sink = Sink.fold[SExpr, Transaction](initialState)((state, transaction) => {
      val message = converter.fromTransaction(transaction)
      machine.ctrl = Speedy.CtrlExpr(SEApp(update, Array(SEValue(message), state)))
      while (!machine.isFinal) {
        machine.step() match {
          case SResultContinue => ()
          case SResultError(err) => {
            throw new RuntimeException(err)
          }
          case res => {
            throw new RuntimeException(s"Unexpected speed result $res")
          }
        }
      }
      machine.toSValue match {
        case SRecord(_, _, values) => {
          val newState = values.get(0)
          val command = values.get(1)
          println(s"Emitted log message: $command")
          println(s"New state: $newState")
          SEValue(newState)
        }
        case v => {
          throw new RuntimeException(s"Expected Tuple2 but got $v")
        }
      }
    })
    Runner(sink)
  }
}

object RunnerMain {

  def main(args: Array[String]): Unit = {

    RunnerConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => {
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val triggerId: Identifier =
          Identifier(dar.main._1, QualifiedName.assertFromString(config.triggerIdentifier))
        val runner = Runner.fromDar(dar, triggerId)

        val system: ActorSystem = ActorSystem("TriggerRunner")
        val materializer: ActorMaterializer = ActorMaterializer()(system)
        val sequencer = new AkkaExecutionSequencerPool("TriggerRunnerPool")(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val applicationId = ApplicationId("Trigger Runner")
        val clientConfig = LedgerClientConfiguration(
          applicationId = ApplicationId.unwrap(applicationId),
          ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
          commandClient = CommandClientConfiguration.default,
          sslContext = None
        )

        val flow: Future[Unit] = for {
          client <- LedgerClient.singleHost(config.ledgerHost, config.ledgerPort, clientConfig)(
            ec,
            sequencer)
          offset <- client.transactionClient.getLedgerEnd.flatMap(response =>
            response.offset match {
              case None => Future.failed(new RuntimeException("Empty option"))
              case Some(a) => Future.successful(a)
          })
          _ <- client.transactionClient
            .getTransactions(
              offset,
              None,
              TransactionFilter(List((config.ledgerParty, Filters.defaultInstance)).toMap))
            .runWith(runner.triggerSink)(materializer)
        } yield ()

        flow.onComplete(_ => system.terminate())

        Await.result(flow, Duration.Inf)
      }
    }
  }
}
