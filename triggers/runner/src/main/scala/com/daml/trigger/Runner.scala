// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import java.io.File
import java.time.Instant
import java.util

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.std.either._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.api.validation.ValueValidator

import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.api.v1.commands.{Commands, Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue
}
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.Dar._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}
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
case class Converter(
    fromTransaction: Transaction => SValue,
    fromACS: Seq[CreatedEvent] => SValue,
    toCommands: SValue => Either[String, (String, Seq[Command])]
)

// Helper to create identifiers pointing to the DAML.Trigger module
case class TriggerIds(
    triggerPackageId: PackageId,
    triggerModuleName: ModuleName,
    mainPackageId: PackageId) {
  def getId(n: String): Identifier =
    Identifier(triggerPackageId, QualifiedName(triggerModuleName, DottedName.assertFromString(n)))
}

object TriggerIds {
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
    TriggerIds(triggerPackageId, triggerModuleName, dar.main._1)
  }
}

case class AnyContractId(templateId: Identifier, contractId: String)

object Converter {
  // Helper to make constructing an SRecord more convenient
  private def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({ case (n, _) => Name.assertFromString(n) }): _*)
    val args = new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  private def toLedgerRecord(v: SValue) = {
    lfValueToApiRecord(
      true,
      v.toValue.mapContractId {
        case rcoid: RelativeContractId =>
          throw new RuntimeException(s"Unexpected contract id $rcoid")
        case acoid: AbsoluteContractId => acoid
      }
    )
  }
  private def toLedgerValue(v: SValue) = {
    lfValueToApiValue(
      true,
      v.toValue.mapContractId {
        case rcoid: RelativeContractId =>
          throw new RuntimeException(s"Unexpected contract id $rcoid")
        case acoid: AbsoluteContractId => acoid
      }
    )
  }

  private def fromIdentifier(triggerIds: TriggerIds, id: value.Identifier): SValue = {
    val identifierTy = triggerIds.getId("Identifier")
    record(
      identifierTy,
      ("packageId", SText(id.packageId)),
      ("moduleName", SText(id.moduleName)),
      ("name", SText(id.entityName)))
  }

  private def fromAnyContractId(
      triggerIds: TriggerIds,
      templateId: value.Identifier,
      contractId: String): SValue = {
    val contractIdTy = triggerIds.getId("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromIdentifier(triggerIds, templateId)),
      ("contractId", SText(contractId))
    )
  }

  private def fromArchivedEvent(triggerIds: TriggerIds, archived: ArchivedEvent): SValue = {
    val archivedTy = triggerIds.getId("Archived")
    record(
      archivedTy,
      ("eventId", SText(archived.eventId)),
      ("contractId", fromAnyContractId(triggerIds, archived.getTemplateId, archived.contractId))
    )
  }

  private def fromCreatedEvent(triggerIds: TriggerIds, created: CreatedEvent): SValue = {
    val createdTy = triggerIds.getId("Created")
    ValueValidator.validateRecord(created.getCreateArguments) match {
      case Right(createArguments) =>
        SValue.fromValue(createArguments) match {
          case r @ SRecord(_, _, _) =>
            record(
              createdTy,
              ("eventId", SText(created.eventId)),
              (
                "contractId",
                fromAnyContractId(triggerIds, created.getTemplateId, created.contractId)),
              ("argument", SAnyTemplate(r))
            )
          case v => throw new RuntimeException(s"Expected record but got $v")
        }
      case Left(err) => throw err
    }
  }

  private def fromEvent(triggerIds: TriggerIds, ev: Event): SValue = {
    val eventTy = triggerIds.getId("Event")
    ev.event match {
      case Archived(archivedEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("ArchivedEvent"),
          fromArchivedEvent(triggerIds, archivedEvent)
        )
      }
      case Created(createdEvent) => {
        SVariant(
          eventTy,
          Name.assertFromString("CreatedEvent"),
          fromCreatedEvent(triggerIds, createdEvent)
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

  private def toText(v: SValue): Either[String, String] = {
    v match {
      case SText(t) => Right(t)
      case _ => Left(s"Expected Text but got $v")
    }
  }

  private def toIdentifier(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 3)
        for {
          packageId <- toText(vals.get(0)).flatMap(PackageId.fromString)
          moduleName <- toText(vals.get(1)).flatMap(DottedName.fromString)
          entityName <- toText(vals.get(2)).flatMap(DottedName.fromString)
        } yield Identifier(packageId, QualifiedName(moduleName, entityName))
      }
      case _ => Left(s"Expected Identifier but got $v")
    }
  }

  private def extractTemplateId(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(templateId, _, _) => Right(templateId)
      case _ => Left(s"Expected contract value but got $v")
    }
  }

  private def toAnyContractId(v: SValue): Either[String, AnyContractId] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          templateId <- toIdentifier(vals.get(0))
          contractId <- toText(vals.get(1))
        } yield AnyContractId(templateId, contractId)
      }
      case _ => Left(s"Expected AnyContractId but got $v")
    }
  }

  private def extractChoiceName(v: SValue): Either[String, String] = {
    v match {
      case SRecord(ty, _, _) => {
        Right(ty.qualifiedName.name.toString)
      }
      case _ => Left(s"Expected choice value but got $v")
    }
  }

  private def toCreate(triggerIds: TriggerIds, v: SValue): Either[String, CreateCommand] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 1)
        vals.get(0) match {
          case SAnyTemplate(tpl) =>
            for {
              templateId <- extractTemplateId(tpl)
              templateArg <- toLedgerRecord(tpl)
            } yield CreateCommand(Some(toApiIdentifier(templateId)), Some(templateArg))
          case v => Left(s"Expected AnyTemplate but got $v")
        }
      }
      case _ => Left(s"Expected CreateCommand but got $v")
    }
  }

  private def toExercise(triggerIds: TriggerIds, v: SValue): Either[String, ExerciseCommand] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          anyContractId <- toAnyContractId(vals.get(0))
          choiceName <- extractChoiceName(vals.get(1))
          choiceArg <- toLedgerValue(vals.get(1))
        } yield {
          ExerciseCommand(
            Some(toApiIdentifier(anyContractId.templateId)),
            anyContractId.contractId,
            choiceName,
            Some(choiceArg))
        }
      }
      case _ => Left(s"Expected ExerciseCommand but got $v")
    }
  }

  private def toCommand(triggerIds: TriggerIds, v: SValue): Either[String, Command] = {
    v match {
      case SVariant(_, "CreateCommand", createVal) =>
        for {
          create <- toCreate(triggerIds, createVal)
        } yield Command().withCreate(create)
      case SVariant(_, "ExerciseCommand", exerciseVal) =>
        for {
          exercise <- toExercise(triggerIds, exerciseVal)
        } yield Command().withExercise(exercise)
      case _ => Left("Expected CreateCommand or ExerciseCommand but got $v")
    }
  }

  private def toCommands(
      triggerIds: TriggerIds,
      v: SValue): Either[String, (String, Seq[Command])] = {
    v match {
      case SRecord(_, _, vals) => {
        assert(vals.size == 2)
        for {
          commandId <- toText(vals.get(0))
          commands <- vals.get(1) match {
            case SList(cmdValues) => cmdValues.traverseU(v => toCommand(triggerIds, v))
            case _ => Left("Expected List but got ${vals.get(1)}")
          }
        } yield (commandId, commands.toImmArray.toSeq)
      }
      case _ => Left("Expected Commands but got $v")
    }
  }

  private def fromACS(triggerIds: TriggerIds, createdEvents: Seq[CreatedEvent]): SValue = {
    val activeContractsTy = triggerIds.getId("ActiveContracts")
    record(
      activeContractsTy,
      ("activeContracts", SList(FrontStack(createdEvents.map(fromCreatedEvent(triggerIds, _))))))
  }

  def fromDar(dar: Dar[(PackageId, Package)]): Converter = {
    val triggerIds = TriggerIds.fromDar(dar)
    Converter(
      fromTransaction(triggerIds, _),
      fromACS(triggerIds, _),
      toCommands(triggerIds, _)
    )
  }
}

class Runner(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    party: String,
    dar: Dar[(PackageId, Package)],
    submit: SubmitRequest => Unit) {

  private val converter = Converter.fromDar(dar)
  private val triggerIds = TriggerIds.fromDar(dar)
  private val darMap: Map[PackageId, Package] = dar.all.toMap
  private val compiler = Compiler(darMap)
  // We overwrite the definition of toLedgerValue with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  private val definitionMap =
    compiler.compilePackages(darMap.keys) +
      (LfDefRef(
        Identifier(
          triggerIds.triggerPackageId,
          QualifiedName(
            triggerIds.triggerModuleName,
            DottedName.assertFromString("toLedgerValue")))) ->
        SEMakeClo(Array(), 1, SEVar(1)))
  private val compiledPackages = PureCompiledPackages(darMap, definitionMap).right.get

  def getTriggerSink(
      triggerId: Identifier,
      acs: Seq[CreatedEvent]): Sink[Transaction, Future[SExpr]] = {
    val triggerExpr = EVal(triggerId)
    val (tyCon: TypeConName, stateTy) =
      dar.main._2.lookupIdentifier(triggerId.qualifiedName).toOption match {
        case Some(DValue(TApp(TTyCon(tcon), stateTy), _, _, _)) => (tcon, stateTy)
        case _ => {
          throw new RuntimeException(
            s"Identifier ${triggerId.qualifiedName} does not point to trigger")
        }
      }
    val triggerTy: TypeConApp = TypeConApp(tyCon, ImmArray(stateTy))
    val update = compiler.compile(ERecProj(triggerTy, Name.assertFromString("update"), triggerExpr))
    val getInitialState =
      compiler.compile(ERecProj(triggerTy, Name.assertFromString("initialState"), triggerExpr))

    val machine = Speedy.Machine.fromSExpr(null, false, compiledPackages)
    val createdExpr: SExpr = SEValue(converter.fromACS(acs))
    val initialState =
      SEApp(getInitialState, Array(SEValue(SParty(Party.assertFromString(party))), createdExpr))
    machine.ctrl = Speedy.CtrlExpr(initialState)
    while (!machine.isFinal) {
      machine.step() match {
        case SResultContinue => ()
        case SResultError(err) => {
          throw new RuntimeException(err)
        }
        case res => {
          throw new RuntimeException(s"Unexpected speedy result $res")
        }
      }
    }
    val evaluatedInitialState = machine.toSValue
    println(s"Initial state: $evaluatedInitialState")
    Sink.fold[SExpr, Transaction](SEValue(evaluatedInitialState))((state, transaction) => {
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
        case SRecord(recordId, _, values)
            if recordId.qualifiedName ==
              QualifiedName(
                DottedName.assertFromString("DA.Types"),
                DottedName.assertFromString("Tuple3")) => {
          val newState = values.get(0)
          val commandOpt = values.get(1)
          val logMessage = values.get(2) match {
            case SText(t) => t
            case _ =>
              throw new RuntimeException(s"Log message should be text but was ${values.get(2)}")
          }
          println(s"New state: $newState")
          println(s"Emitted log message: ${logMessage}")
          commandOpt match {
            case SOptional(Some(commandsVal)) =>
              converter.toCommands(commandsVal) match {
                case Left(err) => throw new RuntimeException(err)
                case Right((commandId, commands)) => {
                  val commandsArg = Commands(
                    ledgerId = ledgerId.unwrap,
                    applicationId = applicationId.unwrap,
                    commandId = commandId,
                    party = party,
                    ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
                    maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5))),
                    commands = commands
                  )
                  submit(SubmitRequest(commands = Some(commandsArg)))
                }
              }
            case _ => {}
          }
          SEValue(newState)
        }
        case v => {
          throw new RuntimeException(s"Expected Tuple3 but got $v")
        }
      }
    })
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

        val system: ActorSystem = ActorSystem("TriggerRunner")
        implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
        val sequencer = new AkkaExecutionSequencerPool("TriggerRunnerPool")(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val applicationId = ApplicationId("Trigger Runner")
        val clientConfig = LedgerClientConfiguration(
          applicationId = ApplicationId.unwrap(applicationId),
          ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
          commandClient = CommandClientConfiguration.default,
          sslContext = None
        )

        val filter = TransactionFilter(List((config.ledgerParty, Filters.defaultInstance)).toMap)

        val flow: Future[Unit] = for {
          client <- LedgerClient.singleHost(config.ledgerHost, config.ledgerPort, clientConfig)(
            ec,
            sequencer)
          runner <- Future {
            new Runner(client.ledgerId, applicationId, config.ledgerParty, dar, submitRequest => {
              val _ = client.commandClient.submitSingleCommand(submitRequest)
            })
          }
          acsResponses <- client.activeContractSetClient
            .getActiveContracts(filter, verbose = true)
            .runWith(Sink.seq)

          offset <- Future {
            Array(acsResponses: _*).lastOption
              .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
                LedgerOffset().withAbsolute(resp.offset))
          }

          _ <- client.transactionClient
            .getTransactions(
              offset,
              None,
              TransactionFilter(List((config.ledgerParty, Filters.defaultInstance)).toMap),
              verbose = true)
            .runWith(runner.getTriggerSink(triggerId, acsResponses.flatMap(x => x.activeContracts)))
        } yield ()

        flow.onComplete(_ => system.terminate())

        Await.result(flow, Duration.Inf)
      }
    }
  }
}
