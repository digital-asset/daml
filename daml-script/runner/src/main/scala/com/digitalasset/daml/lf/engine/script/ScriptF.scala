// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.time.Clock

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.data.FrontStack
import com.daml.lf.{CompiledPackages, command}
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.data.Ref.{Identifier, Name, PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.script.ledgerinteraction.{ScriptLedgerClient, ScriptTimeMode}
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast.{TemplateChoiceSignature, Type}
import com.daml.lf.speedy.SError.DamlEUserError
import com.daml.lf.speedy.SExpr.{SEApp, SEValue}
import com.daml.lf.speedy.{SExpr, SValue}
import com.daml.lf.speedy.SValue.{
  SAnyException,
  SInt64,
  SList,
  SOptional,
  SParty,
  SRecord,
  SText,
  STimestamp,
  SUnit,
}
import com.daml.lf.speedy.Speedy.Machine
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.script.converter.Converter.JavaList
import scalaz.{Foldable, OneAnd}
import scalaz.syntax.traverse._
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import com.daml.script.converter.Converter.toContractId

import scala.concurrent.{ExecutionContext, Future}

sealed trait ScriptF

object ScriptF {
  final class FailedCmd(val cmd: Cmd, val cause: Throwable)
      extends RuntimeException(
        s"""Command ${cmd.description} failed: ${cause.getMessage}
           |Daml stacktrace:
           |${cmd.stackTrace.pretty()}""".stripMargin,
        cause,
      )

  final case class Catch(act: SValue, handle: SValue) extends ScriptF
  final case class Throw(exc: SAnyException) extends ScriptF

  sealed trait Cmd extends ScriptF {
    def stackTrace: StackTrace
    // Human-readable description of the command used in error messages.
    def description: String
    def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr]
  }
  // The environment that the `execute` function gets access to.
  final class Env(
      val scriptIds: ScriptIds,
      val timeMode: ScriptTimeMode,
      private var _clients: Participants[ScriptLedgerClient],
      machine: Machine,
  ) {
    def clients = _clients
    val valueTranslator = new ValueTranslator(machine.compiledPackages)
    val utcClock = Clock.systemUTC()
    // Copy the tracelog from the client to the off-ledger machine.
    def copyTracelog(client: ScriptLedgerClient) = {
      for ((msg, optLoc) <- client.tracelogIterator) {
        machine.traceLog.add(msg, optLoc)
      }
      client.clearTracelog
    }
    def addPartyParticipantMapping(party: Party, participant: Participant) = {
      _clients =
        _clients.copy(party_participants = _clients.party_participants + (party -> participant))
    }
    def compiledPackages = machine.compiledPackages
    def lookupChoice(id: Identifier, choice: Name): Either[String, TemplateChoiceSignature] =
      for {
        pkg <- compiledPackages
          .getSignature(id.packageId)
          .toRight(s"Failed to find package ${id.packageId}")
        module <- pkg.modules
          .get(id.qualifiedName.module)
          .toRight(s"Failed to find module ${id.qualifiedName.module}")
        tpl <- module.templates
          .get(id.qualifiedName.name)
          .toRight(s"Failed to find template ${id.qualifiedName.name}")
        choice <- tpl.choices
          .get(choice)
          .toRight(s"Failed to find choice $choice in $id")
      } yield choice

    def lookupKeyTy(id: Identifier): Either[String, Type] =
      for {
        pkg <- compiledPackages
          .getSignature(id.packageId)
          .toRight(s"Failed to find package ${id.packageId}")
        module <- pkg.modules
          .get(id.qualifiedName.module)
          .toRight(s"Failed to find module ${id.qualifiedName.module}")
        tpl <- module.templates
          .get(id.qualifiedName.name)
          .toRight(s"Failed to find template ${id.qualifiedName.name}")
        key <- tpl.key.toRight(s"Template ${id} does not have a contract key")
      } yield key.typ
    def translateValue(ty: Type, value: Value[ContractId]): Either[String, SValue] =
      valueTranslator.translateValue(ty, value).left.map(_.toString)

  }
  final case class Submit(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace

    override def description = "submit"

    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submit(
          data.actAs,
          data.readAs,
          data.cmds,
          data.stackTrace.topFrame,
        )
        _ = env.copyTracelog(client)
        v <- submitRes match {
          case Right(results) =>
            Converter.toFuture(
              Converter
                .fillCommandResults(
                  env.compiledPackages,
                  env.lookupChoice,
                  env.valueTranslator,
                  data.freeAp,
                  results,
                )
            )
          case Left(statusEx) =>
            // This branch is superseded by SubmitMustFail below,
            // however, it is maintained for backwards
            // compatibility with DAML script DARs generated by
            // older SDK versions that didn't distinguish Submit
            // and SubmitMustFail.
            data.continue match {
              // Separated submit and submitMustFail, fail in Scala land
              // instead of going back to Daml.
              case SUnit => Future.failed(statusEx)
              // Fail in Daml land
              case _ =>
                for {
                  res <- Converter.toFuture(
                    Converter
                      .fromStatusException(env.scriptIds, statusEx)
                  )
                } yield SEApp(SEValue(data.continue), Array(SEValue(res)))
            }
        }
      } yield v
  }

  final case class SubmitMustFail(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace

    override def description = "submitMustFail"

    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submitMustFail(
          data.actAs,
          data.readAs,
          data.cmds,
          data.stackTrace.topFrame,
        )
        _ = env.copyTracelog(client)
        v <- submitRes match {
          case Right(()) =>
            Future.successful(SEApp(SEValue(data.continue), Array(SEValue(SUnit))))
          case Left(()) =>
            Future.failed(
              new DamlEUserError("Expected submit to fail but it succeeded")
            )
        }
      } yield v

  }
  final case class SubmitTree(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace
    override def description = "submitTree"
    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(data.actAs)
        )
        submitRes <- client.submitTree(
          data.actAs,
          data.readAs,
          data.cmds,
          data.stackTrace.topFrame,
        )
        res <- Converter.toFuture(
          Converter.translateTransactionTree(
            env.lookupChoice,
            env.valueTranslator,
            env.scriptIds,
            submitRes,
          )
        )
        _ = env.copyTracelog(client)
      } yield SEApp(SEValue(data.continue), Array(SEValue(res)))
  }
  final case class Query(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {

    override def description = "query"

    override def execute(
        env: Env
    )(implicit ec: ExecutionContext, mat: Materializer, esf: ExecutionSequencerFactory) =
      for {
        client <- Converter.toFuture(
          env.clients
            .getPartiesParticipant(parties)
        )
        acs <- client.query(parties, tplId)
        res <- Converter.toFuture(
          FrontStack(acs)
            .traverse(
              Converter
                .fromCreated(env.valueTranslator, _)
            )
        )
      } yield SEApp(SEValue(continue), Array(SEValue(SList(res))))

  }
  final case class QueryContractId(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      cid: ContractId,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "queryContractId"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartyParticipant(parties.head))
        optR <- client.queryContractId(parties, tplId, cid)
        optR <- Converter.toFuture(
          optR.traverse(Converter.fromContract(env.valueTranslator, _))
        )
      } yield SEApp(SEValue(continue), Array(SEValue(SOptional(optR))))
  }
  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "queryContractKey"

    private def translateKey(
        env: Env
    )(id: Identifier, v: Value[ContractId]): Either[String, SValue] =
      for {
        keyTy <- env.lookupKeyTy(id)
        translated <- env.valueTranslator.translateValue(keyTy, v).left.map(_.msg)
      } yield translated

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- Converter.toFuture(env.clients.getPartiesParticipant(parties))
        optR <- client.queryContractKey(parties, tplId, key.key, translateKey(env))
        optR <- Converter.toFuture(
          optR.traverse(Converter.fromCreated(env.valueTranslator, _))
        )
      } yield SEApp(SEValue(continue), Array(SEValue(SOptional(optR))))
  }
  final case class AllocParty(
      displayName: String,
      idHint: String,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "allocateParty"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- env.clients.getParticipant(participant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        party <- client.allocateParty(idHint, displayName)

      } yield {
        participant.foreach(env.addPartyParticipantMapping(party, _))
        SEApp(SEValue(continue), Array(SEValue(SParty(party))))
      }

  }
  final case class ListKnownParties(
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd {
    override def description = "listKnownParties"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        client <- env.clients.getParticipant(participant) match {
          case Right(client) => Future.successful(client)
          case Left(err) => Future.failed(new RuntimeException(err))
        }
        partyDetails <- client.listKnownParties()
        partyDetails_ <- Converter.toFuture(
          partyDetails
            .traverse(details => Converter.fromPartyDetails(env.scriptIds, details))
        )
      } yield SEApp(SEValue(continue), Array(SEValue(SList(FrontStack(partyDetails_)))))

  }
  final case class GetTime(stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "getTime"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      for {
        time <- env.timeMode match {
          case ScriptTimeMode.Static => {
            // We don’t parametrize this by participant since this
            // is only useful in static time mode and using the time
            // service with multiple participants is very dodgy.
            for {
              client <- Converter.toFuture(env.clients.getParticipant(None))
              t <- client.getStaticTime()
            } yield t
          }
          case ScriptTimeMode.WallClock =>
            Future {
              Timestamp.assertFromInstant(env.utcClock.instant())
            }
        }
      } yield SEApp(SEValue(continue), Array(SEValue(STimestamp(time))))

  }
  final case class SetTime(time: Timestamp, stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "setTime"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] =
      env.timeMode match {
        case ScriptTimeMode.Static =>
          for {
            // We don’t parametrize this by participant since this
            // is only useful in static time mode and using the time
            // service with multiple participants is very dodgy.
            client <- Converter.toFuture(env.clients.getParticipant(None))
            _ <- client.setStaticTime(time)
          } yield SEApp(SEValue(continue), Array(SEValue(SUnit)))
        case ScriptTimeMode.WallClock =>
          Future.failed(
            new RuntimeException("setTime is not supported in wallclock mode")
          )

      }
  }

  final case class Sleep(micros: Long, stackTrace: StackTrace, continue: SValue) extends Cmd {
    override def description = "sleep"

    override def execute(env: Env)(implicit
        ec: ExecutionContext,
        mat: Materializer,
        esf: ExecutionSequencerFactory,
    ): Future[SExpr] = Future {
      val sleepMillis = micros / 1000
      val sleepNanos = (micros % 1000) * 1000
      Thread.sleep(sleepMillis, sleepNanos.toInt)
      SEApp(SEValue(continue), Array(SEValue(SUnit)))
    }
  }

  // Shared between Submit, SubmitMustFail and SubmitTree
  final case class SubmitData(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmds: List[command.ApiCommand],
      freeAp: SValue,
      stackTrace: StackTrace,
      continue: SValue,
  )

  final case class Ctx(knownPackages: Map[String, PackageId], compiledPackages: CompiledPackages)

  private def toStackTrace(ctx: Ctx, stackTrace: Option[SValue]): Either[String, StackTrace] =
    stackTrace match {
      case None => Right(StackTrace.empty)
      case Some(stackTrace) => Converter.toStackTrace(ctx.knownPackages, stackTrace)
    }

  private def parseSubmit(ctx: Ctx, v: SValue): Either[String, SubmitData] = {
    def convert(
        actAs: OneAnd[List, SValue],
        readAs: List[SValue],
        freeAp: SValue,
        continue: SValue,
        stackTrace: Option[SValue],
    ) =
      for {
        actAs <- actAs.traverse(Converter.toParty(_)).map(toOneAndSet(_))
        readAs <- readAs.traverse(Converter.toParty(_))
        cmds <- Converter.toCommands(ctx.compiledPackages, freeAp)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield SubmitData(actAs, readAs.toSet, cmds, freeAp, stackTrace, continue)
    v match {
      // no location
      case SRecord(_, _, JavaList(sParty, SRecord(_, _, JavaList(freeAp)), continue)) =>
        convert(OneAnd(sParty, List()), List(), freeAp, continue, None)
      // location
      case SRecord(_, _, JavaList(sParty, SRecord(_, _, JavaList(freeAp)), continue, loc)) =>
        convert(OneAnd(sParty, List()), List(), freeAp, continue, Some(loc))
      // multi-party actAs/readAs + location
      case SRecord(
            _,
            _,
            JavaList(
              SRecord(_, _, JavaList(hdAct, SList(tlAct))),
              SList(read),
              SRecord(_, _, JavaList(freeAp)),
              continue,
              loc,
            ),
          ) =>
        convert(OneAnd(hdAct, tlAct.toList), read.toList, freeAp, continue, Some(loc))
      case _ => Left(s"Expected Submit payload but got $v")
    }
  }

  private def parseQuery(ctx: Ctx, v: SValue): Either[String, Query] = {
    def convert(readAs: SValue, tplId: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        readAs <- Converter.toParties(readAs)
        tplId <- Converter
          .typeRepToIdentifier(tplId)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield Query(readAs, tplId, stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, continue)) =>
        convert(actAs, tplId, None, continue)
      case SRecord(_, _, JavaList(actAs, tplId, continue, stackTrace)) =>
        convert(actAs, tplId, Some(stackTrace), continue)
      case _ => Left(s"Expected Query payload but got $v")
    }
  }

  private def parseQueryContractId(ctx: Ctx, v: SValue): Either[String, QueryContractId] = {
    def convert(
        actAs: SValue,
        tplId: SValue,
        cid: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        cid <- toContractId(cid)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryContractId(actAs, tplId, cid, stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, cid, continue)) =>
        convert(actAs, tplId, cid, None, continue)
      case SRecord(_, _, JavaList(actAs, tplId, cid, continue, stackTrace)) =>
        convert(actAs, tplId, cid, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryContractId payload but got $v")
    }
  }

  private def parseQueryContractKey(ctx: Ctx, v: SValue): Either[String, QueryContractKey] = {
    def convert(
        actAs: SValue,
        tplId: SValue,
        key: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        key <- Converter.toAnyContractKey(key)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield QueryContractKey(actAs, tplId, key, stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, key, continue)) =>
        convert(actAs, tplId, key, None, continue)
      case SRecord(_, _, JavaList(actAs, tplId, key, continue, stackTrace)) =>
        convert(actAs, tplId, key, Some(stackTrace), continue)
      case _ => Left(s"Expected QueryContractKey payload but got $v")
    }
  }

  private def parseAllocParty(ctx: Ctx, v: SValue): Either[String, AllocParty] = {
    def convert(
        displayName: String,
        idHint: String,
        participantName: SValue,
        stackTrace: Option[SValue],
        continue: SValue,
    ) =
      for {
        participantName <- Converter.toParticipantName(participantName)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield AllocParty(displayName, idHint, participantName, stackTrace, continue)
    v match {
      case SRecord(
            _,
            _,
            JavaList(
              SText(displayName),
              SText(idHint),
              participantName,
              continue,
            ),
          ) =>
        convert(displayName, idHint, participantName, None, continue)
      case SRecord(
            _,
            _,
            JavaList(
              SText(displayName),
              SText(idHint),
              participantName,
              continue,
              stackTrace,
            ),
          ) =>
        convert(displayName, idHint, participantName, Some(stackTrace), continue)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }
  }

  private def parseListKnownParties(ctx: Ctx, v: SValue): Either[String, ListKnownParties] = {
    def convert(participantName: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        participantName <- Converter.toParticipantName(participantName)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield ListKnownParties(participantName, stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(participantName, continue)) =>
        convert(participantName, None, continue)
      case SRecord(_, _, JavaList(participantName, continue, stackTrace)) =>
        convert(participantName, Some(stackTrace), continue)
      case _ => Left(s"Expected ListKnownParties payload but got $v")
    }
  }

  private def parseGetTime(ctx: Ctx, v: SValue): Either[String, GetTime] = {
    def convert(stackTrace: Option[SValue], continue: SValue) =
      for {
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield GetTime(stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(continue, stackTrace)) => convert(Some(stackTrace), continue)
      case _ => convert(None, v)
    }
  }

  private def parseSetTime(ctx: Ctx, v: SValue): Either[String, SetTime] = {
    def convert(time: SValue, stackTrace: Option[SValue], continue: SValue) =
      for {
        time <- Converter.toTimestamp(time)
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield SetTime(time, stackTrace, continue)
    v match {
      case SRecord(_, _, JavaList(time, continue)) => convert(time, None, continue)
      case SRecord(_, _, JavaList(time, continue, stackTrace)) =>
        convert(time, Some(stackTrace), continue)
      case _ => Left(s"Expected SetTime payload but got $v")
    }
  }

  private def parseSleep(ctx: Ctx, v: SValue): Either[String, Sleep] = {
    def convert(micros: Long, stackTrace: Option[SValue], continue: SValue) = {
      for {
        stackTrace <- toStackTrace(ctx, stackTrace)
      } yield Sleep(micros, stackTrace, continue)
    }

    v match {
      case SRecord(_, _, JavaList(SRecord(_, _, JavaList(SInt64(micros))), continue)) =>
        convert(micros, None, continue)
      case SRecord(_, _, JavaList(SRecord(_, _, JavaList(SInt64(micros))), continue, stackTrace)) =>
        convert(micros, Some(stackTrace), continue)
      case _ => Left(s"Expected Sleep payload but got $v")
    }

  }

  private def parseCatch(v: SValue): Either[String, Catch] = {
    v match {
      case SRecord(_, _, JavaList(act, handle)) =>
        Right(Catch(act, handle))
      case _ => Left(s"Expected Catch payload but got $v")
    }

  }

  private def parseThrow(v: SValue): Either[String, Throw] = {
    v match {
      case SRecord(_, _, JavaList(exc: SAnyException)) =>
        Right(Throw(exc))
      case _ => Left(s"Expected Throw payload but got $v")
    }

  }

  def parse(ctx: Ctx, constr: Ast.VariantConName, v: SValue): Either[String, ScriptF] =
    constr match {
      case "Submit" => parseSubmit(ctx, v).map(Submit(_))
      case "SubmitMustFail" => parseSubmit(ctx, v).map(SubmitMustFail(_))
      case "SubmitTree" => parseSubmit(ctx, v).map(SubmitTree(_))
      case "Query" => parseQuery(ctx, v)
      case "QueryContractId" => parseQueryContractId(ctx, v)
      case "QueryContractKey" => parseQueryContractKey(ctx, v)
      case "AllocParty" => parseAllocParty(ctx, v)
      case "ListKnownParties" => parseListKnownParties(ctx, v)
      case "GetTime" => parseGetTime(ctx, v)
      case "SetTime" => parseSetTime(ctx, v)
      case "Sleep" => parseSleep(ctx, v)
      case "Catch" => parseCatch(v)
      case "Throw" => parseThrow(v)
      case _ => Left(s"Unknown constructor $constr")
    }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
