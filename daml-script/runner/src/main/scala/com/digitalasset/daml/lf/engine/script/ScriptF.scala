// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.lf.command
import com.daml.lf.CompiledPackages
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue.{SInt64, SList, SRecord, SText}
import com.daml.lf.value.Value.ContractId
import com.daml.script.converter.Converter.JavaList
import scalaz.{Foldable, OneAnd}
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.either._
import com.daml.script.converter.Converter.toContractId

object ScriptF {
  sealed trait Cmd {
    def stackTrace: StackTrace
  }
  final case class Submit(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace
  }
  final case class SubmitMustFail(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace
  }
  final case class SubmitTree(data: SubmitData) extends Cmd {
    override def stackTrace = data.stackTrace
  }
  final case class Query(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd
  final case class QueryContractId(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      cid: ContractId,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd
  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd
  final case class AllocParty(
      displayName: String,
      idHint: String,
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd
  final case class ListKnownParties(
      participant: Option[Participant],
      stackTrace: StackTrace,
      continue: SValue,
  ) extends Cmd
  final case class GetTime(stackTrace: StackTrace, continue: SValue) extends Cmd
  final case class SetTime(time: Timestamp, stackTrace: StackTrace, continue: SValue) extends Cmd
  final case class Sleep(micros: Long, stackTrace: StackTrace, continue: SValue) extends Cmd

  // Shared between Submit, SubmitMustFail and SubmitTree
  final case class SubmitData(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmds: List[command.Command],
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

  def parse(ctx: Ctx, constr: Ast.VariantConName, v: SValue): Either[String, Cmd] = constr match {
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
    case _ => Left(s"Unknown contructor $constr")
  }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
