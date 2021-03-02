// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.lf.command
import com.daml.lf.CompiledPackages
import com.daml.lf.data.Ref.{Identifier, Location, PackageId, Party}
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
  sealed trait Cmd
  final case class Submit(data: SubmitData) extends Cmd
  final case class SubmitMustFail(data: SubmitData) extends Cmd
  final case class SubmitTree(data: SubmitData) extends Cmd
  final case class Query(parties: OneAnd[Set, Party], tplId: Identifier, continue: SValue)
      extends Cmd
  final case class QueryContractId(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      cid: ContractId,
      continue: SValue,
  ) extends Cmd
  final case class QueryContractKey(
      parties: OneAnd[Set, Party],
      tplId: Identifier,
      key: AnyContractKey,
      continue: SValue,
  ) extends Cmd
  final case class AllocParty(
      displayName: String,
      idHint: String,
      participant: Option[Participant],
      continue: SValue,
  ) extends Cmd
  final case class ListKnownParties(participant: Option[Participant], continue: SValue) extends Cmd
  final case class GetTime(continue: SValue) extends Cmd
  final case class SetTime(time: Timestamp, continue: SValue) extends Cmd
  final case class Sleep(micros: Long, continue: SValue) extends Cmd

  // Shared between Submit, SubmitMustFail and SubmitTree
  final case class SubmitData(
      actAs: OneAnd[Set, Party],
      readAs: Set[Party],
      cmds: List[command.Command],
      freeAp: SValue,
      commitLocation: Option[Location],
      continue: SValue,
  )

  final case class Ctx(knownPackages: Map[String, PackageId], compiledPackages: CompiledPackages)

  def parseSubmit(ctx: Ctx, v: SValue): Either[String, SubmitData] = {
    def convert(
        actAs: OneAnd[List, SValue],
        readAs: List[SValue],
        freeAp: SValue,
        continue: SValue,
        loc: Option[SValue],
    ) =
      for {
        actAs <- actAs.traverse(Converter.toParty(_)).map(toOneAndSet(_))
        readAs <- readAs.traverse(Converter.toParty(_))
        cmds <- Converter.toCommands(ctx.compiledPackages, freeAp)
        commitLocation <- loc.fold(Right(None): Either[String, Option[Location]])(sLoc =>
          Converter.toOptionLocation(ctx.knownPackages, sLoc)
        )
      } yield SubmitData(actAs, readAs.toSet, cmds, freeAp, commitLocation, continue)
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

  def parseQuery(v: SValue): Either[String, Query] = {
    def convert(readAs: SValue, tplId: SValue, continue: SValue) =
      for {
        readAs <- Converter.toParties(readAs)
        tplId <- Converter
          .typeRepToIdentifier(tplId)
      } yield Query(readAs, tplId, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, continue)) =>
        convert(actAs, tplId, continue)
      case _ => Left(s"Expected Query payload but got $v")
    }
  }

  def parseQueryContractId(v: SValue): Either[String, QueryContractId] = {
    def convert(actAs: SValue, tplId: SValue, cid: SValue, continue: SValue) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        cid <- toContractId(cid)
      } yield QueryContractId(actAs, tplId, cid, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, cid, continue)) =>
        convert(actAs, tplId, cid, continue)
      case _ => Left(s"Expected QueryContractId payload but got $v")
    }
  }

  def parseQueryContractKey(v: SValue): Either[String, QueryContractKey] = {
    def convert(actAs: SValue, tplId: SValue, key: SValue, continue: SValue) =
      for {
        actAs <- Converter.toParties(actAs)
        tplId <- Converter.typeRepToIdentifier(tplId)
        key <- Converter.toAnyContractKey(key)
      } yield QueryContractKey(actAs, tplId, key, continue)
    v match {
      case SRecord(_, _, JavaList(actAs, tplId, key, continue)) =>
        convert(actAs, tplId, key, continue)
      case _ => Left(s"Expected QueryContractKey payload but got $v")
    }
  }

  def parseAllocParty(v: SValue): Either[String, AllocParty] = {
    def convert(displayName: String, idHint: String, participantName: SValue, continue: SValue) =
      for {
        participantName <- Converter.toParticipantName(participantName)
      } yield AllocParty(displayName, idHint, participantName, continue)
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
        convert(displayName, idHint, participantName, continue)
      case _ => Left(s"Expected AllocParty payload but got $v")
    }
  }

  def parseListKnownParties(v: SValue): Either[String, ListKnownParties] = {
    def convert(participantName: SValue, continue: SValue) =
      for {
        participantName <- Converter.toParticipantName(participantName)
      } yield ListKnownParties(participantName, continue)
    v match {
      case SRecord(_, _, JavaList(participantName, continue)) => convert(participantName, continue)
      case _ => Left(s"Expected ListKnownParties payload but got $v")
    }
  }

  def parseGetTime(v: SValue): Either[String, GetTime] = {
    Right(GetTime(v))
  }

  def parseSetTime(v: SValue): Either[String, SetTime] = {
    def convert(time: SValue, continue: SValue) =
      for {
        time <- Converter.toTimestamp(time)
      } yield SetTime(time, continue)
    v match {
      case SRecord(_, _, JavaList(time, continue)) => convert(time, continue)
      case _ => Left(s"Expected SetTime payload but got $v")
    }
  }

  def parseSleep(v: SValue): Either[String, Sleep] = {
    def convert(micros: Long, continue: SValue) =
      Right(Sleep(micros, continue))
    v match {
      case SRecord(_, _, JavaList(SRecord(_, _, JavaList(SInt64(micros))), continue)) =>
        convert(micros, continue)
      case _ => Left(s"Expected Sleep payload but got $v")
    }

  }

  def parse(ctx: Ctx, constr: Ast.VariantConName, v: SValue): Either[String, Cmd] = constr match {
    case "Submit" => parseSubmit(ctx, v).map(Submit(_))
    case "SubmitMustFail" => parseSubmit(ctx, v).map(SubmitMustFail(_))
    case "SubmitTree" => parseSubmit(ctx, v).map(SubmitTree(_))
    case "Query" => parseQuery(v)
    case "QueryContractId" => parseQueryContractId(v)
    case "QueryContractKey" => parseQueryContractKey(v)
    case "AllocParty" => parseAllocParty(v)
    case "ListKnownParties" => parseListKnownParties(v)
    case "GetTime" => parseGetTime(v)
    case "SetTime" => parseSetTime(v)
    case "Sleep" => parseSleep(v)
    case _ => Left(s"Unknown contructor $constr")
  }

  private def toOneAndSet[F[_], A](x: OneAnd[F, A])(implicit fF: Foldable[F]): OneAnd[Set, A] =
    OneAnd(x.head, x.tail.toSet - x.head)
}
