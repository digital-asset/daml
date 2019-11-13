// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script

import scala.annotation.tailrec

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SBuiltin._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.{SValue, SExpr}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue
}

case class Converter(
    )

class ConverterException(message: String) extends RuntimeException(message)

case class AnyTemplate(ty: Identifier, arg: SValue)
case class AnyChoice(name: String, arg: SValue)

object Converter {
  private def toLedgerRecord(v: SValue): Either[String, value.Record] = {
    try {
      lfValueToApiRecord(
        true,
        v.toValue.mapContractId {
          case rcoid: RelativeContractId =>
            throw new ConverterException(s"Unexpected contract id $rcoid")
          case acoid: AbsoluteContractId => acoid
        }
      )
    } catch {
      case ex: ConverterException => Left(ex.getMessage())
    }
  }
  private def toLedgerValue(v: SValue) = {
    try {
      lfValueToApiValue(
        true,
        v.toValue.mapContractId {
          case rcoid: RelativeContractId =>
            throw new ConverterException(s"Unexpected contract id $rcoid")
          case acoid: AbsoluteContractId => acoid
        }
      )
    } catch {
      case ex: ConverterException => Left(ex.getMessage())
    }
  }

  def toAnyTemplate(v: SValue): Either[String, AnyTemplate] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case SAny(TTyCon(ty), templateVal) => Right(AnyTemplate(ty, templateVal))
          case v => Left(s"Expected SAny but got $v")
        }
      }
      case _ => Left(s"Expected AnyTemplate but got $v")
    }
  }

  def toAnyChoice(v: SValue): Either[String, AnyChoice] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case SAny(_, choiceVal @ SRecord(_, _, _)) =>
            Right(AnyChoice(choiceVal.id.qualifiedName.name.toString, choiceVal))
          case _ => Left(s"Expected SAny but got $v")
        }
      }
      case _ => Left(s"Expected AnyChoice but got $v")
    }
  }

  def typeRepToIdentifier(v: SValue): Either[String, value.Identifier] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case STypeRep(TTyCon(ty)) => Right(toApiIdentifier(ty))
          case x => Left(s"Expected STypeRep but got $v")
        }
      }
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  def toCreateCommand(v: SValue): Either[String, Command] =
    v match {
      // template argument, continuation
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          templateArg <- toLedgerRecord(anyTemplate.arg)
        } yield
          Command().withCreate(
            CreateCommand(Some(toApiIdentifier(anyTemplate.ty)), Some(templateArg)))
      }
      case _ => Left(s"Expected Create but got $v")
    }

  def toContractId(v: SValue): Either[String, AbsoluteContractId] =
    v match {
      case SContractId(cid @ AbsoluteContractId(_)) => Right(cid)
      case _ => Left(s"Expected AbsoluteContractId but got $v")
    }

  def toExerciseCommand(v: SValue): Either[String, Command] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          cid <- toContractId(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield
          Command().withExercise(
            ExerciseCommand(Some(tplId), cid.coid, anyChoice.name, Some(choiceArg)))
      }
      case _ => Left(s"Expected Exercise but got $v")
    }

  // Extract the two fields out of the RankN encoding used in the Ap constructor.
  def toApFields(
      compiledPackages: CompiledPackages,
      fun: SValue): Either[String, (SValue, SValue)] = {
    val extractTuple = SEMakeClo(
      Array(),
      2,
      SEApp(
        SEBuiltin(SBTupleCon(Name.Array(Name.assertFromString("a"), Name.assertFromString("b")))),
        Array(SEVar(2), SEVar(1))))
    val machine =
      Speedy.Machine.fromSExpr(SEApp(SEValue(fun), Array(extractTuple)), false, compiledPackages)
    @tailrec
    def iter(): Either[String, (SValue, SValue)] = {
      if (machine.isFinal) {
        machine.toSValue match {
          case STuple(_, values) if values.size == 2 => {
            Right((values.get(0), values.get(1)))
          }
          case v => Left(s"Expected STuple but got $v")
        }
      } else {
        machine.step() match {
          case SResultContinue => iter()
          case res => Left(res.toString())
        }
      }
    }
    iter()
  }

  // Walk over the free applicative and extract the list of commands
  def toCommands(
      compiledPackages: CompiledPackages,
      freeAp: SValue): Either[String, Seq[Command]] = {
    // var commands = Seq[Command]()
    // val pure = Name.assertFromString("PureA")
    // val ap = Name.assertFromString("Ap")
    @tailrec
    def iter(v: SValue, commands: Seq[Command]): Either[String, Seq[Command]] = {
      v match {
        case SVariant(_, "PureA", _) => Right(commands)
        case SVariant(_, "Ap", v) =>
          toApFields(compiledPackages, v) match {
            case Right((SVariant(_, "Create", create), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toCreateCommand(create) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((SVariant(_, "Exercise", exercise), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toExerciseCommand(exercise) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((fb, _)) => Left(s"Expected Create or Exercise but got $fb")
            case Left(err) => Left(err)
          }
        case _ => Left(s"Expected PureA or Ap but got $v")
      }
    }
    iter(freeAp, Seq())
  }

  // Given the free applicative for a submit request and the results of that request, we walk over the free applicative and
  // fill in the values for the continuation.
  def fillCommandResults(
      compiledPackages: CompiledPackages,
      freeAp: SValue,
      eventResults: Seq[TreeEvent]): Either[String, SExpr] =
    freeAp match {
      case SVariant(_, "PureA", v) => Right(SEValue(v))
      case SVariant(_, "Ap", v) => {
        for {
          apFields <- toApFields(compiledPackages, v)
          (fb, apfba) = apFields
          bValue <- fb match {
            case SVariant(_, "Create", v) => {
              val continue = v.asInstanceOf[SRecord].values.get(1)
              val contractIdString = eventResults.head.getCreated.contractId
              val contractId =
                SContractId(AbsoluteContractId(ContractIdString.assertFromString(contractIdString)))
              Right(SEApp(SEValue(continue), Array(SEValue(contractId))))
            }
            case SVariant(_, "Exercise", v) => {
              val continue = v.asInstanceOf[SRecord].values.get(3)
              val apiExerciseResult = eventResults.head.getExercised.getExerciseResult
              val exerciseResult =
                SValue.fromValue(ValueValidator.validateValue(apiExerciseResult).right.get)
              Right(SEApp(SEValue(continue), Array(SEValue(exerciseResult))))
            }
            case _ => Left(s"Expected Create or Exercise but got $fb")
          }
          fValue <- fillCommandResults(compiledPackages, apfba, eventResults.tail)
        } yield SEApp(fValue, Array(bValue))
      }
      case _ => Left(s"Expected PureA or Ap but got $freeAp")
    }

  def fromDar(dar: Dar[(PackageId, Package)]): Converter = {
    Converter()
  }
}
