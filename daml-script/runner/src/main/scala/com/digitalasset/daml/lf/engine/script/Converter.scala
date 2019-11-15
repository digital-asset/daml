// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.digitalasset.daml.lf.data.{FrontStack}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.{ResultDone, ValueTranslator}
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{Util => AstUtil}
import com.digitalasset.daml.lf.speedy.SBuiltin._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.{SValue, SExpr}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId}
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue
}

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
      choiceType: (Identifier, Name) => Either[String, Type],
      translator: ValueTranslator,
      freeAp: SValue,
      eventResults: Seq[TreeEvent]): Either[String, SExpr] =
    freeAp match {
      case SVariant(_, "PureA", v) => Right(SEValue(v))
      case SVariant(_, "Ap", v) => {
        for {
          apFields <- toApFields(compiledPackages, v)
          (fb, apfba) = apFields
          bValue <- fb match {
            // We already validate these records during toCommands so we don’t bother doing proper validation again here.
            case SVariant(_, "Create", v) => {
              val continue = v.asInstanceOf[SRecord].values.get(1)
              val contractIdString = eventResults.head.getCreated.contractId
              for {
                cid <- ContractIdString.fromString(contractIdString)
                contractId = SContractId(AbsoluteContractId(cid))
              } yield SEApp(SEValue(continue), Array(SEValue(contractId)))
            }
            case SVariant(_, "Exercise", v) => {
              val continue = v.asInstanceOf[SRecord].values.get(3)
              val exercised = eventResults.head.getExercised
              val apiExerciseResult = exercised.getExerciseResult
              for {
                tplId <- fromApiIdentifier(exercised.templateId.get)
                choice <- Name.fromString(exercised.choice)
                resultType <- choiceType(tplId, choice)
                validated <- ValueValidator.validateValue(apiExerciseResult).left.map(_.toString)
                translated <- translator.translateValue(resultType, validated) match {
                  case ResultDone(r) => Right(r)
                  case err => Left(s"Failed to translate exercise result: $err")
                }
              } yield SEApp(SEValue(continue), Array(SEValue(translated)))
            }
            case _ => Left(s"Expected Create or Exercise but got $fb")
          }
          fValue <- fillCommandResults(
            compiledPackages,
            choiceType,
            translator,
            apfba,
            eventResults.tail)
        } yield SEApp(fValue, Array(bValue))
      }
      case _ => Left(s"Expected PureA or Ap but got $freeAp")
    }

  def toParty(v: SValue): Either[String, SParty] =
    v match {
      case p @ SParty(_) => Right(p)
      case _ => Left(s"Expected SParty but got $v")
    }

  // Helper to construct a record
  def record(ty: Identifier, fields: (String, SValue)*): SValue = {
    val fieldNames = Name.Array(fields.map({
      case (n, _) => Name.assertFromString(n)
    }): _*)
    val args =
      new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
    SRecord(ty, fieldNames, args)
  }

  def fromApiIdentifier(id: value.Identifier): Either[String, Identifier] =
    for {
      packageId <- PackageId.fromString(id.packageId)
      moduleName <- DottedName.fromString(id.moduleName)
      entityName <- DottedName.fromString(id.entityName)
    } yield Identifier(packageId, QualifiedName(moduleName, entityName))

  // Convert a Created event to an AnyTemplate
  def fromCreated(
      translator: ValueTranslator,
      stdlibPackageId: PackageId,
      created: CreatedEvent): Either[String, SValue] = {
    val anyTemplateTyCon =
      Identifier(stdlibPackageId, QualifiedName.assertFromString("DA.Internal.LF:AnyTemplate"))
    for {
      templateId <- created.templateId match {
        case None => Left(s"Missing field templateId in $created")
        case Some(templateId) => Right(templateId)
      }
      tyCon <- fromApiIdentifier(templateId)
      arg <- ValueValidator.validateRecord(created.getCreateArguments).left.map(_.toString)
      argSValue <- translator.translateValue(TTyCon(tyCon), arg) match {
        case ResultDone(v) => Right(v)
        case err => Left(s"Failure to translate value in create: $err")
      }
    } yield record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), argSValue)))
  }

  // TODO This duplicates the type_ method from com.digitalasset.daml.lf.iface.reader.InterfaceReader
  // since it is not exposed. We should find some place to expose that method or a way to
  // avoid duplicating this logic.
  def toIfaceType(
      a: Ast.Type,
      args: FrontStack[iface.Type] = FrontStack.empty
  ): Either[String, iface.Type] =
    a match {
      case Ast.TVar(x) =>
        if (args.isEmpty)
          Right(iface.TypeVar(x))
        else
          Left("arguments passed to a type parameter")
      case Ast.TTyCon(c) =>
        Right(iface.TypeCon(iface.TypeConName(c), args.toImmArray.toSeq))
      case AstUtil.TNumeric(Ast.TNat(n)) if args.isEmpty =>
        Right(iface.TypeNumeric(n))
      case Ast.TBuiltin(bt) =>
        primitiveType(bt, args.toImmArray.toSeq)
      case Ast.TApp(tyfun, arg) =>
        toIfaceType(arg, FrontStack.empty).flatMap(tArg => toIfaceType(tyfun, tArg +: args))
      case Ast.TForall(_, _) | Ast.TTuple(_) | Ast.TNat(_) =>
        Left(s"unserializable data type: ${a.pretty}")
    }

  def primitiveType(
      a: Ast.BuiltinType,
      args: ImmArraySeq[iface.Type]
  ): Either[String, iface.TypePrim] =
    for {
      ab <- a match {
        case Ast.BTUnit => Right((0, iface.PrimType.Unit))
        case Ast.BTBool => Right((0, iface.PrimType.Bool))
        case Ast.BTInt64 => Right((0, iface.PrimType.Int64))
        case Ast.BTText => Right((0, iface.PrimType.Text))
        case Ast.BTDate => Right((0, iface.PrimType.Date))
        case Ast.BTTimestamp => Right((0, iface.PrimType.Timestamp))
        case Ast.BTParty => Right((0, iface.PrimType.Party))
        case Ast.BTContractId => Right((1, iface.PrimType.ContractId))
        case Ast.BTList => Right((1, iface.PrimType.List))
        case Ast.BTOptional => Right((1, iface.PrimType.Optional))
        case Ast.BTMap => Right((1, iface.PrimType.Map))
        case Ast.BTGenMap =>
          // FIXME https://github.com/digital-asset/daml/issues/2256
          Left(s"Unsupported primitive type: $a")
        case Ast.BTNumeric =>
          Left(s"Unserializable primitive type: $a must be applied to one and only one TNat")
        case Ast.BTUpdate | Ast.BTScenario | Ast.BTArrow | Ast.BTAny | Ast.BTTypeRep =>
          Left(s"Unserializable primitive type: $a")
      }
      (arity, primType) = ab
      typ <- {
        if (args.length != arity)
          Left(s"$a requires $arity arguments, but got ${args.length}")
        else
          Right(iface.TypePrim(primType, args))
      }
    } yield typ
}
