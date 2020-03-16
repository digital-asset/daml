// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import io.grpc.StatusRuntimeException
import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scalaz.{\/-, -\/}

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.{ResultDone, ValueTranslator}
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SBuiltin._
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.{SValue, SExpr}
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.ledger.api.v1.commands.{
  Command,
  CreateCommand,
  ExerciseCommand,
  ExerciseByKeyCommand,
  CreateAndExerciseCommand,
}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.value
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.platform.participant.util.LfEngineToApi.{
  toApiIdentifier,
  lfValueToApiRecord,
  lfValueToApiValue
}
import com.digitalasset.daml.lf.speedy.Pretty

// Helper to create identifiers pointing to the DAML.Script module
case class ScriptIds(val scriptPackageId: PackageId) {
  def damlScript(s: String) =
    Identifier(
      scriptPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Script"), DottedName.assertFromString(s)))
}

class ConverterException(message: String) extends RuntimeException(message)

case class AnyTemplate(ty: Identifier, arg: SValue)
case class AnyChoice(name: String, arg: SValue)
case class AnyContractKey(key: SValue)

object Converter {
  private val DA_TYPES_PKGID =
    PackageId.assertFromString("40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7")
  private def daTypes(s: String): Identifier =
    Identifier(
      DA_TYPES_PKGID,
      QualifiedName(DottedName.assertFromString("DA.Types"), DottedName.assertFromString(s)))

  private val DA_INTERNAL_ANY_PKGID =
    PackageId.assertFromString("cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7")
  private def daInternalAny(s: String): Identifier =
    Identifier(
      DA_INTERNAL_ANY_PKGID,
      QualifiedName(DottedName.assertFromString("DA.Internal.Any"), DottedName.assertFromString(s)))

  private def toLedgerRecord(v: SValue): Either[String, value.Record] =
    for {
      value <- v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")
      apiRecord <- lfValueToApiRecord(true, value)
    } yield apiRecord

  private def toLedgerValue(v: SValue): Either[String, value.Value] =
    for {
      value <- v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")
      apiValue <- lfValueToApiValue(true, value)
    } yield apiValue

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
      case SRecord(_, _, vals) if vals.size == 2 => {
        vals.get(0) match {
          case SAny(_, choiceVal @ SRecord(_, _, _)) =>
            Right(AnyChoice(choiceVal.id.qualifiedName.name.toString, choiceVal))
          case _ => Left(s"Expected SAny but got $v")
        }
      }
      case _ => Left(s"Expected AnyChoice but got $v")
    }
  }

  def toAnyContractKey(v: SValue): Either[String, AnyContractKey] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 2 => {
        vals.get(0) match {
          case SAny(_, key) => Right(AnyContractKey(key))
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

  def toExerciseByKeyCommand(v: SValue): Either[String, Command] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          anyKey <- toAnyContractKey(vals.get(1))
          keyArg <- toLedgerValue(anyKey.key)
          anyChoice <- toAnyChoice(vals.get(2))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield
          Command().withExerciseByKey(
            ExerciseByKeyCommand(Some(tplId), Some(keyArg), anyChoice.name, Some(choiceArg)))
      }
      case _ => Left(s"Expected ExerciseByKey but got $v")
    }

  def toCreateAndExerciseCommand(v: SValue): Either[String, Command] =
    v match {
      case SRecord(_, _, vals) if vals.size == 3 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          templateArg <- toLedgerRecord(anyTemplate.arg)
          anyChoice <- toAnyChoice(vals.get(1))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield
          Command().withCreateAndExercise(
            CreateAndExerciseCommand(
              Some(toApiIdentifier(anyTemplate.ty)),
              Some(templateArg),
              anyChoice.name,
              Some(choiceArg)))
      }
      case _ => Left(s"Expected CreateAndExercise but got $v")
    }

  // Extract the two fields out of the RankN encoding used in the Ap constructor.
  def toApFields(
      compiledPackages: CompiledPackages,
      fun: SValue): Either[String, (SValue, SValue)] = {
    val extractStruct = SEMakeClo(
      Array(),
      2,
      SEApp(
        SEBuiltin(SBStructCon(Name.Array(Name.assertFromString("a"), Name.assertFromString("b")))),
        Array(SEVar(2), SEVar(1))))
    val machine =
      Speedy.Machine.fromSExpr(SEApp(SEValue(fun), Array(extractStruct)), false, compiledPackages)
    @tailrec
    def iter(): Either[String, (SValue, SValue)] = {
      if (machine.isFinal) {
        machine.toSValue match {
          case SStruct(_, values) if values.size == 2 => {
            Right((values.get(0), values.get(1)))
          }
          case v => Left(s"Expected SStruct but got $v")
        }
      } else {
        machine.step() match {
          case SResultContinue => iter()
          case SResultError(err) => Left(Pretty.prettyError(err, machine.ptx).render(80))
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
        case SVariant(_, "PureA", _, _) => Right(commands)
        case SVariant(_, "Ap", _, v) =>
          toApFields(compiledPackages, v) match {
            case Right((SVariant(_, "Create", _, create), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toCreateCommand(create) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((SVariant(_, "Exercise", _, exercise), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toExerciseCommand(exercise) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((SVariant(_, "ExerciseByKey", _, exerciseByKey), v)) =>
              toExerciseByKeyCommand(exerciseByKey) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((SVariant(_, "CreateAndExercise", _, createAndExercise), v)) =>
              toCreateAndExerciseCommand(createAndExercise) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, commands ++ Seq(r))
              }
            case Right((fb, _)) =>
              Left(s"Expected Create, Exercise ExerciseByKey or CreateAndExercise but got $fb")
            case Left(err) => Left(err)
          }
        case _ => Left(s"Expected PureA or Ap but got $v")
      }
    }
    iter(freeAp, Seq())
  }

  def translateExerciseResult(
      choiceType: (Identifier, Name) => Either[String, Type],
      translator: ValueTranslator,
      ev: ExercisedEvent) = {
    val apiExerciseResult = ev.getExerciseResult
    for {
      tplId <- fromApiIdentifier(ev.templateId.get)
      choice <- Name.fromString(ev.choice)
      resultType <- choiceType(tplId, choice)
      validated <- ValueValidator.validateValue(apiExerciseResult).left.map(_.toString)
      translated <- translator.translateValue(resultType, validated) match {
        case ResultDone(r) => Right(r)
        case err => Left(s"Failed to translate exercise result: $err")
      }
    } yield translated
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
      case SVariant(_, "PureA", _, v) => Right(SEValue(v))
      case SVariant(_, "Ap", _, v) => {
        for {
          apFields <- toApFields(compiledPackages, v)
          (fb, apfba) = apFields
          r <- fb match {
            // We already validate these records during toCommands so we don’t bother doing proper validation again here.
            case SVariant(_, "Create", _, v) => {
              val continue = v.asInstanceOf[SRecord].values.get(1)
              val contractIdString = eventResults.head.getCreated.contractId
              for {
                cid <- ContractIdString.fromString(contractIdString)
                contractId = SContractId(AbsoluteContractId(cid))
              } yield (SEApp(SEValue(continue), Array(SEValue(contractId))), eventResults.tail)
            }
            case SVariant(_, "Exercise", _, v) => {
              val continue = v.asInstanceOf[SRecord].values.get(3)
              val exercised = eventResults.head.getExercised
              for {
                translated <- translateExerciseResult(choiceType, translator, exercised)
              } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
            }
            case SVariant(_, "ExerciseByKey", _, v) => {
              val continue = v.asInstanceOf[SRecord].values.get(3)
              val exercised = eventResults.head.getExercised
              for {
                translated <- translateExerciseResult(choiceType, translator, exercised)
              } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
            }
            case SVariant(_, "CreateAndExercise", _, v) => {
              val continue = v.asInstanceOf[SRecord].values.get(2)
              // We get a create and an exercise event here. We only care about the exercise event so we skip the create.
              val exercised = eventResults(1).getExercised
              for {
                translated <- translateExerciseResult(choiceType, translator, exercised)
              } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.drop(2))
            }
            case _ => Left(s"Expected Create, Exercise or ExerciseByKey but got $fb")
          }
          (bValue, eventResults) = r
          fValue <- fillCommandResults(
            compiledPackages,
            choiceType,
            translator,
            apfba,
            eventResults)
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

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      translator: ValueTranslator,
      created: CreatedEvent): Either[String, SValue] = {
    val anyTemplateTyCon = daInternalAny("AnyTemplate")
    val pairTyCon = daTypes("Tuple2")
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
      anyTpl = record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), argSValue)))
      cid <- ContractIdString.fromString(created.contractId)
    } yield record(pairTyCon, ("_1", SContractId(AbsoluteContractId(cid))), ("_2", anyTpl))
  }

  def fromStatusException(
      scriptIds: ScriptIds,
      ex: StatusRuntimeException): Either[String, SValue] = {
    val status = ex.getStatus
    Right(
      record(
        scriptIds.damlScript("SubmitFailure"),
        ("status", SInt64(status.getCode.value.asInstanceOf[Long])),
        ("description", SText(status.getDescription))
      ))
  }

  def toIfaceType(
      ctx: QualifiedName,
      astTy: Ast.Type,
  ): Either[String, iface.Type] =
    InterfaceReader.toIfaceType(ctx, astTy) match {
      case -\/(e) => Left(e.toString)
      case \/-(ty) => Right(ty)
    }
}
