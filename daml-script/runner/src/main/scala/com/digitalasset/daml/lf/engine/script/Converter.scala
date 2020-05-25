// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine
package script

import io.grpc.StatusRuntimeException
import java.util

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scalaz.{-\/, \/-}
import spray.json._
import com.daml.lf.data.Ref._
import com.daml.lf.iface
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.{InitialSeeding, Pretty, SExpr, SValue, Speedy}
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.lf.CompiledPackages
import com.daml.ledger.api.v1.value
import com.daml.lf.data.Time

// Helper to create identifiers pointing to the DAML.Script module
case class ScriptIds(val scriptPackageId: PackageId) {
  def damlScript(s: String) =
    Identifier(
      scriptPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Script"), DottedName.assertFromString(s)))
}

object ScriptIds {
  // Constructs ScriptIds if the given type has the form Daml.Script.Script a.
  def fromType(ty: Type): Option[ScriptIds] = {
    ty match {
      case TApp(TTyCon(tyCon), _) => {
        val scriptIds = ScriptIds(tyCon.packageId)
        if (tyCon == scriptIds.damlScript("Script")) {
          Some(scriptIds)
        } else {
          None
        }
      }
      case _ => None
    }
  }
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

  private def toLedgerRecord(v: SValue): Either[String, Value[AbsoluteContractId]] =
    v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")

  private def toLedgerValue(v: SValue): Either[String, Value[AbsoluteContractId]] =
    v.toValue.ensureNoRelCid.left.map(rcoid => s"Unexpected contract id $rcoid")

  def toFuture[T](s: Either[String, T]): Future[T] = s match {
    case Left(err) => Future.failed(new ConverterException(err))
    case Right(s) => Future.successful(s)
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

  def typeRepToIdentifier(v: SValue): Either[String, Identifier] = {
    v match {
      case SRecord(_, _, vals) if vals.size == 1 => {
        vals.get(0) match {
          case STypeRep(TTyCon(ty)) => Right(ty)
          case x => Left(s"Expected STypeRep but got $v")
        }
      }
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  def toCreateCommand(v: SValue): Either[String, ScriptLedgerClient.Command] =
    v match {
      // template argument, continuation
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          templateArg <- toLedgerRecord(anyTemplate.arg)
        } yield ScriptLedgerClient.CreateCommand(anyTemplate.ty, templateArg)
      }
      case _ => Left(s"Expected Create but got $v")
    }

  def toContractId(v: SValue): Either[String, AbsoluteContractId] =
    v match {
      case SContractId(cid: AbsoluteContractId) => Right(cid)
      case _ => Left(s"Expected AbsoluteContractId but got $v")
    }

  def toExerciseCommand(v: SValue): Either[String, ScriptLedgerClient.Command] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          cid <- toContractId(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ScriptLedgerClient.ExerciseCommand(tplId, cid, anyChoice.name, choiceArg)
      }
      case _ => Left(s"Expected Exercise but got $v")
    }

  def toExerciseByKeyCommand(v: SValue): Either[String, ScriptLedgerClient.Command] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          anyKey <- toAnyContractKey(vals.get(1))
          keyArg <- toLedgerValue(anyKey.key)
          anyChoice <- toAnyChoice(vals.get(2))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield ScriptLedgerClient.ExerciseByKeyCommand(tplId, keyArg, anyChoice.name, choiceArg)
      }
      case _ => Left(s"Expected ExerciseByKey but got $v")
    }

  def toCreateAndExerciseCommand(v: SValue): Either[String, ScriptLedgerClient.Command] =
    v match {
      case SRecord(_, _, vals) if vals.size == 3 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          templateArg <- toLedgerRecord(anyTemplate.arg)
          anyChoice <- toAnyChoice(vals.get(1))
          choiceArg <- toLedgerValue(anyChoice.arg)
        } yield
          ScriptLedgerClient.CreateAndExerciseCommand(
            anyTemplate.ty,
            templateArg,
            anyChoice.name,
            choiceArg)
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
        Array(SELocA(0), SELocA(1))),
    )
    val machine =
      Speedy.Machine.fromSExpr(
        sexpr = SEApp(SEValue(fun), Array(extractStruct)),
        compiledPackages = compiledPackages,
        submissionTime = Time.Timestamp.now(),
        seeding = InitialSeeding.NoSeed,
        Set.empty,
      )
    machine.run() match {
      case SResultFinalValue(v) =>
        v match {
          case SStruct(_, values) if values.size == 2 => {
            Right((values.get(0), values.get(1)))
          }
          case v => Left(s"Expected SStruct but got $v")
        }
      case SResultError(err) => Left(Pretty.prettyError(err, machine.ptx).render(80))
      case res => Left(res.toString())
    }
  }

  // Walk over the free applicative and extract the list of commands
  def toCommands(
      compiledPackages: CompiledPackages,
      freeAp: SValue): Either[String, List[ScriptLedgerClient.Command]] = {
    @tailrec
    def iter(v: SValue, commands: List[ScriptLedgerClient.Command])
      : Either[String, List[ScriptLedgerClient.Command]] = {
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
    iter(freeAp, List())
  }

  def translateExerciseResult(
      choiceType: (Identifier, Name) => Either[String, Type],
      translator: preprocessing.ValueTranslator,
      result: ScriptLedgerClient.ExerciseResult) = {
    for {
      choice <- Name.fromString(result.choice)
      resultType <- choiceType(result.templateId, choice)
      translated <- translator
        .translateValue(resultType, result.result)
        .left
        .map(err => s"Failed to translate exercise result: $err")
    } yield translated
  }

  // Given the free applicative for a submit request and the results of that request, we walk over the free applicative and
  // fill in the values for the continuation.
  def fillCommandResults(
      compiledPackages: CompiledPackages,
      choiceType: (Identifier, Name) => Either[String, Type],
      translator: preprocessing.ValueTranslator,
      initialFreeAp: SValue,
      allEventResults: Seq[ScriptLedgerClient.CommandResult]): Either[String, SExpr] = {

    // Given one CommandsF command and the list of events starting at this one
    // apply the continuation in the command to the event result
    // and return the remaining events.
    def fillResult(v: SValue, eventResults: Seq[ScriptLedgerClient.CommandResult])
      : Either[String, (SExpr, Seq[ScriptLedgerClient.CommandResult])] = {
      v match {

        // We already validate these records during toCommands so we don’t bother doing proper validation again here.
        case SVariant(_, "Create", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(1)
          for {
            contractId <- eventResults.head match {
              case ScriptLedgerClient.CreateResult(cid) => Right(SContractId(cid))
              case ScriptLedgerClient.ExerciseResult(_, _, _) =>
                Left("Expected CreateResult but got ExerciseResult")
            }
          } yield (SEApp(SEValue(continue), Array(SEValue(contractId))), eventResults.tail)
        }
        case SVariant(_, "Exercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(choiceType, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "ExerciseByKey", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(choiceType, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "CreateAndExercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(2)
          // We get a create and an exercise event here. We only care about the exercise event so we skip the create.
          val exercised = eventResults(1).asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(choiceType, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.drop(2))
        }
        case _ => Left(s"Expected Create, Exercise or ExerciseByKey but got $v")
      }
    }

    @tailrec
    def go(
        freeAp: SValue,
        eventResults: Seq[ScriptLedgerClient.CommandResult],
        acc: List[SExpr]): Either[String, SExpr] =
      freeAp match {
        case SVariant(_, "PureA", _, v) =>
          Right(acc.foldLeft[SExpr](SEValue(v))({ case (acc, v) => SEApp(acc, Array(v)) }))
        case SVariant(_, "Ap", _, v) => {
          val r = for {
            apFields <- toApFields(compiledPackages, v)
            (fp, apfba) = apFields
            commandRes <- fillResult(fp, eventResults)
            (v, eventResults) = commandRes
          } yield (v, apfba, eventResults)
          r match {
            case Left(err) => Left(err)
            case Right((v, freeAp, eventResults)) =>
              go(freeAp, eventResults, v :: acc)
          }
        }
        case _ => Left(s"Expected PureA or Ap but got $freeAp")
      }
    go(initialFreeAp, allEventResults, List())
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
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract): Either[String, SValue] = {
    val anyTemplateTyCon = daInternalAny("AnyTemplate")
    val pairTyCon = daTypes("Tuple2")
    val tyCon = contract.templateId
    for {
      argSValue <- translator
        .translateValue(TTyCon(tyCon), contract.argument)
        .left
        .map(
          err => s"Failure to translate value in create: $err"
        )
      anyTpl = record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), argSValue)))
    } yield record(pairTyCon, ("_1", SContractId(contract.contractId)), ("_2", anyTpl))
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

  def fromJsonValue(
      ctx: QualifiedName,
      environmentInterface: EnvironmentInterface,
      compiledPackages: CompiledPackages,
      ty: Type,
      jsValue: JsValue
  ): Either[String, SValue] = {
    def damlLfTypeLookup(id: Identifier): Option[iface.DefDataType.FWT] =
      environmentInterface.typeDecls.get(id).map(_.`type`)
    for {
      paramIface <- Converter
        .toIfaceType(ctx, ty)
        .left
        .map(s => s"Failed to convert $ty: $s")
      lfValue <- try {
        Right(
          jsValue.convertTo[Value[AbsoluteContractId]](
            LfValueCodec.apiValueJsonReader(paramIface, damlLfTypeLookup(_))))
      } catch {
        case e: Exception => Left(s"LF conversion failed: ${e.toString}")
      }
      valueTranslator = new preprocessing.ValueTranslator(compiledPackages)
      sValue <- valueTranslator
        .translateValue(ty, lfValue)
        .left
        .map(_.msg)
    } yield sValue
  }
}
