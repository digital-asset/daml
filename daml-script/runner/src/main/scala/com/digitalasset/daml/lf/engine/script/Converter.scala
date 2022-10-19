// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import com.daml.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.language.Ast._
import com.daml.lf.language.StablePackage.DA
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{ArrayList, Pretty, SValue, Speedy}
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.{-\/, OneAnd, \/-}
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.Future

// Helper to create identifiers pointing to the Daml.Script module
case class ScriptIds(val scriptPackageId: PackageId) {
  def damlScript(s: String) =
    Identifier(
      scriptPackageId,
      QualifiedName(ModuleName.assertFromString("Daml.Script"), DottedName.assertFromString(s)),
    )
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

final case class AnyTemplate(ty: Identifier, arg: SValue)
sealed abstract class AnyChoice extends Product with Serializable {
  def name: ChoiceName
  def arg: SValue
}
object AnyChoice {
  final case class Template(name: ChoiceName, arg: SValue) extends AnyChoice
  final case class Interface(ifaceId: Identifier, name: ChoiceName, arg: SValue) extends AnyChoice
}
final case class AnyContractKey(key: SValue)
// frames ordered from most-recent to least-recent
final case class StackTrace(frames: Vector[Location]) {
  // Return the most recent frame
  def topFrame: Option[Location] =
    frames.headOption
  def pretty(l: Location) =
    s"${l.definition} at ${l.packageId}:${l.module}:${l.start._1}"
  def pretty(): String =
    frames.map(pretty(_)).mkString("\n")

}
object StackTrace {
  val empty: StackTrace = StackTrace(Vector.empty)
}

object Converter {
  import com.daml.script.converter.Converter._

  private def toNonEmptySet[A](as: OneAnd[FrontStack, A]): OneAnd[Set, A] = {
    import scalaz.syntax.foldable._
    OneAnd(as.head, as.tail.toSet - as.head)
  }

  private def fromIdentifier(id: value.Identifier): SValue = {
    STypeRep(
      TTyCon(
        TypeConName(
          PackageId.assertFromString(id.packageId),
          QualifiedName(
            DottedName.assertFromString(id.moduleName),
            DottedName.assertFromString(id.entityName),
          ),
        )
      )
    )
  }

  private def fromTemplateTypeRep(templateId: value.Identifier): SValue =
    record(DA.Internal.Any.TemplateTypeRep, ("getTemplateTypeRep", fromIdentifier(templateId)))

  private def fromAnyContractId(
      scriptIds: ScriptIds,
      templateId: value.Identifier,
      contractId: String,
  ): SValue = {
    val contractIdTy = scriptIds.damlScript("AnyContractId")
    record(
      contractIdTy,
      ("templateId", fromTemplateTypeRep(templateId)),
      ("contractId", SContractId(ContractId.assertFromString(contractId))),
    )
  }

  def toFuture[T](s: Either[String, T]): Future[T] = s match {
    case Left(err) => Future.failed(new ConverterException(err))
    case Right(s) => Future.successful(s)
  }

  private def fromAnyTemplate(
      translator: preprocessing.ValueTranslator,
      templateId: Identifier,
      argument: Value,
  ): Either[String, SValue] = {
    for {
      translated <- translator
        .translateValue(TTyCon(templateId), argument)
        .left
        .map(err => s"Failed to translate create argument: $err")
    } yield record(
      DA.Internal.Any.AnyTemplate,
      ("getAnyTemplate", SAny(TTyCon(templateId), translated)),
    )
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

  private def fromAnyChoice(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      argument: Value,
  ): Either[String, SValue] = {
    for {
      choice <- lookupChoice(templateId, interfaceId, choiceName)
      translated <- translator
        .translateValue(choice.argBinder._2, argument)
        .left
        .map(err => s"Failed to translate exercise argument: $err")
    } yield record(
      DA.Internal.Any.AnyChoice,
      ("getAnyChoice", SAny(choice.argBinder._2, translated)),
      (
        "getAnyChoiceTemplateTypeRep",
        fromTemplateTypeRep(toApiIdentifier(interfaceId.getOrElse(templateId))),
      ),
    )
  }

  private[this] def choiceArgTypeToChoiceName(choiceCons: TypeConName) = {
    // This exploits the fact that in Daml, choice argument type names
    // and choice names match up.
    assert(choiceCons.qualifiedName.name.segments.length == 1)
    choiceCons.qualifiedName.name.segments.head
  }

  private[this] def toAnyChoice(v: SValue): Either[String, AnyChoice] =
    v match {
      case SRecord(_, _, ArrayList(SAny(TTyCon(choiceCons), choiceVal), _)) =>
        Right(AnyChoice.Template(choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case SRecord(
            _,
            _,
            ArrayList(
              SAny(
                TStruct(Struct((_, TTyCon(choiceCons)), _)),
                SStruct(_, ArrayList(choiceVal, STypeRep(TTyCon(ifaceId)))),
              ),
              _,
            ),
          ) =>
        Right(AnyChoice.Interface(ifaceId, choiceArgTypeToChoiceName(choiceCons), choiceVal))
      case _ =>
        Left(s"Expected AnyChoice but got $v")
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
          case x => Left(s"Expected STypeRep but got $x")
        }
      }
      case _ => Left(s"Expected TemplateTypeRep but got $v")
    }
  }

  def toCreateCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // template argument, continuation
      case SRecord(_, _, vals) if vals.size == 2 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
        } yield command.ApiCommand.Create(
          templateId = anyTemplate.ty,
          argument = anyTemplate.arg.toUnnormalizedValue,
        )
      }
      case _ => Left(s"Expected Create but got $v")
    }

  def toExerciseCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 =>
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          cid <- toContractId(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
        } yield anyChoice match {
          case AnyChoice.Template(name, arg) =>
            command.ApiCommand.Exercise(
              typeId = TemplateOrInterface.Template(tplId),
              contractId = cid,
              choiceId = name,
              argument = arg.toUnnormalizedValue,
            )
          case AnyChoice.Interface(ifaceId, name, arg) =>
            command.ApiCommand.Exercise(
              typeId = TemplateOrInterface.Interface(ifaceId),
              contractId = cid,
              choiceId = name,
              argument = arg.toUnnormalizedValue,
            )
        }
      case _ => Left(s"Expected Exercise but got $v")
    }

  def toExerciseByKeyCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 =>
        for {
          anyKey <- toAnyContractKey(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
          typeId <- anyChoice match {
            case _: AnyChoice.Template =>
              typeRepToIdentifier(vals.get(0))
            case AnyChoice.Interface(ifaceId, _, _) =>
              Right(ifaceId)
          }
        } yield command.ApiCommand.ExerciseByKey(
          templateId = typeId,
          contractKey = anyKey.key.toUnnormalizedValue,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case _ => Left(s"Expected ExerciseByKey but got $v")
    }

  def toCreateAndExerciseCommand(v: SValue): Either[String, command.ApiCommand.CreateAndExercise] =
    v match {
      case SRecord(_, _, vals) if vals.size == 3 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          anyChoice <- toAnyChoice(vals.get(1))
        } yield command.ApiCommand.CreateAndExercise(
          templateId = anyTemplate.ty,
          createArgument = anyTemplate.arg.toUnnormalizedValue,
          choiceId = anyChoice.name,
          choiceArgument = anyChoice.arg.toUnnormalizedValue,
        )
      }
      case _ => Left(s"Expected CreateAndExercise but got $v")
    }

  private val fstName = Name.assertFromString("fst")
  private val sndName = Name.assertFromString("snd")
  private[this] val tupleFieldInputOrder =
    Struct.assertFromSeq(List(fstName, sndName).zipWithIndex)
  private[this] val fstOutputIdx = tupleFieldInputOrder.indexOf(fstName)
  private[this] val sndOutputIdx = tupleFieldInputOrder.indexOf(sndName)

  private[this] val extractToTuple = SEMakeClo(
    Array(),
    2,
    SEAppAtomic(SEBuiltin(SBStructCon(tupleFieldInputOrder)), Array(SELocA(0), SELocA(1))),
  )

  // Extract the two fields out of the RankN encoding used in the Ap constructor.
  def toApFields(
      compiledPackages: CompiledPackages,
      fun: SValue,
  ): Either[String, (SValue, SValue)] = {
    val e = SELet1(extractToTuple, SEAppAtomic(SEValue(fun), Array(SELocS(1))))
    val machine =
      Speedy.Machine.fromPureSExpr(compiledPackages, e)(
        Script.DummyLoggingContext
      )
    machine.run() match {
      case SResultFinal(v, _) =>
        v match {
          case SStruct(_, values) if values.size == 2 =>
            Right((values.get(fstOutputIdx), values.get(sndOutputIdx)))
          case v => Left(s"Expected binary SStruct but got $v")
        }
      case SResultError(err) => Left(Pretty.prettyError(err).render(80))
      case res => Left(res.toString)
    }
  }

  // Walk over the free applicative and extract the list of commands
  def toCommands(
      compiledPackages: CompiledPackages,
      freeAp: SValue,
  ): Either[String, List[command.ApiCommand]] = {
    @tailrec
    def iter(
        v: SValue,
        commands: List[command.ApiCommand],
    ): Either[String, List[command.ApiCommand]] = {
      v match {
        case SVariant(_, "PureA", _, _) => Right(commands.reverse)
        case SVariant(_, "Ap", _, v) =>
          toApFields(compiledPackages, v) match {
            case Right((SVariant(_, "Create", _, create), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toCreateCommand(create) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, r :: commands)
              }
            case Right((SVariant(_, "Exercise", _, exercise), v)) =>
              // This can’t be a for-comprehension since it trips up tailrec optimization.
              toExerciseCommand(exercise) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, r :: commands)
              }
            case Right((SVariant(_, "ExerciseByKey", _, exerciseByKey), v)) =>
              toExerciseByKeyCommand(exerciseByKey) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, r :: commands)
              }
            case Right((SVariant(_, "CreateAndExercise", _, createAndExercise), v)) =>
              toCreateAndExerciseCommand(createAndExercise) match {
                case Left(err) => Left(err)
                case Right(r) => iter(v, r :: commands)
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
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      result: ScriptLedgerClient.ExerciseResult,
  ) = {
    for {
      choice <- Name.fromString(result.choice)
      c <- lookupChoice(result.templateId, result.interfaceId, choice)
      translated <- translator
        .translateValue(c.returnType, result.result)
        .left
        .map(err => s"Failed to translate exercise result: $err")
    } yield translated
  }

  def translateTransactionTree(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      scriptIds: ScriptIds,
      tree: ScriptLedgerClient.TransactionTree,
  ): Either[String, SValue] = {
    def translateTreeEvent(ev: ScriptLedgerClient.TreeEvent): Either[String, SValue] = ev match {
      case ScriptLedgerClient.Created(tplId, contractId, argument) =>
        for {
          anyTemplate <- fromAnyTemplate(translator, tplId, argument)
        } yield SVariant(
          scriptIds.damlScript("TreeEvent"),
          Name.assertFromString("CreatedEvent"),
          0,
          record(
            scriptIds.damlScript("Created"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId.coid)),
            ("argument", anyTemplate),
          ),
        )
      case ScriptLedgerClient.Exercised(
            tplId,
            ifaceId,
            contractId,
            choiceName,
            arg,
            childEvents,
          ) =>
        for {
          evs <- childEvents.traverse(translateTreeEvent(_))
          anyChoice <- fromAnyChoice(lookupChoice, translator, tplId, ifaceId, choiceName, arg)
        } yield SVariant(
          scriptIds.damlScript("TreeEvent"),
          Name.assertFromString("ExercisedEvent"),
          1,
          record(
            scriptIds.damlScript("Exercised"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId.coid)),
            ("choice", SText(choiceName)),
            ("argument", anyChoice),
            ("childEvents", SList(evs.to(FrontStack))),
          ),
        )
    }
    for {
      events <- tree.rootEvents.traverse(translateTreeEvent(_)): Either[String, List[SValue]]
    } yield record(
      scriptIds.damlScript("SubmitFailure"),
      ("rootEvents", SList(events.to(FrontStack))),
    )
  }

  // Given the free applicative for a submit request and the results of that request, we walk over the free applicative and
  // fill in the values for the continuation.
  def fillCommandResults(
      compiledPackages: CompiledPackages,
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      initialFreeAp: SValue,
      allEventResults: Seq[ScriptLedgerClient.CommandResult],
  ): Either[String, SExpr] = {

    // Given one CommandsF command and the list of events starting at this one
    // apply the continuation in the command to the event result
    // and return the remaining events.
    def fillResult(
        v: SValue,
        eventResults: Seq[ScriptLedgerClient.CommandResult],
    ): Either[String, (SExpr, Seq[ScriptLedgerClient.CommandResult])] = {
      v match {

        // We already validate these records during toCommands so we don’t bother doing proper validation again here.
        case SVariant(_, "Create", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(1)
          for {
            contractId <- eventResults.head match {
              case ScriptLedgerClient.CreateResult(cid) => Right(SContractId(cid))
              case ScriptLedgerClient.ExerciseResult(_, _, _, _) =>
                Left("Expected CreateResult but got ExerciseResult")
            }
          } yield (SEAppAtomic(SEValue(continue), Array(SEValue(contractId))), eventResults.tail)
        }
        case SVariant(_, "Exercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEAppAtomic(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "ExerciseByKey", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEAppAtomic(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "CreateAndExercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(2)
          // We get a create and an exercise event here. We only care about the exercise event so we skip the create.
          val exercised = eventResults(1).asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEAppAtomic(SEValue(continue), Array(SEValue(translated))), eventResults.drop(2))
        }
        case _ => Left(s"Expected Create, Exercise or ExerciseByKey but got $v")
      }
    }

    @tailrec
    def go(
        freeAp: SValue,
        eventResults: Seq[ScriptLedgerClient.CommandResult],
        acc: List[SExpr],
    ): Either[String, SExpr] =
      freeAp match {
        case SVariant(_, "PureA", _, v) =>
          Right(acc match {
            case Nil => SEValue(v)
            case _ :: _ =>
              val locs: Array[SExprAtomic] = (1 to acc.length).toArray.reverse.map(SELocS(_))
              acc.foldRight[SExpr](SEAppAtomic(SEValue(v), locs))({ case (e, acc) =>
                SELet1(e, acc)
              })
          })

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

  def toParty(v: SValue): Either[String, Party] =
    v match {
      case SParty(p) => Right(p)
      case _ => Left(s"Expected SParty but got $v")
    }

  def toParties(v: SValue): Either[String, OneAnd[Set, Party]] =
    v match {
      case SList(FrontStackCons(x, xs)) =>
        OneAnd(x, xs).traverse(toParty(_)).map(toNonEmptySet(_))
      case SParty(p) =>
        Right(
          OneAnd(p, Set())
        ) // For backwards compatibility, we support a single part here as well.
      case _ => Left(s"Expected non-empty SList but got $v")
    }

  def toTimestamp(v: SValue): Either[String, Time.Timestamp] =
    v match {
      case STimestamp(t) => Right(t)
      case _ => Left(s"Expected STimestamp but got $v")
    }

  def toInt(v: SValue): Either[String, Int] = v match {
    case SInt64(n) => Right(n.toInt)
    case v => Left(s"Expected SInt64 but got $v")
  }

  def toList[A](v: SValue, convertElem: SValue => Either[String, A]): Either[String, List[A]] =
    v match {
      case SList(xs) =>
        xs.toImmArray.toList.traverse(convertElem)
      case _ => Left(s"Expected SList but got $v")
    }

  def toOptional[A](
      v: SValue,
      convertElem: SValue => Either[String, A],
  ): Either[String, Option[A]] =
    v match {
      case SOptional(v) => v.traverse(convertElem)
      case _ => Left(s"Expected SOptional but got $v")
    }

  private case class SrcLoc(
      pkgId: PackageId,
      module: ModuleName,
      start: (Int, Int),
      end: (Int, Int),
  )

  private def toSrcLoc(knownPackages: Map[String, PackageId], v: SValue): Either[String, SrcLoc] =
    v match {
      case SRecord(
            _,
            _,
            ArrayList(unitId, module, file @ _, startLine, startCol, endLine, endCol),
          ) =>
        for {
          unitId <- toText(unitId)
          packageId <- unitId match {
            // GHC uses unit-id "main" for the current package,
            // but the scenario context expects "-homePackageId-".
            case "main" => PackageId.fromString("-homePackageId-")
            case id => knownPackages.get(id).toRight(s"Unknown package $id")
          }
          module <- toText(module).flatMap(ModuleName.fromString(_))
          startLine <- toInt(startLine)
          startCol <- toInt(startCol)
          endLine <- toInt(endLine)
          endCol <- toInt(endCol)
        } yield SrcLoc(packageId, module, (startLine, startCol), (endLine, endCol))
      case _ => Left(s"Expected SrcLoc but got $v")
    }

  def toLocation(knownPackages: Map[String, PackageId], v: SValue): Either[String, Location] =
    v match {
      case SRecord(_, _, ArrayList(definition, loc)) =>
        for {
          // TODO[AH] This should be the outer definition. E.g. `main` in `main = do submit ...`.
          //   However, the call-stack only gives us access to the inner definition, `submit` in this case.
          //   The definition is not used when pretty printing locations. So, we can ignore this for now.
          definition <- toText(definition)
          loc <- toSrcLoc(knownPackages, loc)
        } yield Location(loc.pkgId, loc.module, definition, loc.start, loc.end)
      case _ => Left(s"Expected (Text, SrcLoc) but got $v")
    }

  def toStackTrace(
      knownPackages: Map[String, PackageId],
      v: SValue,
  ): Either[String, StackTrace] =
    v match {
      case SList(frames) =>
        frames.toVector.traverse(toLocation(knownPackages, _)).map(StackTrace(_))
      case _ =>
        new Throwable().printStackTrace();
        Left(s"Expected SList but got $v")
    }

  def toParticipantName(v: SValue): Either[String, Option[Participant]] = v match {
    case SOptional(Some(SText(t))) => Right(Some(Participant(t)))
    case SOptional(None) => Right(None)
    case _ => Left(s"Expected optional participant name but got $v")
  }

  def fromApiIdentifier(id: value.Identifier): Either[String, Identifier] =
    for {
      packageId <- PackageId.fromString(id.packageId)
      moduleName <- DottedName.fromString(id.moduleName)
      entityName <- DottedName.fromString(id.entityName)
    } yield Identifier(packageId, QualifiedName(moduleName, entityName))

  // Convert an active contract to AnyTemplate
  def fromContract(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
  ): Either[String, SValue] = fromAnyTemplate(translator, contract.templateId, contract.argument)

  def fromInterfaceView(
      translator: preprocessing.ValueTranslator,
      viewType: Type,
      value: Value,
  ): Either[String, SValue] = {
    for {
      translated <- translator
        .translateValue(viewType, value)
        .left
        .map(err => s"Failed to translate value of interface view: $err")
    } yield translated
  }

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
  ): Either[String, SValue] = {
    for {
      anyTpl <- fromContract(translator, contract)
    } yield record(DA.Types.Tuple2, ("_1", SContractId(contract.contractId)), ("_2", anyTpl))
  }

  def fromStatusException(
      scriptIds: ScriptIds,
      ex: StatusRuntimeException,
  ): Either[String, SValue] = {
    val status = ex.getStatus
    Right(
      record(
        scriptIds.damlScript("SubmitFailure"),
        ("status", SInt64(status.getCode.value.asInstanceOf[Long])),
        ("description", SText(status.getDescription)),
      )
    )
  }

  def fromPartyDetails(scriptIds: ScriptIds, details: PartyDetails): Either[String, SValue] = {
    Right(
      record(
        scriptIds.damlScript("PartyDetails"),
        ("party", SParty(details.party)),
        ("displayName", SOptional(details.displayName.map(SText))),
        ("isLocal", SBool(details.isLocal)),
      )
    )
  }

  def fromOptional[A](x: Option[A], f: A => Either[String, SValue]): Either[String, SOptional] =
    x.traverse(f).map(SOptional(_))

  def fromUser(scriptIds: ScriptIds, user: User): Either[String, SValue] =
    Right(
      record(
        scriptIds.damlScript("User"),
        ("id", fromUserId(scriptIds, user.id)),
        ("primaryParty", SOptional(user.primaryParty.map(SParty(_)))),
      )
    )

  def fromUserId(scriptIds: ScriptIds, userId: UserId): SValue =
    record(
      scriptIds.damlScript("UserId"),
      ("userName", SText(userId)),
    )

  def toUser(v: SValue): Either[String, User] =
    v match {
      case SRecord(_, _, vals) if vals.size >= 2 =>
        for {
          id <- toUserId(vals.get(0))
          primaryParty <- toOptional(vals.get(1), toParty)
        } yield User(id, primaryParty)
      case _ => Left(s"Expected User but got $v")
    }

  def toUserId(v: SValue): Either[String, UserId] =
    v match {
      case SRecord(_, _, vals) if vals.size == 1 =>
        for {
          userName <- toText(vals.get(0))
          userId <- UserId.fromString(userName)
        } yield userId
      case _ => Left(s"Expected UserId but got $v")
    }

  def fromUserRight(
      scriptIds: ScriptIds,
      right: UserRight,
  ): Either[String, SValue] = {
    def toRight(constructor: String, rank: Int, value: SValue): SValue =
      SVariant(scriptIds.damlScript("UserRight"), Name.assertFromString(constructor), rank, value)
    Right(right match {
      case UserRight.ParticipantAdmin => toRight("ParticipantAdmin", 0, SUnit)
      case UserRight.CanActAs(p) => toRight("CanActAs", 1, SParty(p))
      case UserRight.CanReadAs(p) => toRight("CanReadAs", 2, SParty(p))
    })
  }

  def toUserRight(v: SValue): Either[String, UserRight] =
    v match {
      case SVariant(_, "ParticipantAdmin", _, SUnit) =>
        Right(UserRight.ParticipantAdmin)
      case SVariant(_, "CanReadAs", _, v) =>
        toParty(v).map(UserRight.CanReadAs(_))
      case SVariant(_, "CanActAs", _, v) =>
        toParty(v).map(UserRight.CanActAs(_))
      case _ => Left(s"Expected ParticipantAdmin, CanReadAs or CanActAs but got $v")
    }

  def toIfaceType(
      ctx: QualifiedName,
      astTy: Type,
  ): Either[String, typesig.Type] =
    SignatureReader.toIfaceType(ctx, astTy) match {
      case -\/(e) => Left(e.toString)
      case \/-(ty) => Right(ty)
    }

  def fromJsonValue(
      ctx: QualifiedName,
      environmentSignature: EnvironmentSignature,
      compiledPackages: CompiledPackages,
      ty: Type,
      jsValue: JsValue,
  ): Either[String, SValue] = {
    def damlLfTypeLookup(id: Identifier): Option[typesig.DefDataType.FWT] =
      environmentSignature.typeDecls.get(id).map(_.`type`)
    for {
      paramIface <- Converter
        .toIfaceType(ctx, ty)
        .left
        .map(s => s"Failed to convert $ty: $s")
      lfValue <-
        try {
          Right(
            jsValue.convertTo[Value](
              LfValueCodec.apiValueJsonReader(paramIface, damlLfTypeLookup(_))
            )
          )
        } catch {
          case e: Exception => Left(s"LF conversion failed: ${e.toString}")
        }
      valueTranslator =
        new preprocessing.ValueTranslator(
          compiledPackages.pkgInterface,
          requireV1ContractIdSuffix = false,
        )
      sValue <- valueTranslator
        .translateValue(ty, lfValue)
        .left
        .map(_.message)
    } yield sValue
  }

  def fromTransactionTree(
      tree: TransactionTree
  ): Either[String, ScriptLedgerClient.TransactionTree] = {
    def convEvent(ev: String): Either[String, ScriptLedgerClient.TreeEvent] =
      tree.eventsById.get(ev).toRight(s"Event id $ev does not exist").flatMap { event =>
        event.kind match {
          case TreeEvent.Kind.Created(created) =>
            for {
              tplId <- Converter.fromApiIdentifier(created.getTemplateId)
              cid <- ContractId.fromString(created.contractId)
              arg <-
                NoLoggingValueValidator
                  .validateRecord(created.getCreateArguments)
                  .left
                  .map(err => s"Failed to validate create argument: $err")
            } yield ScriptLedgerClient.Created(
              tplId,
              cid,
              arg,
            )
          case TreeEvent.Kind.Exercised(exercised) =>
            for {
              tplId <- Converter.fromApiIdentifier(exercised.getTemplateId)
              ifaceId <- exercised.interfaceId.traverse(Converter.fromApiIdentifier)
              cid <- ContractId.fromString(exercised.contractId)
              choice <- ChoiceName.fromString(exercised.choice)
              choiceArg <- NoLoggingValueValidator
                .validateValue(exercised.getChoiceArgument)
                .left
                .map(err => s"Failed to validate exercise argument: $err")
              childEvents <- exercised.childEventIds.toList.traverse(convEvent(_))
            } yield ScriptLedgerClient.Exercised(
              tplId,
              ifaceId,
              cid,
              choice,
              choiceArg,
              childEvents,
            )
          case TreeEvent.Kind.Empty => throw new RuntimeException("foo")
        }
      }
    for {
      rootEvents <- tree.rootEventIds.toList.traverse(convEvent(_))
    } yield {
      ScriptLedgerClient.TransactionTree(rootEvents)
    }
  }
}
