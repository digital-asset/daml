// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value
import com.daml.ledger.api.validation.ValueValidator
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{Pretty, SExpr, SValue, Speedy}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.script.converter.ConverterException
import io.grpc.StatusRuntimeException
import scalaz.std.list._
import scalaz.std.either._
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
final case class AnyChoice(name: ChoiceName, arg: SValue)
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

  private val DA_TYPES_PKGID =
    PackageId.assertFromString("40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7")
  private def daTypes(s: String): Identifier =
    Identifier(
      DA_TYPES_PKGID,
      QualifiedName(DottedName.assertFromString("DA.Types"), DottedName.assertFromString(s)),
    )

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

  private def fromTemplateTypeRep(templateId: value.Identifier): SValue = {
    val templateTypeRepTy = daInternalAny("TemplateTypeRep")
    record(templateTypeRepTy, ("getTemplateTypeRep", fromIdentifier(templateId)))
  }

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
      argument: Value[ContractId],
  ): Either[String, SValue] = {
    val anyTemplateTy = daInternalAny("AnyTemplate")
    for {
      translated <- translator
        .translateValue(TTyCon(templateId), argument)
        .left
        .map(err => s"Failed to translate create argument: $err")
    } yield record(
      anyTemplateTy,
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
      lookupChoice: (Identifier, Name) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      templateId: Identifier,
      choiceName: ChoiceName,
      argument: Value[ContractId],
  ): Either[String, SValue] = {
    val contractIdTy = daInternalAny("AnyChoice")
    for {
      choice <- lookupChoice(templateId, choiceName)
      translated <- translator
        .translateValue(choice.argBinder._2, argument)
        .left
        .map(err => s"Failed to translate exercise argument: $err")
    } yield record(
      contractIdTy,
      ("getAnyChoice", SAny(choice.argBinder._2, translated)),
      ("getAnyChoiceTemplateTypeRep", fromIdentifier(toApiIdentifier(templateId))),
    )
  }

  def toAnyChoice(v: SValue): Either[String, AnyChoice] = {
    v match {
      case SRecord(_, _, JavaList(SAny(TTyCon(tyCon), choiceVal), _)) =>
        // This exploits the fact that in Daml, choice argument type names
        // and choice names match up.
        ChoiceName.fromString(tyCon.qualifiedName.name.toString).map(AnyChoice(_, choiceVal))
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
        } yield command.CreateCommand(
          templateId = anyTemplate.ty,
          argument = anyTemplate.arg.toUnnormalizedValue,
        )
      }
      case _ => Left(s"Expected Create but got $v")
    }

  def toExerciseCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          cid <- toContractId(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
        } yield command.ExerciseCommand(
          templateId = tplId,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      }
      case _ => Left(s"Expected Exercise but got $v")
    }

  def toExerciseByKeyCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 => {
        for {
          tplId <- typeRepToIdentifier(vals.get(0))
          anyKey <- toAnyContractKey(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
        } yield command.ExerciseByKeyCommand(
          templateId = tplId,
          contractKey = anyKey.key.toUnnormalizedValue,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      }
      case _ => Left(s"Expected ExerciseByKey but got $v")
    }

  def toCreateAndExerciseCommand(v: SValue): Either[String, command.CreateAndExerciseCommand] =
    v match {
      case SRecord(_, _, vals) if vals.size == 3 => {
        for {
          anyTemplate <- toAnyTemplate(vals.get(0))
          anyChoice <- toAnyChoice(vals.get(1))
        } yield command.CreateAndExerciseCommand(
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
    SEApp(SEBuiltin(SBStructCon(tupleFieldInputOrder)), Array(SELocA(0), SELocA(1))),
  )

  // Extract the two fields out of the RankN encoding used in the Ap constructor.
  def toApFields(
      compiledPackages: CompiledPackages,
      fun: SValue,
  ): Either[String, (SValue, SValue)] = {
    val machine =
      Speedy.Machine.fromPureSExpr(compiledPackages, SEApp(SEValue(fun), Array(extractToTuple)))
    machine.run() match {
      case SResultFinalValue(v) =>
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
      lookupChoice: (Identifier, Name) => Either[String, TemplateChoiceSignature],
      translator: preprocessing.ValueTranslator,
      result: ScriptLedgerClient.ExerciseResult,
  ) = {
    for {
      choice <- Name.fromString(result.choice)
      c <- lookupChoice(result.templateId, choice)
      translated <- translator
        .translateValue(c.returnType, result.result)
        .left
        .map(err => s"Failed to translate exercise result: $err")
    } yield translated
  }

  def translateTransactionTree(
      lookupChoice: (Identifier, Name) => Either[String, TemplateChoiceSignature],
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
      case ScriptLedgerClient.Exercised(tplId, contractId, choiceName, arg, childEvents) =>
        for {
          evs <- childEvents.traverse(translateTreeEvent(_))
          anyChoice <- fromAnyChoice(lookupChoice, translator, tplId, choiceName, arg)
        } yield SVariant(
          scriptIds.damlScript("TreeEvent"),
          Name.assertFromString("ExercisedEvent"),
          1,
          record(
            scriptIds.damlScript("Exercised"),
            ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId.coid)),
            ("choice", SText(choiceName)),
            ("argument", anyChoice),
            ("childEvents", SList(FrontStack(evs))),
          ),
        )
    }
    for {
      events <- tree.rootEvents.traverse(translateTreeEvent(_)): Either[String, List[SValue]]
    } yield record(
      scriptIds.damlScript("SubmitFailure"),
      ("rootEvents", SList(FrontStack(events))),
    )
  }

  // Given the free applicative for a submit request and the results of that request, we walk over the free applicative and
  // fill in the values for the continuation.
  def fillCommandResults(
      compiledPackages: CompiledPackages,
      lookupChoice: (Identifier, Name) => Either[String, TemplateChoiceSignature],
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
              case ScriptLedgerClient.ExerciseResult(_, _, _) =>
                Left("Expected CreateResult but got ExerciseResult")
            }
          } yield (SEApp(SEValue(continue), Array(SEValue(contractId))), eventResults.tail)
        }
        case SVariant(_, "Exercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "ExerciseByKey", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(3)
          val exercised = eventResults.head.asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.tail)
        }
        case SVariant(_, "CreateAndExercise", _, v) => {
          val continue = v.asInstanceOf[SRecord].values.get(2)
          // We get a create and an exercise event here. We only care about the exercise event so we skip the create.
          val exercised = eventResults(1).asInstanceOf[ScriptLedgerClient.ExerciseResult]
          for {
            translated <- translateExerciseResult(lookupChoice, translator, exercised)
          } yield (SEApp(SEValue(continue), Array(SEValue(translated))), eventResults.drop(2))
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

  def toParty(v: SValue): Either[String, Ref.Party] =
    v match {
      case SParty(p) => Right(p)
      case _ => Left(s"Expected SParty but got $v")
    }

  def toParties(v: SValue): Either[String, OneAnd[Set, Ref.Party]] =
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
            JavaList(unitId, module, file @ _, startLine, startCol, endLine, endCol),
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
      case SRecord(_, _, JavaList(definition, loc)) =>
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

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      translator: preprocessing.ValueTranslator,
      contract: ScriptLedgerClient.ActiveContract,
  ): Either[String, SValue] = {
    val pairTyCon = daTypes("Tuple2")
    for {
      anyTpl <- fromContract(translator, contract)
    } yield record(pairTyCon, ("_1", SContractId(contract.contractId)), ("_2", anyTpl))
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

  def toIfaceType(
      ctx: QualifiedName,
      astTy: Type,
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
      jsValue: JsValue,
  ): Either[String, SValue] = {
    def damlLfTypeLookup(id: Identifier): Option[iface.DefDataType.FWT] =
      environmentInterface.typeDecls.get(id).map(_.`type`)
    for {
      paramIface <- Converter
        .toIfaceType(ctx, ty)
        .left
        .map(s => s"Failed to convert $ty: $s")
      lfValue <-
        try {
          Right(
            jsValue.convertTo[Value[ContractId]](
              LfValueCodec.apiValueJsonReader(paramIface, damlLfTypeLookup(_))
            )
          )
        } catch {
          case e: Exception => Left(s"LF conversion failed: ${e.toString}")
        }
      valueTranslator =
        new preprocessing.ValueTranslator(
          compiledPackages.interface,
          requireV1ContractId = false,
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
                ValueValidator
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
              cid <- ContractId.fromString(exercised.contractId)
              choice <- ChoiceName.fromString(exercised.choice)
              choiceArg <- ValueValidator
                .validateValue(exercised.getChoiceArgument)
                .left
                .map(err => s"Failed to validate exercise argument: $err")
              childEvents <- exercised.childEventIds.toList.traverse(convEvent(_))
            } yield ScriptLedgerClient.Exercised(
              tplId,
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
