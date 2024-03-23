// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script
package v1

import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.engine.script.v1.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.language.Ast._
import com.daml.lf.language.StablePackage.DA
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.{Pretty, SValue, Speedy}
import com.daml.lf.speedy.SExpr.SExpr
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

import scala.annotation.tailrec

object Converter extends script.ConverterMethods {
  import com.daml.script.converter.Converter._

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
    for {
      anyTpl <- fromContract(translator, contract)
    } yield record(DA.Types.Tuple2, ("_1", SContractId(contract.contractId)), ("_2", anyTpl))
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
          typeId <- typeRepToIdentifier(vals.get(0))
          cid <- toContractId(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
        } yield command.ApiCommand.Exercise(
          typeId = typeId,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = anyChoice.arg.toUnnormalizedValue,
        )
      case _ => Left(s"Expected Exercise but got $v")
    }

  def toExerciseByKeyCommand(v: SValue): Either[String, command.ApiCommand] =
    v match {
      // typerep, contract id, choice argument and continuation
      case SRecord(_, _, vals) if vals.size == 4 =>
        for {
          typeId <- typeRepToIdentifier(vals.get(0))
          anyKey <- toAnyContractKey(vals.get(1))
          anyChoice <- toAnyChoice(vals.get(2))
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

  // Extract the two fields out of the RankN encoding used in the Ap constructor.
  private[lf] def toApFields(
      compiledPackages: CompiledPackages,
      fun: SValue,
  ): Either[String, (SValue, SValue)] = {
    val e = SELet1(extractToTuple, SEAppAtomic(SEValue(fun), Array(SELocS(1))))
    val machine =
      Speedy.Machine.fromPureSExpr(compiledPackages, e)(
        Script.DummyLoggingContext
      )
    machine.run() match {
      case SResultFinal(v) =>
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
  private[lf] def toCommands(
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
}
