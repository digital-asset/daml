// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.instances.all._
import com.daml.lf.data.Ref
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.{
  CommandResult,
  CommandWithMeta,
  CreateResult,
  ExerciseResult,
}
import com.daml.lf.model.test.Ledgers._
import com.daml.lf.value.{Value => V}
import org.apache.pekko.stream.Materializer
import scalaz.OneAnd

import scala.concurrent.{ExecutionContext, Future}

object Interpreter {
  type PartyIdMapping = Map[PartyId, Ref.Party]
  type ContractIdMapping = Map[ContractId, V.ContractId]

  sealed trait InterpreterError {
    def pretty: String = this match {
      case TranslationError(error) => error.toString
      case SubmitFailure(failure) =>
        failure match {
          case ScriptLedgerClient.SubmitFailure(statusError: com.daml.lf.scenario.Error, _) =>
            com.daml.lf.scenario.Pretty.prettyError(statusError).render(80)
          case _ => failure.toString
        }
    }
  }
  final case class TranslationError(error: ToCommands.TranslationError) extends InterpreterError
  final case class SubmitFailure(failure: ScriptLedgerClient.SubmitFailure) extends InterpreterError
}

class Interpreter(
    universalTemplatePkgId: Ref.PackageId,
    ledgerClient: ScriptLedgerClient,
) {
  import Interpreter._

  private val toCommands = new ToCommands(universalTemplatePkgId)

  private def collectLedgerPartyIds(ledger: Ledger): Set[PartyId] =
    ledger.view.flatMap(collectCommandsPartyIds).toSet

  private def collectCommandsPartyIds(commands: Commands): Set[PartyId] =
    commands.actAs ++ collectTransactionPartyIds(commands.actions)

  private def collectTransactionPartyIds(transaction: Transaction): Set[PartyId] =
    transaction.view.flatMap(collectActionPartyIds).toSet

  private def collectActionPartyIds(action: Action): Set[PartyId] = action match {
    case Create(_, signatories, observers) => signatories ++ observers
    case Exercise(_, _, controllers, choiceObservers, subTransaction) =>
      controllers ++ choiceObservers ++ collectTransactionPartyIds(subTransaction)
    case Fetch(_) => Set.empty
    case Rollback(subTransaction) => collectTransactionPartyIds(subTransaction)
  }

  private def allocateParties(partyIds: Iterable[PartyId])(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[PartyIdMapping] = {
    val futures = partyIds
      .map(partyId => {
        val name = s"p$partyId"
        ledgerClient.allocateParty("", name).map(partyId -> _)
      })
    Future.sequence(futures).map(_.toMap)
  }

  private type Eval[A] = EitherT[Future, InterpreterError, A]

  private def assertToOneAnd[A](set: Set[A]): OneAnd[Set, A] = {
    set.toSeq match {
      case h +: t => OneAnd(h, t.toSet)
      case _ => throw new IllegalArgumentException("toOneAnd: empty set")
    }
  }

  private def assertContractIdMapping(value: V): ContractIdMapping =
    value match {
      case V.ValueGenMap(entries) =>
        entries.iterator.map {
          case (V.ValueInt64(contractId), V.ValueContractId(cid)) =>
            contractId.toInt -> cid
          case entry =>
            throw new IllegalArgumentException(s"assertContractIdMapping: invalid map entry $entry")
        }.toMap
      case _ =>
        throw new IllegalArgumentException(
          s"assertContractIdMapping: expected ValueGenMap, got $value"
        )
    }

  private def commandResultsToContractIdMapping(
      actions: List[Action],
      results: Seq[CommandResult],
  ): ContractIdMapping =
    actions
      .zip(results)
      .map { case (action, result) =>
        (action, result) match {
          case (c: Create, r: CreateResult) =>
            Map(c.contractId -> r.contractId)
          case (_: Exercise, r: ExerciseResult) =>
            assertContractIdMapping(r.result)
          case (_, _) =>
            throw new IllegalArgumentException("unexpected action or result")
        }
      }
      .fold(Map.empty)(_ ++ _)

  private def runCommands(
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      commands: Commands,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Eval[ContractIdMapping] =
    for {
      apiCommands <- EitherT
        .fromEither[Future](
          commands.actions
            .traverse(
              toCommands.actionToApiCommand(partyIds, contractIds, _)
            )
        )
        .leftMap(TranslationError)
      resultAndTree <- EitherT(
        ledgerClient.submit(
          actAs = assertToOneAnd(commands.actAs.map(partyIds)),
          readAs = partyIds.values.toSet,
          disclosures = List.empty,
          commands = apiCommands.map(cmd =>
            CommandWithMeta(
              cmd,
              explicitPackageId = false,
            )
          ),
          optLocation = None,
          languageVersionLookup = _ => Left("language version lookup not supported"),
          errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.MustSucceed,
        )
      ).leftMap[InterpreterError](SubmitFailure)
    } yield commandResultsToContractIdMapping(commands.actions, resultAndTree._1)

  private def runCommandsList(
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      commandsList: List[Commands],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Eval[ContractIdMapping] =
    commandsList match {
      case Nil => EitherT.pure(contractIds)
      case commands :: tail =>
        for {
          newContractIds <- runCommands(partyIds, contractIds, commands)
          result <- runCommandsList(partyIds, contractIds ++ newContractIds, tail)
        } yield result
    }

  def runLedger(ledger: Ledger)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(PartyIdMapping, Either[InterpreterError, ContractIdMapping])] =
    for {
      partyIds <- allocateParties(collectLedgerPartyIds(ledger))
      result <- runCommandsList(partyIds, Map.empty, ledger).value
    } yield (partyIds, result)
}
