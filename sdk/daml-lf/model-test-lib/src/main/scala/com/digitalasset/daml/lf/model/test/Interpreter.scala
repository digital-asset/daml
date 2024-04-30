// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.instances.all._
import com.daml.lf.data.Ref
import com.daml.lf.engine.script.Disclosure
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.{
  CommandResult,
  CommandWithMeta,
  CreateResult,
  ExerciseResult,
}
import com.daml.lf.model.test.LedgerImplicits._
import com.daml.lf.model.test.Ledgers._
import com.daml.lf.model.test.ToCommands.{
  ContractIdMapping,
  PartyIdMapping,
  UniversalContractId,
  UniversalWithKeyContractId,
}
import com.daml.lf.value.{Value => V}
import org.apache.pekko.stream.Materializer
import scalaz.OneAnd

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.{ExecutionContext, Future, blocking}

object Interpreter {
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
    ledgerClients: PartialFunction[ParticipantId, ScriptLedgerClient],
    replicateParties: Function[Map[Ref.Party, (ParticipantId, Set[ParticipantId])], Future[Unit]],
) {
  import Interpreter._

  private val toCommands = new ToCommands(universalTemplatePkgId)

  type SymbSimplifiedTopology = Map[ParticipantId, Set[PartyId]]
  type SymbReplications = Map[PartyId, (ParticipantId, Set[ParticipantId])]

  type SimplifiedTopology = Map[ParticipantId, Set[Ref.Party]]
  type Replications = Map[Ref.Party, (ParticipantId, Set[ParticipantId])]

  def substituteInTopology(
      symbSimplifiedTopology: SymbSimplifiedTopology,
      partyIds: PartyIdMapping,
  ): SimplifiedTopology =
    symbSimplifiedTopology.view.mapValues(_.map(partyIds)).toMap

  def substituteInReplications(
      symbReplications: SymbReplications,
      partyIds: PartyIdMapping,
  ): Replications =
    symbReplications.map { case (partyId, replications) => partyIds(partyId) -> replications }

  // For each party, arbitrarily picks a participant that will host it and a
  // set of participants it will need to be replicated to.
  private def splitTopology(
      topology: Topology
  ): (SymbSimplifiedTopology, SymbReplications) = {
    val partyToReplications =
      topology.groupedByPartyId.map { case (partyId, participants) =>
        val participantIds = participants.map(_.participantId)
        (partyId, (participantIds.head, participantIds.tail))
      }
    val participantToDisjointPartySets =
      partyToReplications.groupMapReduce(_._2._1)(entry => Set(entry._1))(_ ++ _)
    (participantToDisjointPartySets, partyToReplications)
  }

  private def allocateParties(initialTopology: SymbSimplifiedTopology)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[PartyIdMapping] = initialTopology.toList
    .traverse { case (participantId, parties) =>
      allocateParties(ledgerClients(participantId), parties)
    }
    .map(_.foldLeft(Map.empty[PartyId, Ref.Party])(_ ++ _))

  // Party allocation doesn't seem thread-safe on the IDE ledger
  val partyAllocationLock = new ReentrantLock()
  private def allocateParties(ledgerClient: ScriptLedgerClient, partyIds: Iterable[PartyId])(
      implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[PartyIdMapping] = {
    val futures = partyIds
      .map(partyId => {
        val name = s"p$partyId"
        partyAllocationLock.lockInterruptibly()
        val res = ledgerClient.allocateParty("", name).map(partyId -> _)
        partyAllocationLock.unlock()
        res
      })
    Future.sequence(futures).map(_.toMap)
  }

  private def waitForPartyPropagation(
      topology: SimplifiedTopology
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Unit] =
    Future
      .sequence(
        for {
          participant <- topology
          ledgerClient = ledgerClients(participant._1)
        } yield waitForPartyPropagation(ledgerClient, participant._2)
      )
      .map(_ => ())

  private def waitForPartyPropagation(
      ledgerClient: ScriptLedgerClient,
      parties: Set[Ref.Party],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Unit] = {
    ledgerClient
      .listKnownParties()
      .flatMap(knownPartyDetails => {
        val knownParties = knownPartyDetails.map(_.party).toSet
        if (parties.subsetOf(knownParties)) {
          Future.successful(())
        } else {
          waitForPartyPropagation(ledgerClient, parties)
        }
      })
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
          case (
                V.ValueInt64(contractId),
                V.ValueVariant(_, "UniversalContractId", V.ValueContractId(cid)),
              ) =>
            contractId.toInt -> UniversalContractId(cid)
          case (
                V.ValueInt64(contractId),
                V.ValueVariant(_, "UniversalWithKeyContractId", V.ValueContractId(cid)),
              ) =>
            contractId.toInt -> UniversalWithKeyContractId(cid)
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
            Map(c.contractId -> UniversalContractId(r.contractId))
          case (c: CreateWithKey, r: CreateResult) =>
            Map(c.contractId -> UniversalWithKeyContractId(r.contractId))
          case (_: Exercise, r: ExerciseResult) =>
            assertContractIdMapping(r.result)
          case (_: ExerciseByKey, r: ExerciseResult) =>
            assertContractIdMapping(r.result)
          case (_, _) =>
            throw new IllegalArgumentException("unexpected action or result")
        }
      }
      .fold(Map.empty)(_ ++ _)

  private def fetchDisclosure(
      participants: Map[PartyId, Set[Participant]],
      contractInfos: ContractInfos,
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      contractIdToFetch: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Disclosure] = {
    val ContractInfo(templatedId, signatories) = contractInfos(contractIdToFetch)
    val concreteContractId = contractIds(contractIdToFetch).contractId
    val concreteSignatory = partyIds(signatories.head)
    for {
      contract <- ledgerClients(participants(signatories.head).head.participantId)
        .queryContractId(OneAnd(concreteSignatory, Set.empty), templatedId, concreteContractId)
        .map(
          _.getOrElse(
            throw new IllegalArgumentException(
              s"contract ${contractIdToFetch} not found while fetching disclosures"
            )
          )
        )
    } yield Disclosure(contract.templateId, concreteContractId, contract.blob)
  }

  private def fetchDisclosures(
      participants: Map[PartyId, Set[Participant]],
      contractInfos: ContractInfos,
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      contractIdsToFetch: Iterable[ContractId],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[List[Disclosure]] =
    contractIdsToFetch.toList
      .traverse(fetchDisclosure(participants, contractInfos, partyIds, contractIds, _))

  private def runCommands(
      participants: Map[PartyId, Set[Participant]],
      contractInfos: ContractInfos,
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      commands: Commands,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Eval[ContractIdMapping] = {
    for {
      apiCommands <- EitherT
        .fromEither[Future](
          commands.actions
            .traverse(
              toCommands.actionToApiCommand(partyIds, contractIds, _)
            )
        )
        .leftMap(TranslationError)
      disclosures <- EitherT.liftF(
        fetchDisclosures(participants, contractInfos, partyIds, contractIds, commands.disclosures)
      )
      resultAndTree <- EitherT(
        ledgerClients(commands.participantId).submit(
          actAs = assertToOneAnd(commands.actAs.map(partyIds)),
          readAs = partyIds.values.toSet,
          disclosures = disclosures,
          commands = apiCommands.map(cmd =>
            CommandWithMeta(
              cmd,
              explicitPackageId = true,
            )
          ),
          optLocation = None,
          languageVersionLookup = _ => Left("language version lookup not supported"),
          errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.MustSucceed,
        )
      ).leftMap[InterpreterError](SubmitFailure)
    } yield commandResultsToContractIdMapping(commands.actions, resultAndTree._1)
  }

  private def runCommandsList(
      participants: Map[PartyId, Set[Participant]],
      contractInfos: ContractInfos,
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
          newContractIds <- runCommands(
            participants,
            contractInfos,
            partyIds,
            contractIds,
            commands,
          )
          // TODO: find a better way
          _ <- EitherT.liftF(Future { blocking { Thread.sleep(500) } })
          result <- runCommandsList(
            participants,
            contractInfos,
            partyIds,
            contractIds ++ newContractIds,
            tail,
          )
        } yield result
    }

  private case class ContractInfo(
      templatedId: Ref.TypeConName,
      signatories: PartySet,
  )
  private type ContractInfos = Map[ContractId, ContractInfo]

  private def extractContractInfos(ledger: Ledger): ContractInfos = {

    def unions(infos: Iterable[ContractInfos]): ContractInfos =
      infos.foldLeft(Map.empty[ContractId, ContractInfo])(_ ++ _)

    def extractFromAction(
        action: Action
    ): ContractInfos = action match {
      case create: Create =>
        Map(
          create.contractId -> ContractInfo(
            toCommands.universalTemplateId,
            create.signatories,
          )
        )
      case create: CreateWithKey =>
        Map(
          create.contractId -> ContractInfo(
            toCommands.universalWithKeyTemplateId,
            create.signatories,
          )
        )
      case exe: Exercise =>
        extractFromTransaction(exe.subTransaction)
      case exe: ExerciseByKey =>
        extractFromTransaction(exe.subTransaction)
      case rb: Rollback =>
        extractFromTransaction(rb.subTransaction)
      case _: Fetch | _: FetchByKey | _: LookupByKey =>
        Map.empty
    }

    def extractFromTransaction(actions: Transaction): ContractInfos =
      unions(actions.map(extractFromAction))

    unions(ledger.map(commands => extractFromTransaction(commands.actions)))
  }

  def runLedger(scenario: Scenario)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[(PartyIdMapping, Either[InterpreterError, ContractIdMapping])] = {
    val Scenario(topology, ledger) = scenario
    val (disjointTopology, replications) = splitTopology(topology)
    for {
      partyIds <- allocateParties(disjointTopology)
      _ <- waitForPartyPropagation(substituteInTopology(disjointTopology, partyIds))
      _ <- replicateParties(substituteInReplications(replications, partyIds))
      _ <- waitForPartyPropagation(substituteInTopology(topology.simplify, partyIds))
      result <- runCommandsList(
        topology.groupedByPartyId,
        extractContractInfos(ledger),
        partyIds,
        Map.empty,
        ledger,
      ).value
    } yield (partyIds, result)
  }
}
