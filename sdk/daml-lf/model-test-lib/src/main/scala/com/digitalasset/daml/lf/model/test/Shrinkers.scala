// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.model.test.Ledgers._
import com.daml.lf.model.test.SymbolicSolver
import org.scalacheck.Shrink

import scala.annotation.nowarn

object Shrinkers {

  private def validScenario(scenario: Scenario): Boolean = {
    val numParties = scenario.topology.flatMap(_.parties).toSet.size
    val res =
      try {
        SymbolicSolver.valid(scenario, numParties)
      } catch {
        case _: Throwable =>
          // sometimes z3 times out on ground constraints (!)
          false
      }
    if (res) print('.')
    res
  }

  lazy val shrinkPartyId: Shrink[PartyId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkPartySet: Shrink[PartySet] =
    Shrink.shrinkContainer[Set, PartyId](implicitly, shrinkPartyId, implicitly)

  lazy val shrinkContractIdSet: Shrink[ContractIdSet] =
    Shrink.shrinkContainer[Set, ContractId](implicitly, shrinkContractId, implicitly)

  lazy val shrinkKeyId: Shrink[KeyId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkContractId: Shrink[ContractId] =
    Shrink.shrinkIntegral[KeyId].suchThat(_ >= 0)

  lazy val shrinkParticipantId: Shrink[ParticipantId] =
    Shrink.shrinkIntegral[ParticipantId].suchThat(_ >= 0)

  @nowarn("cat=deprecation")
  lazy val shrinkExerciseKind: Shrink[ExerciseKind] = Shrink {
    case NonConsuming => Stream(Consuming)
    case Consuming => Stream.empty
  }

  lazy val shrinkTransaction: Shrink[Transaction] =
    Shrink.shrinkContainer[List, Action](implicitly, shrinkAction, implicitly)

  private def isNoRollback(action: Action): Boolean = action match {
    case _: Rollback => false
    case _ => true
  }

  lazy val shrinkAction: Shrink[Action] = Shrink {
    case Create(contractId, signatories, observers) =>
      Shrink
        .shrinkTuple2(shrinkPartySet, shrinkPartySet)
        .shrink((signatories, observers))
        .map { case (shrunkenSignatories, shrunkenObservers) =>
          Create(contractId, shrunkenSignatories, shrunkenObservers)
        }
    case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
      Shrink
        .shrinkTuple3(shrinkPartySet, shrinkPartySet, shrinkPartySet)
        .shrink((maintainers, signatories, observers))
        .map { case (shrunkenMaintainers, shrunkenSignatories, shrunkenObservers) =>
          CreateWithKey(
            contractId,
            keyId,
            shrunkenMaintainers,
            shrunkenSignatories,
            shrunkenObservers,
          )
        }
        .lazyAppendedAll(
          List(
            Create(contractId, signatories, observers)
          )
        )
    case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      Shrink
        .shrinkTuple5(
          shrinkExerciseKind,
          shrinkContractId,
          shrinkPartySet,
          shrinkPartySet,
          shrinkTransaction,
        )
        .shrink((kind, contractId, controllers, choiceObservers, subTransaction))
        .map((Exercise.apply _).tupled)
        .lazyAppendedAll(subTransaction)
    case ExerciseByKey(
          kind,
          contractId,
          keyId,
          maintainers,
          controllers,
          choiceObservers,
          subTransaction,
        ) =>
      Shrink
        .shrinkTuple7(
          shrinkExerciseKind,
          shrinkContractId,
          shrinkKeyId,
          shrinkPartySet,
          shrinkPartySet,
          shrinkPartySet,
          shrinkTransaction,
        )
        .shrink(
          (kind, contractId, keyId, maintainers, controllers, choiceObservers, subTransaction)
        )
        .map((ExerciseByKey.apply _).tupled)
        .lazyAppendedAll(subTransaction)
        .lazyAppendedAll(
          List(
            Exercise(
              kind,
              contractId,
              controllers,
              choiceObservers,
              subTransaction,
            )
          )
        )
    case Fetch(contractId) =>
      shrinkContractId
        .shrink(contractId)
        .map(Fetch)
    case LookupByKey(contractId, keyId, maintainers) =>
      Shrink
        .shrinkTuple3(
          Shrink.shrinkOption(shrinkContractId),
          shrinkKeyId,
          shrinkPartySet,
        )
        .shrink((contractId, keyId, maintainers))
        .map((LookupByKey.apply _).tupled)
    case FetchByKey(contractId, keyId, maintainers) =>
      Shrink
        .shrinkTuple3(
          shrinkContractId,
          shrinkKeyId,
          shrinkPartySet,
        )
        .shrink((contractId, keyId, maintainers))
        .map((FetchByKey.apply _).tupled)
    case Rollback(subTransaction) =>
      shrinkTransaction
        .suchThat(as => as.nonEmpty && as.forall(isNoRollback))
        .shrink(subTransaction)
        .map(Rollback)
        .lazyAppendedAll(subTransaction)
  }

  private def isToplevelAction(action: Action): Boolean = action match {
    case _: Create | _: CreateWithKey | _: Exercise | _: ExerciseByKey => true
    case _ => false
  }

  lazy val shrinkCommands: Shrink[Commands] = Shrink {
    case Commands(participantId, actAs, disclosures, actions) =>
      Shrink
        .shrinkTuple4(
          shrinkParticipantId,
          shrinkPartySet,
          shrinkContractIdSet,
          shrinkTransaction.suchThat(as => as.nonEmpty && as.forall(isToplevelAction)),
        )
        .shrink((participantId, actAs, disclosures, actions))
        .map((Commands.apply _).tupled)
  }

  lazy val shrinkLedger: Shrink[Ledger] =
    Shrink.shrinkContainer[List, Commands](implicitly, shrinkCommands, implicitly)

  lazy val shrinkParticipant: Shrink[Participant] = Shrink {
    case Participant(participantId, parties) =>
      shrinkPartySet.shrink(parties).map(Participant(participantId, _))
  }

  lazy val shrinkTopology: Shrink[Topology] =
    Shrink.shrinkContainer[Seq, Participant](implicitly, shrinkParticipant, implicitly)

  lazy val shrinkScenario: Shrink[Scenario] = Shrink[Scenario] { case Scenario(topology, ledger) =>
    Shrink
      .shrinkTuple2(shrinkTopology, shrinkLedger)
      .shrink((topology, ledger))
      .map((Scenario.apply _).tupled)
  }.suchThat(validScenario)
}
