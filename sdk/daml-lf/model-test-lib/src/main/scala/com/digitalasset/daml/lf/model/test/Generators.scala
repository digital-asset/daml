// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import org.scalacheck.Gen
import com.daml.lf.model.test.Ledgers._
import GenInstances._
import cats.implicits.toTraverseOps

class Generators(numParticipants: Int, numPackages: Int, numParties: Int) {

  private def numberCreates(ledger: Ledger): Ledger = {
    var lastContractId: ContractId = 0

    def numberCommandsCreates(commands: Commands): Commands =
      commands.copy(commands = commands.commands.map(numberCommandCreates))

    def numberCommandCreates(command: Command): Command =
      command.copy(action = numberActionCreates(command.action))

    def numberTransactionCreates(transaction: Transaction): Transaction =
      transaction.map(numberActionCreates)

    def numberActionCreates(action: Action): Action = action match {
      case c: Create =>
        lastContractId += 1
        c.copy(contractId = lastContractId)
      case c: CreateWithKey =>
        lastContractId += 1
        c.copy(contractId = lastContractId)
      case e: Exercise =>
        e.copy(subTransaction = numberTransactionCreates(e.subTransaction))
      case e: ExerciseByKey =>
        e.copy(subTransaction = numberTransactionCreates(e.subTransaction))
      case f: Fetch =>
        f
      case f: FetchByKey =>
        f
      case l: LookupByKey =>
        l
      case r: Rollback =>
        r.copy(subTransaction = numberTransactionCreates(r.subTransaction))
    }

    ledger.map(numberCommandsCreates)
  }

  lazy val scenarioGen: Gen[Scenario] =
    for {
      topology <- topologyGen
      ledger <- ledgerGen(topology)
    } yield Scenario(topology, ledger)

  // Randomly assigns a participant ID to each party. Retry until each
  // party is hosted by at least one participant.
  lazy val topologyGen: Gen[Topology] =
    (0 until numParticipants).toList
      .traverse(participantId =>
        for {
          packages <- nonEmptyPackageIdSetGen
          parties <- nonEmptyPartySetGen
        } yield Participant(participantId, packages, parties)
      )
      .retryUntil(_.flatMap(_.parties).toSet.size == numParties)

  lazy val partySetGen: Gen[PartySet] =
    Gen.someOf(1 to numParties).map(_.toSet)

  lazy val nonEmptyPartySetGen: Gen[PartySet] =
    Gen.atLeastOne(1 to numParties).map(_.toSet)

  lazy val nonEmptyPackageIdSetGen: Gen[PackageIdSet] =
    Gen.atLeastOne(0 until numPackages).map(_.toSet)

  lazy val contractIdGen: Gen[ContractId] =
    Gen.posNum[ContractId]

  lazy val packageIdGen: Gen[PackageId] =
    Gen.choose(0, numPackages - 1)

  def ledgerGen(topology: Topology): Gen[Ledger] = Gen.sized(size =>
    for {
      listLen <- Gen.choose(0, size)
      res <- Gen.listOfN(listLen, commandsGen(topology))
    } yield numberCreates(res)
  )

  def commandsGen(topology: Topology): Gen[Commands] =
    for {
      participantId <- Gen.choose(0, numParticipants - 1)
      actAs <- Gen.atLeastOne(topology(participantId).parties).map(_.toSet)
      disclosures <- Gen.listOf(contractIdGen).map(_.toSet)
      actions <- commandListGen
    } yield Commands(participantId, actAs, disclosures, actions)

  lazy val commandListGen: Gen[List[Command]] = Gen.sized(size =>
    for {
      listLen <- Gen.choose(1, size)
      res <- Gen.listOfN(
        listLen,
        Gen.lzy(Gen.resize(size / listLen, commandGen)),
      )
    } yield res
  )

  lazy val commandGen: Gen[Command] = for {
    pkgId <- Gen.option(packageIdGen)
    action <- actionGen(NoRollbacksAllowed, NoFetchesAllowed)
  } yield Command(pkgId, action)

  trait RollbacksAllowed
  case object NoRollbacksAllowed extends RollbacksAllowed
  case object SomeRollbacksAllowed extends RollbacksAllowed

  trait FetchesAllowed
  case object NoFetchesAllowed extends FetchesAllowed
  case object SomeFetchesAllowed extends FetchesAllowed

  def transactionGen(
      minNumActions: Int,
      rollbacksAllowed: RollbacksAllowed,
      fetchesAllowed: FetchesAllowed,
  ): Gen[List[Action]] = Gen.sized(size =>
    for {
      listLen <- Gen.choose(minNumActions, size)
      res <- Gen.listOfN(
        listLen,
        Gen.lzy(Gen.resize(size / listLen, actionGen(rollbacksAllowed, fetchesAllowed))),
      )
    } yield res
  )

  def actionGen(rollbacksAllowed: RollbacksAllowed, fetchesAllowed: FetchesAllowed): Gen[Action] = {
    val gens = List(
      Some(createGen),
      Some(exerciseGen),
      Option.when(rollbacksAllowed == SomeRollbacksAllowed)(rollbackGen),
      Option.when(fetchesAllowed == SomeFetchesAllowed)(fetchGen),
    ).flatten
    Gen.choose(0, gens.size - 1).flatMap(gens(_))
  }

  lazy val createGen: Gen[Create] =
    for {
      signatories <- nonEmptyPartySetGen
      observers <- partySetGen
    } yield Create(null.asInstanceOf[Int], signatories, observers)

  lazy val exerciseGen: Gen[Exercise] =
    for {
      kind <- Gen.oneOf(Consuming, NonConsuming)
      contractId <- contractIdGen
      controllers <- nonEmptyPartySetGen
      choiceObservers <- partySetGen
      subTransaction <- transactionGen(0, SomeRollbacksAllowed, SomeFetchesAllowed)
    } yield Exercise(kind, contractId, controllers, choiceObservers, subTransaction)

  lazy val fetchGen: Gen[Fetch] =
    contractIdGen.map(Fetch)

  lazy val rollbackGen: Gen[Rollback] =
    transactionGen(1, NoRollbacksAllowed, SomeFetchesAllowed).map(Rollback)
}
