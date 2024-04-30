// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.{Ledgers => L, Symbolic => S}
import com.microsoft.z3.enumerations.Z3_lbool
import com.microsoft.z3.{Context, IntNum, Model}
import LedgerImplicits._

class FromSymbolic(numParties: Int, ctx: Context, model: Model) {

  private def evalParticipantId(pid: S.ParticipantId): L.ParticipantId =
    model.evaluate(pid, false).asInstanceOf[IntNum].getInt

  private def evalContractId(cid: S.ContractId): L.ContractId =
    model.evaluate(cid, false).asInstanceOf[IntNum].getInt

  private def evalKeyId(cid: S.KeyId): L.KeyId =
    model.evaluate(cid, true).asInstanceOf[IntNum].getInt

  private def evalPackageId(pid: S.PackageId): L.PackageId =
    model.evaluate(pid, false).asInstanceOf[IntNum].getInt

  private def evalPartySet(set: S.PartySet): L.PartySet =
    (1 to numParties).toSet.filter { i =>
      model
        .evaluate(ctx.mkSelect(set, ctx.mkInt(i)), false)
        .getBoolValue == Z3_lbool.Z3_L_TRUE
    }

  private def evalContractIdSet(numContracts: Int, set: S.ContractIdSet): L.ContractIdSet =
    (0 until numContracts).toSet.filter { i =>
      model
        .evaluate(ctx.mkSelect(set, ctx.mkInt(i)), false)
        .getBoolValue == Z3_lbool.Z3_L_TRUE
    }

  private def toConcrete(numContracts: Int, commands: S.Commands): L.Commands =
    L.Commands(
      evalParticipantId(commands.participantId),
      evalPartySet(commands.actAs),
      evalContractIdSet(numContracts, commands.disclosures),
      commands.commands.map(toConcrete),
    )

  private def toConcrete(command: S.Command): L.Command =
    L.Command(command.packageId.map(evalPackageId), toConcrete(command.action))

  private def toConcrete(kind: S.ExerciseKind): L.ExerciseKind = {
    kind match {
      case S.Consuming => L.Consuming
      case S.NonConsuming => L.NonConsuming
    }
  }

  private def toConcrete(action: S.Action): L.Action = action match {
    case Symbolic.Create(contractId, signatories, observers) =>
      L.Create(
        evalContractId(contractId),
        evalPartySet(signatories),
        evalPartySet(observers),
      )
    case Symbolic.CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
      L.CreateWithKey(
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
        evalPartySet(signatories),
        evalPartySet(observers),
      )
    case Symbolic.Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      L.Exercise(
        toConcrete(kind),
        evalContractId(contractId),
        evalPartySet(controllers),
        evalPartySet(choiceObservers),
        subTransaction.map(toConcrete),
      )
    case Symbolic.ExerciseByKey(
          kind,
          contractId,
          keyId,
          maintainers,
          controllers,
          choiceObservers,
          subTransaction,
        ) =>
      L.ExerciseByKey(
        toConcrete(kind),
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
        evalPartySet(controllers),
        evalPartySet(choiceObservers),
        subTransaction.map(toConcrete),
      )
    case Symbolic.Fetch(contractId) =>
      L.Fetch(evalContractId(contractId))
    case Symbolic.FetchByKey(contractId, keyId, maintainers) =>
      L.FetchByKey(
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
      )
    case Symbolic.LookupByKey(contractId, keyId, maintainers) =>
      L.LookupByKey(
        contractId.map(evalContractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
      )
    case Symbolic.Rollback(subTransaction) =>
      L.Rollback(subTransaction.map(toConcrete))
  }

  private def toConcrete(ledger: S.Ledger): L.Ledger =
    ledger.map(toConcrete(ledger.numContracts, _))

  private def toConcrete(participant: S.Participant): L.Participant = {
    L.Participant(
      evalParticipantId(participant.participantId),
      evalPartySet(participant.parties),
    )
  }

  def toConcrete(scenario: S.Scenario): L.Scenario = {
    L.Scenario(scenario.topology.map(toConcrete), toConcrete(scenario.ledger))
  }
}
