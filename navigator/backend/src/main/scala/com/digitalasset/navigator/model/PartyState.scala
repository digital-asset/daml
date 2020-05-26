// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import java.util.concurrent.atomic.AtomicReference

import com.daml.lf.{iface => DamlLfIface}
import com.daml.ledger.api.refinements.ApiTypes

import scalaz.Tag

case class State(ledger: Ledger, packageRegistry: PackageRegistry)

/** A DA party and its ledger view(s). */
class PartyState(val name: ApiTypes.Party, val useDatabase: Boolean) {
  private val stateRef: AtomicReference[State] = new AtomicReference(
    State(Ledger(name, None, useDatabase), new PackageRegistry))

  def ledger: Ledger = stateRef.get.ledger
  def packageRegistry: PackageRegistry = stateRef.get.packageRegistry

  def addLatestTransaction(tx: Transaction): Unit = {
    stateRef.updateAndGet(state =>
      state.copy(ledger = state.ledger.withTransaction(tx, packageRegistry)))
    ()
  }

  def addCommand(cmd: Command): Unit = {
    stateRef.updateAndGet(state => state.copy(ledger = state.ledger.withCommand(cmd)))
    ()
  }

  def addCommandStatus(id: ApiTypes.CommandId, status: CommandStatus): Unit = {
    stateRef.updateAndGet(state => state.copy(ledger = state.ledger.withCommandStatus(id, status)))
    ()
  }

  def addPackages(packs: List[DamlLfIface.Interface]): Unit = {
    stateRef.updateAndGet(state =>
      state.copy(packageRegistry = packageRegistry.withPackages(packs)))
    ()
  }

  override def hashCode(): Int = Tag.unwrap(name).hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: PartyState => Tag.unwrap(this.name) equals Tag.unwrap(that.name)
    case _ => false
  }

  override def toString: String = "Party(" + name.toString + ")"

  def contracts(): Stream[Contract] = this.ledger.allContracts(this.packageRegistry)
}
