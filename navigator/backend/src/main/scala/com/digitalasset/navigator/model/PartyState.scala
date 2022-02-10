// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

import java.util.concurrent.atomic.AtomicReference
import com.daml.lf.{iface => DamlLfIface}
import com.daml.ledger.api.refinements.ApiTypes

import scala.collection.immutable.LazyList
import scalaz.Tag

import java.net.URLEncoder

case class State(ledger: Ledger, packageRegistry: PackageRegistry)

/** A DA party and its ledger view(s). */
class PartyState(
    val name: ApiTypes.Party,
    // A named role specified in our config. The role is an arbitrary string chosen by the user. It gets passed to the frontend, which can display a custom theme or custom views depending on the role.
    val userRole: Option[String] = None,
    val useDatabase: Boolean = false,
) {
  def actorName: String =
    "party-" + URLEncoder.encode(ApiTypes.Party.unwrap(name), "UTF-8")

  private val stateRef: AtomicReference[State] = new AtomicReference(
    State(Ledger(name, None, useDatabase), new PackageRegistry)
  )

  def ledger: Ledger = stateRef.get.ledger
  def packageRegistry: PackageRegistry = stateRef.get.packageRegistry

  def addLatestTransaction(tx: Transaction): Unit = {
    stateRef.updateAndGet(state =>
      state.copy(ledger = state.ledger.withTransaction(tx, packageRegistry))
    )
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
      state.copy(packageRegistry = packageRegistry.withPackages(packs))
    )
    ()
  }

  override def hashCode(): Int = Tag.unwrap(name).hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: PartyState => Tag.unwrap(this.name) equals Tag.unwrap(that.name)
    case _ => false
  }

  override def toString: String = "Party(" + name.toString + ")"

  def contracts(): LazyList[Contract] = this.ledger.allContracts(this.packageRegistry)
}
