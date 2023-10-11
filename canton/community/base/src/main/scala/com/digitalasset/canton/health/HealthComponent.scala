// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Refines the state of a [[HealthElement]] to [[ToComponentHealthState]] */
trait HealthQuasiComponent extends HealthElement {
  override type State <: ToComponentHealthState & PrettyPrinting
  override protected def prettyState: Pretty[State] = Pretty[State]

  def isFailed: Boolean = getState.toComponentHealthState.isFailed
  def toComponentStatus: ComponentStatus = ComponentStatus(name, getState.toComponentHealthState)

  override def closingState: State
}

/** Refines the state of a [[HealthElement]] to [[ComponentHealthState]] */
trait HealthComponent extends HealthQuasiComponent {
  override type State = ComponentHealthState

  override def closingState: ComponentHealthState = ComponentHealthState.ShutdownState
}
