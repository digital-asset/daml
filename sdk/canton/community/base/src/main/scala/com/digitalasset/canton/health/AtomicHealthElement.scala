// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.Eval
import com.daml.error.BaseError
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.tracing.TraceContext

/** Trait for [[HealthElement]] at the leaves of the health dependency tree.
  * Maintains its own state instead of aggregating other components.
  *
  * @see CompositeHealthElement for the aggregating counterpart
  */
trait AtomicHealthElement extends HealthElement {

  /** Sets the state of this component and notifies its listeners */
  def reportHealthState(state: State)(implicit tc: TraceContext): Unit =
    refreshState(Eval.now(state))
}

/** An [[AtomicHealthElement]] whose state is a [[ComponentHealthState]] */
trait AtomicHealthComponent extends AtomicHealthElement with HealthComponent {

  /** Set the health state to Ok and if the previous state was unhealthy, log a message to inform about the resolution
    * of the ongoing issue.
    */
  def resolveUnhealthy()(implicit traceContext: TraceContext): Unit =
    reportHealthState(ComponentHealthState.Ok())

  /** Report that the component is now degraded.
    * Note that this will override the component state, even if it is currently failed!
    */
  def degradationOccurred(error: BaseError)(implicit tc: TraceContext): Unit =
    reportHealthState(ComponentHealthState.Degraded(UnhealthyState(None, Some(error))))

  /** Report that the component is now failed
    */
  def failureOccurred(error: BaseError)(implicit tc: TraceContext): Unit =
    reportHealthState(ComponentHealthState.Failed(UnhealthyState(None, Some(error))))

  /** Report that the component is now degraded.
    * Note that this will override the component state, even if it is currently failed!
    */
  def degradationOccurred(error: String)(implicit tc: TraceContext): Unit =
    reportHealthState(ComponentHealthState.degraded(error))

  /** Report that the component is now failed
    */
  def failureOccurred(error: String)(implicit tc: TraceContext): Unit =
    reportHealthState(ComponentHealthState.failed(error))
}

trait CloseableAtomicHealthComponent extends CloseableHealthComponent with AtomicHealthComponent
