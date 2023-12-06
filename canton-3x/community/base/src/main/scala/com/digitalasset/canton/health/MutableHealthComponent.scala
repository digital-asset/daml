// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

/** A [[CloseableHealthQuasiComponent]] that derives its state from an optional mutable [[HealthQuasiComponent]].
  * Use when the health component is not instantiated at bootstrap time and/or changes during the lifetime.
  *
  * Must be closed separately.
  *
  * @param uninitializedName  name used to identify this component while it has not yet been initialized
  * @param initialHealthState state the component will return while it has not yet been initialized
  */
class MutableHealthQuasiComponent[H <: HealthQuasiComponent](
    override protected val loggerFactory: NamedLoggerFactory,
    uninitializedName: String,
    override protected val initialHealthState: H#State,
    override protected val timeouts: ProcessingTimeout,
    initialClosingState: H#State,
) extends CloseableHealthQuasiComponent
    with CompositeHealthElement[Unit, H]
    with NamedLogging {
  override type State = H#State

  private def currentDelegate: Option[H] = getDependencies.get(())

  override def name: String =
    currentDelegate.map(_.name).getOrElse(uninitializedName)

  override def closingState: State =
    currentDelegate.map(_.closingState).getOrElse(initialClosingState)

  def set(element: H): Unit =
    alterDependencies(remove = Set.empty, add = Map(() -> element))

  override protected def prettyState: Pretty[H#State] = implicitly[Pretty[H#State]]

  override protected def combineDependentStates: State =
    currentDelegate.map(_.getState).getOrElse(initialHealthState)
}

/** Refines a [[MutableHealthQuasiComponent]] state to [[ComponentHealthState]]
  */
final class MutableHealthComponent(
    loggerFactory: NamedLoggerFactory,
    uninitializedName: String,
    timeouts: ProcessingTimeout,
    shutdownState: ComponentHealthState,
) extends MutableHealthQuasiComponent[HealthComponent](
      loggerFactory,
      uninitializedName,
      ComponentHealthState.NotInitializedState,
      timeouts,
      shutdownState,
    )
    with HealthComponent

object MutableHealthComponent {
  def apply(
      loggerFactory: NamedLoggerFactory,
      uninitializedName: String,
      timeouts: ProcessingTimeout,
      shutdownState: ComponentHealthState = ComponentHealthState.ShutdownState,
  ): MutableHealthComponent =
    new MutableHealthComponent(loggerFactory, uninitializedName, timeouts, shutdownState)
}

/** A health component that aggregates the health state of multiple mutable dependencies using `reduceState`
  * into a single health state.
  *
  * @param reduceState Computes the aggregate health state of the component given the health states of all dependencies.
  *                    When given the empty map, produces the initial state.
  */
abstract class DelegatingMutableHealthQuasiComponent[Id, H <: HealthQuasiComponent](
    override protected val loggerFactory: NamedLoggerFactory,
    override val name: String,
    override protected val timeouts: ProcessingTimeout,
    private val reduceState: Map[Id, H#State] => H#State,
) extends CloseableHealthQuasiComponent
    with CompositeHealthElement[Id, H]
    with NamedLogging {
  override type State = H#State

  override protected def initialHealthState: H#State = reduceState(Map.empty)

  override protected def combineDependentStates: State =
    reduceState(getDependencies.fmap(_.getState))

  def set(id: Id, element: H): Unit = setDependency(id, element)
}

final class DelegatingMutableHealthComponent[Id](
    override val loggerFactory: NamedLoggerFactory,
    name: String,
    override val timeouts: ProcessingTimeout,
    reduceStateFromMany: Map[Id, ComponentHealthState] => ComponentHealthState,
    override val closingState: ComponentHealthState,
) extends DelegatingMutableHealthQuasiComponent[Id, HealthComponent](
      loggerFactory,
      name,
      timeouts,
      reduceStateFromMany,
    )
    with CloseableHealthComponent
