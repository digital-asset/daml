// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.Eval
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.lifecycle.{RunOnShutdown, UnlessShutdown}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap

/** Defines a [[HealthElement]] that merely aggregates the state of other (dependent) [[HealthElement]]s.
  * The dependencies need not be reported anywhere.
  *
  * If you need to manage a state separately for this component,
  * add a dedicated dependency on [[AtomicHealthElement]].
  *
  * @tparam ID The identifier type for the dependent health elements.
  */
trait CompositeHealthElement[ID, HE <: HealthElement] extends HealthElement {

  /** Fetch the current states from the relevant dependencies
    * and combine them into the new state to report for this element.
    */
  protected def combineDependentStates: State

  private val dependencies: TrieMap[ID, HE] = TrieMap.empty[ID, HE]
  private val dependencyListener: HealthListener = new HealthListener {
    override def name: String = CompositeHealthElement.this.name

    override def poke()(implicit traceContext: TraceContext): Unit =
      refreshState(Eval.always(combineDependentStates))
  }

  // Unregister all dependencies when this element is closed.
  locally {
    import TraceContext.Implicits.Empty.*
    associatedFlagCloseable.runOnShutdown_(new RunOnShutdown {
      override def name: String = s"unregister-$name-from-dependencies"
      override def done: Boolean = false
      override def run(): Unit = {
        dependencies.foreachEntry((_, element) =>
          element.unregisterOnHealthChange(dependencyListener).discard[Boolean]
        )
      }
    })
  }

  protected def getDependencies: Map[ID, HE] = dependencies.readOnlySnapshot().toMap

  protected def setDependency(id: ID, dependency: HE): Unit =
    alterDependencies(add = Map(id -> dependency), remove = Set.empty)

  protected def removeDependency(id: ID): Unit =
    alterDependencies(add = Map.empty, remove = Set(id))

  /** First removes all dependencies in `remove`, then adds all those in `add`.
    * If an `ID` appears in `remove` and `add`, then the `ID` is replaced.
    * Refreshes the state if any of the dependencies was changed.
    *
    * Updates of `dependencies` are not atomic: If this method is called concurrently
    * multiple times, the resulting dependencies may not correspond to a serializable execution.
    *
    * If an dependency triggers a concurrent state refresh, then the state refresh may see
    * an inconsistent set of dependencies and therefore derive an inconsistent state.
    * This however is only temporary as in this case another state refresh will be triggered at the end.
    */
  protected def alterDependencies(remove: Set[ID], add: Map[ID, HE]): Unit = {
    import TraceContext.Implicits.Empty.*
    def removeId(id: ID): Boolean =
      if (add.contains(id)) false
      else
        dependencies.remove(id) match {
          case None => false
          case Some(removed) =>
            removed.unregisterOnHealthChange(dependencyListener).discard[Boolean]
            true
        }

    def addOrReplace(id: ID, dependency: HE): Boolean =
      dependencies.put(id, dependency) match {
        case Some(`dependency`) => false
        case other =>
          other.foreach(_.unregisterOnHealthChange(dependencyListener).discard[Boolean])
          dependency.registerOnHealthChange(dependencyListener).discard[Boolean]
          true
      }

    associatedFlagCloseable
      .performUnlessClosing("alter dependencies") {
        val removedAtLeastOne = remove.map(removeId).exists(Predef.identity)
        val addedAtLeastOne =
          add.map { case (id, dependency) => addOrReplace(id, dependency) }.exists(Predef.identity)
        if (addedAtLeastOne || removedAtLeastOne) dependencyListener.poke()(TraceContext.empty)
      }
      .discard[UnlessShutdown[Unit]]
  }
}
