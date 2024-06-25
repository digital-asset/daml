// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier, Party}
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.GlobalKeyWithMaintainers
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.daml.scalautil.Statement.discard

// --------------------------
// Emitted events for the API
// --------------------------

sealed trait Event extends Product with Serializable {
  def witnesses: Set[Party]
}

/** Event for created contracts, follows ledger api event protocol
  *
  *  @param contractId id for the contract this event notifies
  *  @param templateId identifier of the creating template
  *  @param contractKey key for the contract this event notifies
  *  @param argument argument of the contract creation
  *  @param signatories as defined by the template
  *  @param observers as defined by the template or implicitly as choice controllers
  *  @param witnesses additional witnesses induced by parent exercises
  */
final case class CreateEvent(
    contractId: ContractId,
    templateId: Identifier,
    contractKey: Option[GlobalKeyWithMaintainers],
    argument: Value,
    signatories: Set[Party],
    observers: Set[Party],
    witnesses: Set[Party],
) extends Event {

  /** Note that the stakeholders of each event node will always be a subset of the event witnesses. We perform this
    * narrowing since usually when consuming these events we only care about the parties that were included in the
    * disclosure information. Consumers should be aware that the stakeholders stored are _not_ all the stakeholders of
    * the contract, but just the stakeholders "up to witnesses".
    *
    * For broader and more detailed information, the consumer can use [[signatories]] and/or [[observers]].
    */
  val stakeholders = signatories.union(observers).intersect(witnesses)
}

/** Event for exercises
  *
  *  @param contractId contract id for the target contract
  *  @param templateId identifier of the exercised contract template
  *  @param choice choice exercised
  *  @param choiceArgument arguments given to the choice
  *  @param actingParties parties acting in the exercise
  *  @param isConsuming marks if this exercise archived the target contract
  *  @param children consequence events. note that they're paired with the NodeId of the transaction that originated the event.
  *  @param stakeholders the stakeholders of the target contract -- must be a subset of witnesses.
  *  @param witnesses additional witnesses induced by parent exercises
  *  @param exerciseResult result of exercise of the choice. Optional since this feature was introduced in transaction version 6.
  */
final case class ExerciseEvent(
    contractId: ContractId,
    templateId: Identifier,
    choice: ChoiceName,
    choiceArgument: Value,
    actingParties: Set[Party],
    isConsuming: Boolean,
    children: ImmArray[NodeId],
    stakeholders: Set[Party],
    witnesses: Set[Party],
    exerciseResult: Option[Value],
) extends Event

object Event {
  case class Events(roots: ImmArray[NodeId], events: Map[NodeId, Event]) {
    // filters from the leaves upwards: if any any exercise node returns false all its children will be purged, too
    def filter(f: Event => Boolean): Events = {
      val liveEvts = scala.collection.mutable.Map[NodeId, Event]()
      def go(evtids: ImmArray[NodeId]): Unit = {
        evtids.foreach((evtid: NodeId) => {
          val evt = events(evtid)
          evt match {
            case ce: CreateEvent =>
              if (f(ce)) {
                discard(liveEvts += (evtid -> ce))
              }
            case ee: ExerciseEvent =>
              if (f(ee)) {
                go(ee.children)
                discard(
                  liveEvts += (evtid -> ee.copy(children = ee.children.filter(liveEvts.contains)))
                )
              }
          }
        })
      }
      go(roots)

      Events(roots.filter(liveEvts.contains), Map() ++ liveEvts)
    }
  }
}
