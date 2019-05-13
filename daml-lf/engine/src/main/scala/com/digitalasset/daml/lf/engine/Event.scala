// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.data.Ref.{DefinitionRef, Party}
import com.digitalasset.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.data.Relation.Relation

import scala.annotation.tailrec

// --------------------------
// Emitted events for the API
// --------------------------

sealed trait Event[+Nid, +Cid, +Val] extends Product with Serializable {
  def witnesses: Set[Party]
  def mapContractId[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): Event[Nid, Cid2, Val2]
  def mapNodeId[Nid2](f: Nid => Nid2): Event[Nid2, Cid, Val]
}

/** Event for created contracts, follows ledger api event protocol
  *
  *  @param contractId id for the contract this event notifies
  *  @param templateId identifier of the creating template
  *  @param argument argument of the contract creation
  *  @param stakeholders the stakeholders of the created contract -- must be a subset of witnesses. see comment for `collectEvents`
  *  @param witnesses additional witnesses induced by parent exercises
  */
final case class CreateEvent[Cid, Val](
    contractId: Cid,
    templateId: DefinitionRef,
    argument: Val,
    stakeholders: Set[Party],
    witnesses: Set[Party])
    extends Event[Nothing, Cid, Val] {
  override def mapContractId[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): CreateEvent[Cid2, Val2] =
    copy(contractId = f(contractId), argument = g(argument))

  override def mapNodeId[Nid2](f: Nothing => Nid2): CreateEvent[Cid, Val] = this
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
  *  @param stakeholders the stakeholders of the target contract -- must be a subset of witnesses. see comment for `collectEvents`
  *  @param witnesses additional witnesses induced by parent exercises
  *  @param exerciseResult result of exercise of the choice. Optional since this feature was introduced in transaction version 6.
  */
final case class ExerciseEvent[Nid, Cid, Val](
    contractId: Cid,
    templateId: DefinitionRef,
    choice: String,
    choiceArgument: Val,
    actingParties: Set[Party],
    isConsuming: Boolean,
    children: ImmArray[Nid],
    stakeholders: Set[Party],
    witnesses: Set[Party],
    exerciseResult: Option[Val])
    extends Event[Nid, Cid, Val] {
  override def mapContractId[Cid2, Val2](
      f: Cid => Cid2,
      g: Val => Val2): ExerciseEvent[Nid, Cid2, Val2] =
    copy(
      contractId = f(contractId),
      choiceArgument = g(choiceArgument),
      exerciseResult = exerciseResult.map(g)
    )

  override def mapNodeId[Nid2](f: Nid => Nid2): ExerciseEvent[Nid2, Cid, Val] =
    copy(children = children.map(f))
}

object Event {
  case class Events[Nid, Cid, Val](roots: ImmArray[Nid], events: Map[Nid, Event[Nid, Cid, Val]]) {
    // filters from the leaves upwards: if any any exercise node returns false all its children will be purged, too
    def filter(f: Event[Nid, Cid, Val] => Boolean): Events[Nid, Cid, Val] = {
      val liveEvts = scala.collection.mutable.Map[Nid, Event[Nid, Cid, Val]]()
      def go(evtids: ImmArray[Nid]): Unit = {
        evtids.foreach((evtid: Nid) => {
          val evt = events(evtid)
          evt match {
            case ce: CreateEvent[Cid, Val] =>
              if (f(ce)) {
                liveEvts += (evtid -> ce)
              }
            case ee: ExerciseEvent[Nid, Cid, Val] =>
              if (f(ee)) {
                go(ee.children)
                liveEvts += (evtid -> ee.copy(children = ee.children.filter(liveEvts.contains)))
              }
          }
        })
      }
      go(roots)

      Events(roots.filter(liveEvts.contains), Map() ++ liveEvts)
    }

    def mapContractIdAndValue[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): Events[Nid, Cid2, Val2] =
      copy(events = events.mapValues(_.mapContractId(f, g)))

    /** The function must be injective */
    def mapNodeId[Nid2](f: Nid => Nid2): Events[Nid2, Cid, Val] =
      Events(roots.map(f), events.map { case (nid, evt) => (f(nid), evt.mapNodeId(f)) })
  }

  /** Use Blinding to get the blinding which will contain the disclosure
    *
    * Note that the stakeholders of each event node will always be a subset of the event witnesses. We perform this
    * narrowing since usually when consuming these events we only care about the parties that were included in the
    * disclosure information. Consumers should be aware that the stakeholders stored are _not_ all the stakeholders of
    * the contract, but just the stakeholders "up to witnesses".
    */
  def collectEvents[Nid, Cid, Val](
      tx: GenTransaction[Nid, Cid, Val],
      disclosure: Relation[Nid, Party]): Events[Nid, Cid, Val] = {
    val evts =
      scala.collection.mutable.Map[Nid, Event[Nid, Cid, Val]]()

    def isIrrelevantNode(nid: Nid): Boolean = tx.nodes(nid) match {
      case _: NodeFetch[Cid] => true
      case _: NodeLookupByKey[_, _] => true
      case _ => false

    }

    @tailrec
    def go(remaining: FrontStack[Nid]): Unit = {
      remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, remaining) =>
          val node = tx.nodes(nodeId)
          node match {
            case nc: NodeCreate[Cid, Val] =>
              val templateId = nc.coinst.template
              val stakeholders = nc.stakeholders
              val evt =
                CreateEvent(
                  nc.coid,
                  templateId,
                  nc.coinst.arg,
                  stakeholders intersect disclosure(nodeId),
                  disclosure(nodeId))
              evts += (nodeId -> evt)
              go(remaining)
            case ne: NodeExercises[Nid, Cid, Val] =>
              val templateId = ne.templateId
              // purge fetch children -- we do not have fetch events
              val relevantChildren =
                ne.children.filter(!isIrrelevantNode(_))
              val stakeholders = ne.stakeholders
              val evt = ExerciseEvent(
                ne.targetCoid,
                templateId,
                ne.choiceId,
                ne.chosenValue,
                ne.actingParties,
                ne.consuming,
                relevantChildren,
                stakeholders intersect disclosure(nodeId),
                disclosure(nodeId),
                ne.exerciseResult
              )
              evts += (nodeId -> evt)
              go(relevantChildren ++: remaining)
            case nf: NodeFetch[Cid] =>
              throw new RuntimeException(
                s"Unexpected fetch node $nf, we purge them before we get here!")
            case nlbk: NodeLookupByKey[Cid, Val] =>
              throw new RuntimeException(
                s"Unexpected lookup by key node $nlbk, we purge them before we get here!"
              )
          }
      }
    }

    // purge fetch children -- we do not have fetch events
    val relevantRoots = tx.roots.filter(!isIrrelevantNode(_))
    go(FrontStack(relevantRoots))
    Events(relevantRoots, Map() ++ evts)
  }
}
