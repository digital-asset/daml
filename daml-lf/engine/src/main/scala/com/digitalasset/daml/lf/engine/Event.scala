// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.{ChoiceName, Identifier, Party}
import com.daml.lf.transaction.Node._
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.data.Relation.Relation
import com.daml.lf.value.Value

import scala.annotation.tailrec

// --------------------------
// Emitted events for the API
// --------------------------

sealed trait Event[+Nid, +Cid]
    extends value.CidContainer[Event[Nid, Cid]]
    with Product
    with Serializable {
  def witnesses: Set[Party]

  final override protected def self: this.type = this

  final def mapNodeId[Nid2](f: Nid => Nid2): Event[Nid2, Cid] =
    Event.map2(f, identity[Cid])(this)

  final def foreach2(fNid: Nid => Unit, fCid: Cid => Unit): Unit =
    Event.foreach2(fNid, fCid)(this)
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
final case class CreateEvent[Cid](
    contractId: Cid,
    templateId: Identifier,
    contractKey: Option[KeyWithMaintainers[Value[Cid]]],
    argument: Value[Cid],
    agreementText: String,
    signatories: Set[Party],
    observers: Set[Party],
    witnesses: Set[Party])
    extends Event[Nothing, Cid] {

  /**
    * Note that the stakeholders of each event node will always be a subset of the event witnesses. We perform this
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
  *  @param stakeholders the stakeholders of the target contract -- must be a subset of witnesses. see comment for `collectEvents`
  *  @param witnesses additional witnesses induced by parent exercises
  *  @param exerciseResult result of exercise of the choice. Optional since this feature was introduced in transaction version 6.
  */
final case class ExerciseEvent[Nid, Cid](
    contractId: Cid,
    templateId: Identifier,
    choice: ChoiceName,
    choiceArgument: Value[Cid],
    actingParties: Set[Party],
    isConsuming: Boolean,
    children: ImmArray[Nid],
    stakeholders: Set[Party],
    witnesses: Set[Party],
    exerciseResult: Option[Value[Cid]],
) extends Event[Nid, Cid]

object Event extends value.CidContainer2[Event] {

  override private[lf] def map2[Nid, Cid, Nid2, Cid2](
      f1: Nid => Nid2,
      f2: Cid => Cid2,
  ): Event[Nid, Cid] => Event[Nid2, Cid2] = {
    case CreateEvent(
        contractId,
        templateId,
        contractKey,
        argument,
        agreementText,
        signatories,
        observers,
        witnesses,
        ) =>
      CreateEvent(
        contractId = f2(contractId),
        templateId = templateId,
        contractKey = contractKey.map(KeyWithMaintainers.map1(Value.map1(f2))),
        argument = Value.map1(f2)(argument),
        agreementText = agreementText,
        signatories = signatories,
        observers = observers,
        witnesses = witnesses,
      )

    case ExerciseEvent(
        contractId,
        templateId,
        choice,
        choiceArgument,
        actingParties,
        isConsuming,
        children,
        stakeholders,
        witnesses,
        exerciseResult,
        ) =>
      ExerciseEvent(
        contractId = f2(contractId),
        templateId = templateId,
        choice = choice,
        choiceArgument = Value.map1(f2)(choiceArgument),
        actingParties = actingParties,
        isConsuming = isConsuming,
        children = children.map(f1),
        stakeholders = stakeholders,
        witnesses = witnesses,
        exerciseResult = exerciseResult.map(Value.map1(f2)),
      )
  }

  override private[lf] def foreach2[A, B](
      f1: A => Unit,
      f2: B => Unit,
  ): Event[A, B] => Unit = {
    case CreateEvent(
        contractId,
        templateId @ _,
        contractKey,
        argument,
        agreementText @ _,
        signatories @ _,
        observers @ _,
        witnesses @ _,
        ) =>
      f2(contractId)
      contractKey.foreach(KeyWithMaintainers.foreach1(Value.foreach1(f2)))
      Value.foreach1(f2)(argument)

    case ExerciseEvent(
        contractId,
        templateId @ _,
        choice @ _,
        choiceArgument,
        actingParties @ _,
        isConsuming @ _,
        children,
        stakeholders @ _,
        witnesses @ _,
        exerciseResult,
        ) =>
      f2(contractId)
      Value.map1(f2)(choiceArgument)
      children.foreach(f1)
      exerciseResult.foreach(Value.foreach1(f2))
  }

  case class Events[Nid, Cid](roots: ImmArray[Nid], events: Map[Nid, Event[Nid, Cid]]) {
    // filters from the leaves upwards: if any any exercise node returns false all its children will be purged, too
    def filter(f: Event[Nid, Cid] => Boolean): Events[Nid, Cid] = {
      val liveEvts = scala.collection.mutable.Map[Nid, Event[Nid, Cid]]()
      def go(evtids: ImmArray[Nid]): Unit = {
        evtids.foreach((evtid: Nid) => {
          val evt = events(evtid)
          evt match {
            case ce: CreateEvent[Cid] =>
              if (f(ce)) {
                liveEvts += (evtid -> ce)
              }
            case ee: ExerciseEvent[Nid, Cid] =>
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

    /** The function must be injective */
    def mapNodeId[Nid2](f: Nid => Nid2): Events[Nid2, Cid] =
      Events(roots.map(f), events.map { case (nid, evt) => (f(nid), evt.mapNodeId(f)) })
  }

  /** Use Blinding to get the blinding which will contain the disclosure
    */
  def collectEvents[Nid, Cid](
      tx: GenTransaction[Nid, Cid],
      disclosure: Relation[Nid, Party]): Events[Nid, Cid] = {
    val evts =
      scala.collection.mutable.Map[Nid, Event[Nid, Cid]]()

    def isIrrelevantNode(nid: Nid): Boolean = tx.nodes(nid) match {
      case _: NodeFetch[_] => true
      case _: NodeLookupByKey[_] => true
      case _ => false

    }

    @tailrec
    def go(remaining: FrontStack[Nid]): Unit = {
      remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, remaining) =>
          val node = tx.nodes(nodeId)
          node match {
            case nc: NodeCreate[Cid] =>
              val evt =
                CreateEvent(
                  contractId = nc.coid,
                  templateId = nc.coinst.template,
                  contractKey = nc.key,
                  argument = nc.coinst.arg,
                  agreementText = nc.coinst.agreementText,
                  signatories = nc.signatories,
                  observers = nc.stakeholders diff nc.signatories,
                  witnesses = disclosure(nodeId)
                )
              evts += (nodeId -> evt)
              go(remaining)
            case ne: NodeExercises[Nid, Cid] =>
              val templateId = ne.templateId
              // purge fetch children -- we do not have fetch events
              val relevantChildren =
                ne.children.filter(!isIrrelevantNode(_))
              val evt = ExerciseEvent(
                ne.targetCoid,
                templateId,
                ne.choiceId,
                ne.chosenValue,
                ne.actingParties,
                ne.consuming,
                relevantChildren,
                ne.stakeholders,
                disclosure(nodeId),
                ne.exerciseResult
              )
              evts += (nodeId -> evt)
              go(relevantChildren ++: remaining)
            case nf: NodeFetch[Cid] =>
              throw new RuntimeException(
                s"Unexpected fetch node $nf, we purge them before we get here!")
            case nlbk: NodeLookupByKey[Cid] =>
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

  object Events extends value.CidContainer2[Events] {
    override private[lf] def map2[Nid, Cid, Nid2, Cid2](
        f1: Nid => Nid2,
        f2: Cid => Cid2,
    ): Events[Nid, Cid] => Events[Nid2, Cid2] = {
      case Events(roots, events) =>
        Events(roots.map(f1), events.map {
          case (id, event) => f1(id) -> Event.map2(f1, f2)(event)
        })
    }

    override private[lf] def foreach2[A, B](
        f1: A => Unit,
        f2: B => Unit,
    ): Events[A, B] => Unit = {
      case Events(roots, events) =>
        roots.foreach(f1)
        events.foreach {
          case (id, event) => f1(id) -> Event.foreach2(f1, f2)(event)
        }
    }
  }

}
