// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.{ChoiceName, Identifier, Party}
import com.daml.lf.transaction.Node._
import com.daml.lf.data.{FrontStack, FrontStackCons, ImmArray}
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.data.Relation.Relation

import scala.annotation.tailrec

// --------------------------
// Emitted events for the API
// --------------------------

sealed trait Event[+Nid, +Cid, +Val]
    extends value.CidContainer[Event[Nid, Cid, Val]]
    with Product
    with Serializable {
  def witnesses: Set[Party]

  final override protected val self: this.type = this

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  final def mapContractId[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): Event[Nid, Cid2, Val2] =
    Event.map3(identity[Nid], f, g)(this)
  final def mapNodeId[Nid2](f: Nid => Nid2): Event[Nid2, Cid, Val] =
    Event.map3(f, identity[Cid], identity[Val])(this)

  final def foreach3(fNid: Nid => Unit, fCid: Cid => Unit, fVal: Val => Unit): Unit =
    Event.foreach3(fNid, fCid, fVal)(self)
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
final case class CreateEvent[Cid, Val](
    contractId: Cid,
    templateId: Identifier,
    contractKey: Option[KeyWithMaintainers[Val]],
    argument: Val,
    agreementText: String,
    signatories: Set[Party],
    observers: Set[Party],
    witnesses: Set[Party])
    extends Event[Nothing, Cid, Val] {

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
final case class ExerciseEvent[Nid, Cid, Val](
    contractId: Cid,
    templateId: Identifier,
    choice: ChoiceName,
    choiceArgument: Val,
    actingParties: Set[Party],
    isConsuming: Boolean,
    children: ImmArray[Nid],
    stakeholders: Set[Party],
    witnesses: Set[Party],
    exerciseResult: Option[Val])
    extends Event[Nid, Cid, Val]

object Event extends value.CidContainer3WithDefaultCidResolver[Event] {

  override private[lf] def map3[Nid, Cid, Val, Nid2, Cid2, Val2](
      f1: Nid => Nid2,
      f2: Cid => Cid2,
      f3: Val => Val2
  ): Event[Nid, Cid, Val] => Event[Nid2, Cid2, Val2] = {
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
        contractKey = contractKey.map(KeyWithMaintainers.map1(f3)),
        argument = f3(argument),
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
        choiceArgument = f3(choiceArgument),
        actingParties = actingParties,
        isConsuming = isConsuming,
        children = children.map(f1),
        stakeholders = stakeholders,
        witnesses = witnesses,
        exerciseResult = exerciseResult.map(f3),
      )
  }

  override private[lf] def foreach3[A, B, C](
      f1: A => Unit,
      f2: B => Unit,
      f3: C => Unit,
  ): Event[A, B, C] => Unit = {
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
      contractKey.foreach(KeyWithMaintainers.foreach1(f3))
      f3(argument)

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
      f3(choiceArgument)
      children.foreach(f1)
      exerciseResult.foreach(f3)
  }

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

    @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
    def mapContractIdAndValue[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): Events[Nid, Cid2, Val2] =
      // do NOT use `Map#mapValues`! it applies the function lazily on lookup. see #1861
      copy(events = events.transform { (_, value) =>
        value.mapContractId(f, g)
      })

    /** The function must be injective */
    def mapNodeId[Nid2](f: Nid => Nid2): Events[Nid2, Cid, Val] =
      Events(roots.map(f), events.map { case (nid, evt) => (f(nid), evt.mapNodeId(f)) })
  }

  /** Use Blinding to get the blinding which will contain the disclosure
    */
  def collectEvents[Nid, Cid, Val](
      tx: GenTransaction[Nid, Cid, Val],
      disclosure: Relation[Nid, Party]): Events[Nid, Cid, Val] = {
    val evts =
      scala.collection.mutable.Map[Nid, Event[Nid, Cid, Val]]()

    def isIrrelevantNode(nid: Nid): Boolean = tx.nodes(nid) match {
      case _: NodeFetch[_, _] => true
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
            case ne: NodeExercises[Nid, Cid, Val] =>
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
            case nf: NodeFetch[Cid, Val] =>
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

  object Events extends value.CidContainer3WithDefaultCidResolver[Events] {
    override private[lf] def map3[Nid, Cid, Val, Nid2, Cid2, Val2](
        f1: Nid => Nid2,
        f2: Cid => Cid2,
        f3: Val => Val2,
    ): Events[Nid, Cid, Val] => Events[Nid2, Cid2, Val2] = {
      case Events(roots, events) =>
        Events(roots.map(f1), events.map {
          case (id, event) => f1(id) -> Event.map3(f1, f2, f3)(event)
        })
    }

    override private[lf] def foreach3[A, B, C](
        f1: A => Unit,
        f2: B => Unit,
        f3: C => Unit,
    ): Events[A, B, C] => Unit = {
      case Events(roots, events) =>
        roots.foreach(f1)
        events.foreach {
          case (id, event) => f1(id) -> Event.foreach3(f1, f2, f3)(event)
        }
    }
  }

}
