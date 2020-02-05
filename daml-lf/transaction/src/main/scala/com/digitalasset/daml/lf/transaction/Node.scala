// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{ImmArray, ScalazEqual}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value.{ContractInst, VersionedValue}

import scala.language.higherKinds
import scalaz.Equal
import scalaz.std.option._
import scalaz.syntax.equal._

/**
  * Generic transaction node type for both update transactions and the
  * transaction graph.
  */
object Node {

  /** Transaction nodes parametrized over identifier type */
  sealed trait GenNode[+Nid, +Cid, +Val] extends Product with Serializable with NodeInfo {
    def mapContractIdAndValue[Cid2, Val2](f: Cid => Cid2, g: Val => Val2): GenNode[Nid, Cid2, Val2]
    def mapNodeId[Nid2](f: Nid => Nid2): GenNode[Nid2, Cid, Val]
  }

  object GenNode extends WithTxValue3[GenNode]

  /** A transaction node that can't possibly refer to `Nid`s. */
  sealed trait LeafOnlyNode[+Cid, +Val] extends GenNode[Nothing, Cid, Val]

  object LeafOnlyNode extends WithTxValue2[LeafOnlyNode]

  /** Denotes the creation of a contract instance. */
  final case class NodeCreate[+Cid, +Val](
      nodeSeed: Option[crypto.Hash],
      coid: Cid,
      coinst: ContractInst[Val],
      optLocation: Option[Location], // Optional location of the create expression
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[KeyWithMaintainers[Val]],
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.Create {
    override def mapContractIdAndValue[Cid2, Val2](
        f: Cid => Cid2,
        g: Val => Val2,
    ): NodeCreate[Cid2, Val2] =
      copy(coid = f(coid), coinst = coinst.mapValue(g), key = key.map(_.mapValue(g)))

    override def mapNodeId[Nid2](f: Nothing => Nid2): NodeCreate[Cid, Val] = this

  }

  object NodeCreate extends WithTxValue2[NodeCreate]

  /** Denotes that the contract identifier `coid` needs to be active for the transaction to be valid. */
  final case class NodeFetch[+Cid](
      coid: Cid,
      templateId: Identifier,
      optLocation: Option[Location], // Optional location of the fetch expression
      actingParties: Option[Set[Party]],
      signatories: Set[Party],
      stakeholders: Set[Party],
  ) extends LeafOnlyNode[Cid, Nothing]
      with NodeInfo.Fetch {
    override def mapContractIdAndValue[Cid2, Val2](
        f: Cid => Cid2,
        g: Nothing => Val2,
    ): NodeFetch[Cid2] =
      copy(coid = f(coid))

    override def mapNodeId[Nid2](f: Nothing => Nid2): NodeFetch[Cid] = this

  }

  /** Denotes a transaction node for an exercise.
    * We remember the `children` of this `NodeExercises`
    * to allow segregating the graph afterwards into party-specific
    * ledgers.
    */
  final case class NodeExercises[+Nid, +Cid, +Val](
      nodeSeed: Option[crypto.Hash],
      targetCoid: Cid,
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location], // Optional location of the exercise expression
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Val,
      stakeholders: Set[Party],
      signatories: Set[Party],
      /** Note that here we have both actors and controllers because we use this
        * data structure _before_ we validate the transaction. However for every
        * valid transaction the actors must be the same as the controllers. This
        * is why we removed the controllers field in transaction version 6.
        */
      controllers: Set[Party],
      children: ImmArray[Nid],
      exerciseResult: Option[Val],
      key: Option[KeyWithMaintainers[Val]],
  ) extends GenNode[Nid, Cid, Val]
      with NodeInfo.Exercise {
    override def mapContractIdAndValue[Cid2, Val2](
        f: Cid => Cid2,
        g: Val => Val2,
    ): NodeExercises[Nid, Cid2, Val2] =
      copy(
        targetCoid = f(targetCoid),
        chosenValue = g(chosenValue),
        exerciseResult = exerciseResult.map(g),
        key = key.map(_.mapValue(g)),
      )

    override def mapNodeId[Nid2](f: Nid => Nid2): NodeExercises[Nid2, Cid, Val] =
      copy(
        children = children.map(f),
      )
  }

  object NodeExercises extends WithTxValue3[NodeExercises] {

    /** After interpretation authorization, it must be the case that
      * the controllers are the same as the acting parties. This
      * apply method enforces it.
      */
    def apply[Nid, Cid, Val](
        nodeSeed: Option[crypto.Hash] = None,
        targetCoid: Cid,
        templateId: Identifier,
        choiceId: ChoiceName,
        optLocation: Option[Location],
        consuming: Boolean,
        actingParties: Set[Party],
        chosenValue: Val,
        stakeholders: Set[Party],
        signatories: Set[Party],
        children: ImmArray[Nid],
        exerciseResult: Option[Val],
        key: Option[KeyWithMaintainers[Val]],
    ): NodeExercises[Nid, Cid, Val] =
      NodeExercises(
        nodeSeed,
        targetCoid,
        templateId,
        choiceId,
        optLocation,
        consuming,
        actingParties,
        chosenValue,
        stakeholders,
        signatories,
        actingParties,
        children,
        exerciseResult,
        key,
      )
  }

  final case class NodeLookupByKey[+Cid, +Val](
      templateId: Identifier,
      optLocation: Option[Location],
      key: KeyWithMaintainers[Val],
      result: Option[Cid],
  ) extends LeafOnlyNode[Cid, Val]
      with NodeInfo.LookupByKey {
    override def mapContractIdAndValue[Cid2, Val2](
        f: Cid => Cid2,
        g: Val => Val2,
    ): NodeLookupByKey[Cid2, Val2] =
      copy(result = result.map(f), key = key.mapValue(g))

    override def mapNodeId[Nid2](f: Nothing => Nid2): NodeLookupByKey[Cid, Val] = this

    override def keyMaintainers: Set[Party] = key.maintainers
    override def hasResult: Boolean = result.isDefined

  }

  object NodeLookupByKey extends WithTxValue2[NodeLookupByKey]

  case class KeyWithMaintainers[+Val](key: Val, maintainers: Set[Party]) {
    def mapValue[Val1](f: Val => Val1): KeyWithMaintainers[Val1] = copy(key = f(key))
  }

  object KeyWithMaintainers {
    implicit def equalInstance[Val: Equal]: Equal[KeyWithMaintainers[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val KeyWithMaintainers(bKey, bMaintainers) = b
        key === bKey && maintainers == bMaintainers
      }
  }

  final def isReplayedBy[Cid: Equal, Val: Equal](
      recorded: GenNode[Nothing, Cid, Val],
      isReplayedBy: GenNode[Nothing, Cid, Val],
  ): Boolean =
    ScalazEqual.match2[recorded.type, isReplayedBy.type, Boolean](fallback = false) {
      case nc: NodeCreate[Cid, Val] => {
        case NodeCreate(
            nodeSeed2,
            coid2,
            coinst2,
            optLocation2 @ _,
            signatories2,
            stakeholders2,
            key2) =>
          import nc._
          // NOTE(JM): Do not compare location annotations as they may differ due to
          // differing update expression constructed from the root node.
          nodeSeed == nodeSeed2 && coid === coid2 && coinst === coinst2 &&
          signatories == signatories2 && stakeholders == stakeholders2 && key === key2
        case _ => false
      }
      case nf: NodeFetch[Cid] => {
        case NodeFetch(
            coid2,
            templateId2,
            optLocation2 @ _,
            actingParties2,
            signatories2,
            stakeholders2,
            ) =>
          import nf._
          coid === coid2 && templateId == templateId2 &&
          actingParties.forall(_ => actingParties == actingParties2) &&
          signatories == signatories2 && stakeholders == stakeholders2
      }
      case ne: NodeExercises[Nothing, Cid, Val] => {
        case NodeExercises(
            nodeSeed2,
            targetCoid2,
            templateId2,
            choiceId2,
            optLocation2 @ _,
            consuming2,
            actingParties2,
            chosenValue2,
            stakeholders2,
            signatories2,
            controllers2,
            _,
            exerciseResult2,
            key2,
            ) =>
          import ne._
          nodeSeed == nodeSeed2 &&
          targetCoid === targetCoid2 && templateId == templateId2 && choiceId == choiceId2 &&
          consuming == consuming2 && actingParties == actingParties2 && chosenValue === chosenValue2 &&
          stakeholders == stakeholders2 && signatories == signatories2 && controllers == controllers2 &&
          exerciseResult.fold(true)(_ => exerciseResult === exerciseResult2) &&
          key.fold(true)(_ => key === key2)
      }
      case nl: NodeLookupByKey[Cid, Val] => {
        case NodeLookupByKey(templateId2, optLocation2 @ _, key2, result2) =>
          import nl._
          templateId == templateId2 &&
          key === key2 && result === result2
      }
    }(recorded, isReplayedBy)

  /** Useful in various circumstances -- basically this is what a ledger implementation must use as
    * a key.
    */
  case class GlobalKey(templateId: Identifier, key: VersionedValue[Nothing])

  sealed trait WithTxValue2[F[+ _, + _]] {
    type WithTxValue[+Cid] = F[Cid, Transaction.Value[Cid]]
  }

  sealed trait WithTxValue3[F[+ _, + _, + _]] {
    type WithTxValue[+Nid, +Cid] = F[Nid, Cid, Transaction.Value[Cid]]
  }
}
