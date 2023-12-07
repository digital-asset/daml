// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package transaction

import stainless.lang.{
  unfold,
  decreases,
  BooleanDecorations,
  Either,
  Some,
  None,
  Option,
  Right,
  Left,
}
import stainless.annotation._
import scala.annotation.targetName
import stainless.collection._
import utils.Value.ContractId
import utils.TransactionErrors.{CreateError, InconsistentContractKey, KeyInputError}
import utils._

import ContractStateMachine._
import CSMHelpers._
import CSMEither._
import CSMEitherDef._

/** Proofs related to globalKeys and localKeys fields of a state.
  * As usual CSMKeysPropertiesDef contains all the definitions whereas CSMKeysProperties contains all the theorems
  * with their respective proofs.
  *
  * In the simplified version of the contract state machine, the handleNode function is split into two functions:
  * - A simplified version of handleNode that does not modify the global keys
  * - A function addKey that adds the right mapping to the global keys if the key is still not registered in the map.
  *   This mapping is described by nodeActionKeyMapping in CSMHelpers
  *
  * We prove in TransactionTreeFull that we can extract all addKey of a transaction to put them at the beginning.
  * Going through a transaction is therefore equivalent to first update all the keys and then processing the transaction.
  *
  * Therefore when dealing with (the simplified version of) handleNode, we always make the assumption that there is
  * already an entry in the activeKeys for the key of the node we are handling.
  */
object CSMKeysPropertiesDef {

  /** Checks whether there is already an entry for the key k in the globalKeys of the state.
    *
    * Since in a well-defined state the keys in localKeys are a subset of the keys in globalKeys
    * this is easier yet equivalent to checking whether k is contained in the activeKeys.
    *
    * All the versions of the function are opaque which can make reasonning tedious. This is however
    * necessary to reduce the complexity of the proofs and being able to keep a low timeout.
    */
  @pure
  @opaque
  def containsKey(s: State)(k: GlobalKey): Boolean = {
    s.globalKeys.contains(k)
  }

  /** Checks whether there is already an entry for k in the globalKeys of the state in case k is defined.
    * If the key is not defined, returns true.
    *
    * For more details cf containsKey above
    */
  @pure
  @opaque
  def containsOptionKey(s: State)(k: Option[GlobalKey]): Boolean = {
    k.forall(containsKey(s))
  }

  /** Checks whether there is already an entry for the key of n in the globalKeys of the state in case it is defined.
    * If the key is not defined, returns true.
    *
    * For more details cf containsKey above
    */
  @pure
  @opaque
  def containsActionKey(s: State)(n: Node.Action): Boolean = {
    containsOptionKey(s)(n.gkeyOpt)
  }
  @pure
  @opaque
  def containsNodeKey(s: State)(n: Node): Boolean = {
    n match {
      case a: Node.Action => containsActionKey(s)(a)
      case r: Node.Rollback => true
    }
  }

  /** Checks whether there is already an entry for the key of n in the globalKeys of the state in case both are defined.
    * If the key or the state is not defined, returns true.
    *
    * For more details cf containsKey above
    */
  @pure
  @opaque
  def containsKey[T](e: Either[T, State])(n: Node): Boolean = {
    e match {
      case Right(s) => containsNodeKey(s)(n)
      case Left(_) => true
    }
  }

  /** Replace the globalKeys of s with glK one of their supermap
    *
    * Among the direct properties one can deduce from the definition we have that the activeKeys of s are also
    * a submap of the activeKeys of the result
    */

  @pure
  @opaque
  def extendGlobalKeys(s: State, glK: Map[GlobalKey, KeyMapping]): State = {
    require(s.globalKeys.submapOf(glK))

    val res = s.copy(globalKeys = glK)

    @pure @opaque
    def extendGlobalKeysProperties: Unit = {
      MapProperties.submapOfKeySet(s.globalKeys, glK)
      unfold(s.activeKeys)
      unfold(res.activeKeys)
      unfold(s.keys)
      unfold(res.keys)
      MapProperties.concatSubmapOf(s.globalKeys, glK, s.activeState.localKeys.mapValues(KeyActive))
      MapProperties.mapValuesSubmapOf(
        s.keys,
        res.keys,
        keyMappingToActiveMapping(s.activeState.consumedBy),
      )
    }.ensuring(
      (s.rollbackStack == res.rollbackStack) &&
        (s.activeState == res.activeState) &&
        (s.locallyCreated == res.locallyCreated) &&
        (s.inputContractIds == res.inputContractIds) &&
        (s.consumed == res.consumed) &&
        (res.globalKeys == glK) &&
        (s.activeKeys.submapOf(res.activeKeys))
    )

    extendGlobalKeysProperties

    res

  }.ensuring(res =>
    (s.rollbackStack == res.rollbackStack) &&
      (s.activeState == res.activeState) &&
      (s.locallyCreated == res.locallyCreated) &&
      (s.inputContractIds == res.inputContractIds) &&
      (s.consumed == res.consumed) &&
      (res.globalKeys == glK) &&
      (s.activeKeys.submapOf(res.activeKeys))
  )

  /** Extends the globalKeys of s to the left. More precisely overwrite the globalKeys of glK with the globalKeys of s.
    *
    * Among the direct properties one can deduce from the definition we have that the activeKeys of s are also
    * a submap of the activeKeys of the result. The same holds for the globalKeys.
    */

  @pure
  @opaque
  def concatLeftGlobalKeys(s: State, glK: Map[GlobalKey, KeyMapping]): State = {
    MapProperties.concatSubmapOf(glK, s.globalKeys)
    extendGlobalKeys(s, glK ++ s.globalKeys)
  }.ensuring(res =>
    (s.rollbackStack == res.rollbackStack) &&
      (s.activeState == res.activeState) &&
      (s.locallyCreated == res.locallyCreated) &&
      (s.inputContractIds == res.inputContractIds) &&
      (s.consumed == res.consumed) &&
      (s.activeKeys.submapOf(res.activeKeys)) &&
      (s.globalKeys.submapOf(res.globalKeys))
  )

  /** If e is a well defined state, extends the globalKeys of s to the left. Otherwise returns the same error.
    */

  @pure
  def concatLeftGlobalKeys[T](
      e: Either[T, State],
      glK: Map[GlobalKey, KeyMapping],
  ): Either[T, State] = {
    val res = e.map(s => concatLeftGlobalKeys(s, glK))

    @pure
    @opaque
    def concatLeftGlobalKeysProp: Unit = {
      sameErrorMap(e, s => concatLeftGlobalKeys(s, glK))
      unfold(sameStack(e, res))
      unfold(sameActiveState(e, res))
      unfold(sameLocallyCreated(e, res))
      unfold(sameConsumed(e, res))
      e match {
        case Left(_) => Trivial()
        case Right(r) =>
          unfold(sameStack(r, res))
          unfold(sameActiveState(r, res))
          unfold(sameLocallyCreated(r, res))
          unfold(sameConsumed(r, res))
      }
    }.ensuring(
      sameError(e, res) &&
        sameStack(e, res) &&
        sameActiveState(e, res) &&
        sameLocallyCreated(e, res) &&
        sameConsumed(e, res)
    )

    concatLeftGlobalKeysProp
    res
  }.ensuring(res =>
    sameError(e, res) &&
      sameStack(e, res) &&
      sameActiveState(e, res) &&
      sameLocallyCreated(e, res) &&
      sameConsumed(e, res)
  )

  /** Adds the pair (k, mapping) to the globalKeys of s if s does not contain k.
    */

  @pure
  @opaque
  def addKey(s: State, k: GlobalKey, mapping: KeyMapping): State = {
    MapProperties.submapOfReflexivity(s.globalKeys)
    unfold(containsKey(s)(k))
    val res =
      extendGlobalKeys(s, if (containsKey(s)(k)) s.globalKeys else s.globalKeys.updated(k, mapping))
    unfold(containsKey(res)(k))
    unfold(concatLeftGlobalKeys(s, Map[GlobalKey, KeyMapping](k -> mapping)))
    if (containsKey(s)(k)) {
      MapProperties.keySetContains(s.globalKeys, k)
      MapProperties.singletonKeySet(k, mapping)
      SetProperties.singletonSubsetOf(s.globalKeys.keySet, k)
      SetProperties.equalsSubsetOfTransitivity(
        Set(k),
        Map[GlobalKey, KeyMapping](k -> mapping).keySet,
        s.globalKeys.keySet,
      )
      MapProperties.concatSubsetOfEquals(Map[GlobalKey, KeyMapping](k -> mapping), s.globalKeys)
      MapAxioms.extensionality(
        Map[GlobalKey, KeyMapping](k -> mapping) ++ s.globalKeys,
        s.globalKeys,
      )
    } else {
      MapProperties.updatedCommutativity(s.globalKeys, k, mapping)
      MapAxioms.extensionality(
        Map[GlobalKey, KeyMapping](k -> mapping) ++ s.globalKeys,
        s.globalKeys.updated(k, mapping),
      )
    }
    res
  }.ensuring(res =>
    containsKey(res)(k) &&
      (res == concatLeftGlobalKeys(s, Map[GlobalKey, KeyMapping](k -> mapping))) &&
      (containsKey(s)(k) ==> (res == s))
  )

  /** Adds the pair (k, mapping) to the globalKeys of s if k is defined and s does not contain k.
    */
  @pure
  @opaque
  def addKey(s: State, k: Option[GlobalKey], mapping: KeyMapping): State = {
    MapProperties.submapOfReflexivity(s.globalKeys)
    val res = k match {
      case None() => s
      case Some(gk) => addKey(s, gk, mapping)
    }
    unfold(containsOptionKey(s)(k))
    unfold(containsOptionKey(res)(k))
    unfold(optionKeyMap(k, mapping))
    unfold(concatLeftGlobalKeys(s, optionKeyMap(k, mapping)))
    MapProperties.concatEmpty(s.globalKeys)
    MapAxioms.extensionality(Map.empty[GlobalKey, KeyMapping] ++ s.globalKeys, s.globalKeys)
    res
  }.ensuring(res =>
    containsOptionKey(res)(k) &&
      (res == concatLeftGlobalKeys(s, optionKeyMap(k, mapping))) &&
      (containsOptionKey(s)(k) ==> (res == s))
  )

  /** Adds the pair (n.gkeyOpt, nodeActionKeyMapping(n)) to the globalKeys of s if the key of n is well-defined.
    *
    * nodeActionKeyMapping is the mapping corresponding to the node (cf. CSMHelpers). It is defined as:
    * - KeyInactive if n is a Create Node
    * - KeyActive(n.coid) if n is a Fetch Node
    * - n.result if n is a Lookup Node
    * - KeyActive(n.targetCoid) if n is an Exercise Node
    */
  @pure
  @opaque
  def addKeyBeforeAction(s: State, n: Node.Action): State = {
    val res = addKey(s, n.gkeyOpt, nodeActionKeyMapping(n))
    unfold(containsActionKey(s)(n))
    unfold(containsActionKey(res)(n))
    unfold(actionKeyMap(n))
    res
  }.ensuring(res =>
    containsActionKey(res)(n) &&
      (res == concatLeftGlobalKeys(s, actionKeyMap(n))) &&
      (containsActionKey(s)(n) ==> (res == s))
  )
  @pure
  @opaque
  def addKeyBeforeNode(s: State, n: Node): State = {
    val res =
      n match {
        case a: Node.Action => addKeyBeforeAction(s, a)
        case r: Node.Rollback => s
      }
    unfold(containsNodeKey(res)(n))
    unfold(containsNodeKey(s)(n))
    unfold(nodeKeyMap(n))
    unfold(concatLeftGlobalKeys(s, nodeKeyMap(n)))
    MapProperties.concatEmpty(s.globalKeys)
    MapAxioms.extensionality(Map.empty[GlobalKey, KeyMapping] ++ s.globalKeys, s.globalKeys)
    res
  }.ensuring(res =>
    (res == concatLeftGlobalKeys(s, nodeKeyMap(n))) &&
      containsNodeKey(res)(n) &&
      (containsNodeKey(s)(n) ==> (res == s))
  )

  /** If e is well-defined adds the pair corresponding to n to the global keys of the state.
    *
    * For more details cf. addKeyBeforeAction
    */
  @pure
  @opaque
  def addKeyBeforeNode[T](e: Either[T, State], n: Node): Either[T, State] = {

    val res = e.map((s: State) => addKeyBeforeNode(s, n))

    @pure
    @opaque
    def addKeyBeforeNodeProperties: Unit = {
      sameErrorMap(e, (s: State) => addKeyBeforeNode(s, n))
      unfold(containsKey(res)(n))
      unfold(containsKey(e)(n))
      unfold(sameStack(e, res))
      unfold(concatLeftGlobalKeys(e, nodeKeyMap(n)))
      e match {
        case Left(_) => Trivial()
        case Right(r) => unfold(sameStack(r, res))
      }
    }.ensuring(
      sameStack(e, res) &&
        sameError(e, res) &&
        containsKey(res)(n) &&
        (containsKey(e)(n) ==> (res == e)) &&
        (res == concatLeftGlobalKeys(e, nodeKeyMap(n)))
    )

    addKeyBeforeNodeProperties
    res
  }.ensuring(res =>
    sameStack(e, res) &&
      sameError(e, res) &&
      containsKey(res)(n) &&
      (containsKey(e)(n) ==> (res == e)) &&
      (res == concatLeftGlobalKeys(e, nodeKeyMap(n)))
  )
  @pure
  def addKeyBeforeNode[T](e: Either[T, State], p: (NodeId, Node)): Either[T, State] = {
    addKeyBeforeNode(e, p._2)
  }

  /** Map representing a key-mapping pair when the key is an option. If the key is not defined then the result is empty
    * otherwise it is a singleton containing the pair.
    */
  @pure
  @opaque
  def optionKeyMap(k: Option[GlobalKey], m: KeyMapping): Map[GlobalKey, KeyMapping] = {
    k match {
      case None() => Map.empty[GlobalKey, KeyMapping]
      case Some(gk) => Map[GlobalKey, KeyMapping](gk -> m)
    }
  }

  /** Map representing the key-mapping pair of a node. If the key of the node is not defined then the result is empty
    * otherwise it is a singleton containing the key with the node's corresponding mapping.
    *
    * The latter is defined as:
    * - KeyInactive if n is a Create Node
    * - KeyActive(n.coid) if n is a Fetch Node
    * - n.result if n is a Lookup Node
    * - KeyActive(n.targetCoid) if n is an Exercise Node
    */
  @pure
  @opaque
  def actionKeyMap(n: Node.Action): Map[GlobalKey, KeyMapping] = {
    optionKeyMap(n.gkeyOpt, nodeActionKeyMapping(n))
  }
  @pure
  @opaque
  def nodeKeyMap(n: Node): Map[GlobalKey, KeyMapping] = {
    n match {
      case r: Node.Rollback => Map.empty[GlobalKey, KeyMapping]
      case a: Node.Action => actionKeyMap(a)
    }
  }

}

object CSMKeysProperties {

  import CSMKeysPropertiesDef._

  /** If s1 and s2 have the same globalKeys, then s1 contains k if and only if s2 contains k.
    *
    * Even though this is obvious from the definition, the opaque annotations require the needs for theorems
    * stating this property.
    */
  @pure
  @opaque
  def containsKeySameGlobalKeys(s1: State, s2: State, k: GlobalKey): Unit = {
    require(s1.globalKeys == s2.globalKeys)
    unfold(containsKey(s1)(k))
    unfold(containsKey(s2)(k))
  }.ensuring(containsKey(s1)(k) == containsKey(s2)(k))
  @pure
  @opaque
  def containsOptionKeySameGlobalKeys(s1: State, s2: State, k: Option[GlobalKey]): Unit = {
    require(s1.globalKeys == s2.globalKeys)
    unfold(containsOptionKey(s1)(k))
    unfold(containsOptionKey(s2)(k))
    k match {
      case Some(gk) => containsKeySameGlobalKeys(s1, s2, gk)
      case _ => Trivial()
    }
  }.ensuring(containsOptionKey(s1)(k) == containsOptionKey(s2)(k))

  /** If s1 and s2 have the same globalKeys, then s1 contains the key of the argument node n if and only if s2 contains it.
    *
    * Even though this is obvious from the definition, the opaque annotations require the needs for theorems
    * stating this property.
    */
  @pure
  @opaque
  def containsActionKeySameGlobalKeys(s1: State, s2: State, n: Node.Action): Unit = {
    require(s1.globalKeys == s2.globalKeys)
    unfold(containsActionKey(s1)(n))
    unfold(containsActionKey(s2)(n))
    containsOptionKeySameGlobalKeys(s1, s2, n.gkeyOpt)
  }.ensuring(containsActionKey(s1)(n) == containsActionKey(s2)(n))
  @pure
  @opaque
  def containsNodeKeySameGlobalKeys(s1: State, s2: State, n: Node): Unit = {
    require(s1.globalKeys == s2.globalKeys)
    unfold(containsNodeKey(s1)(n))
    unfold(containsNodeKey(s2)(n))
    n match {
      case a: Node.Action =>
        containsActionKeySameGlobalKeys(s1, s2, a)
      case r: Node.Rollback => Trivial()
    }
  }.ensuring(containsNodeKey(s1)(n) == containsNodeKey(s2)(n))

  /** e1 and e2 are states such that when e1 is not defined e2 is not defined as well.
    * If when both are defined, their globalKeys are the same then one contains the key of n if and only if the other
    * also does.
    *
    * Even though this is obvious from the definition, the opaque annotations require the needs for theorems
    * stating this property.
    */
  @pure
  @opaque
  def containsKeySameGlobalKeys[S, T](e1: Either[S, State], e2: Either[T, State], n: Node): Unit = {
    require(sameGlobalKeys(e1, e2))
    require(propagatesError(e1, e2))

    unfold(sameGlobalKeys(e1, e2))
    unfold(containsKey(e1)(n))
    unfold(containsKey(e2)(n))
    unfold(propagatesError(e1, e2))

    e1 match {
      case Right(s1) =>
        unfold(sameGlobalKeys(s1, e2))
        e2 match {
          case Right(s2) => containsNodeKeySameGlobalKeys(s1, s2, n)
          case _ => Trivial()
        }
      case _ => Trivial()
    }

  }.ensuring(containsKey(e1)(n) ==> containsKey(e2)(n))

  /** Definition of s.keys.contains expressed in function containsKey
    */
  @pure
  @opaque
  def keysContains(s: State, k: GlobalKey): Unit = {
    unfold(s.keys)
    unfold(containsKey(s)(k))

    MapProperties.concatContains(s.globalKeys, s.activeState.localKeys.mapValues(KeyActive), k)
    MapProperties.mapValuesContains(s.activeState.localKeys, KeyActive, k)
  }.ensuring(s.keys.contains(k) == (containsKey(s)(k) || s.activeState.localKeys.contains(k)))

  /** Definition of s.activeKeys.contains expressed in function containsKey
    */
  @pure
  @opaque
  def activeKeysContains(s: State, k: GlobalKey): Unit = {
    unfold(s.activeKeys)
    keysContains(s, k)
    MapProperties.mapValuesContains(s.keys, keyMappingToActiveMapping(s.activeState.consumedBy), k)
  }.ensuring(s.activeKeys.contains(k) == (containsKey(s)(k) || s.activeState.localKeys.contains(k)))

  /** If s contains k then the activeKeys of s also contains k
    */
  @pure
  @opaque
  def activeKeysContainsKey(s: State, k: GlobalKey): Unit = {
    require(containsKey(s)(k))
    activeKeysContains(s, k)
  }.ensuring(s.activeKeys.contains(k))

  /** If k is defined and s contains k then the activeKeys of s also contains k
    */
  @pure
  @opaque
  def activeKeysContainsKey(s: State, k: Option[GlobalKey]): Unit = {
    require(containsOptionKey(s)(k))
    unfold(containsOptionKey(s)(k))
    k match {
      case Some(gk) => activeKeysContains(s, gk)
      case None() => Trivial()
    }
  }.ensuring(k.forall(s.activeKeys.contains))

  /** If n key is defined and s contains it then the activeKeys of s also contains the key
    */
  @pure
  @opaque
  def activeKeysContainsKey(s: State, n: Node.Action): Unit = {
    require(containsActionKey(s)(n))
    unfold(containsActionKey(s)(n))
    activeKeysContainsKey(s, n.gkeyOpt)
  }.ensuring(n.gkeyOpt.forall(s.activeKeys.contains))

  /** If s contains k then any state extending its global keys also contains k
    */
  @pure
  @opaque
  def containsKeyExtend(s: State, k: GlobalKey, glK: Map[GlobalKey, KeyMapping]): Unit = {
    require(s.globalKeys.submapOf(glK))
    require(containsKey(s)(k))
    unfold(containsKey(s)(k))
    unfold(containsKey(extendGlobalKeys(s, glK))(k))
    MapProperties.submapOfContains(s.globalKeys, extendGlobalKeys(s, glK).globalKeys, k)
  }.ensuring(containsKey(extendGlobalKeys(s, glK))(k))

  /** If k is defined and s contains k then any state extending its global keys also contains k
    */
  @pure
  @opaque
  def containsOptionKeyExtend(
      s: State,
      k: Option[GlobalKey],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(s.globalKeys.submapOf(glK))
    require(containsOptionKey(s)(k))
    unfold(containsOptionKey(s)(k))
    unfold(containsOptionKey(extendGlobalKeys(s, glK))(k))
    k match {
      case None() => Trivial()
      case Some(gk) => containsKeyExtend(s, gk, glK)
    }
  }.ensuring(containsOptionKey(extendGlobalKeys(s, glK))(k))

  /** If the key of a node n is defined and s contains its then any state extending its global keys also contains the key
    */
  @pure
  @opaque
  def containsActionKeyExtend(s: State, n: Node.Action, glK: Map[GlobalKey, KeyMapping]): Unit = {
    require(s.globalKeys.submapOf(glK))
    require(containsActionKey(s)(n))
    unfold(containsActionKey(s)(n))
    unfold(containsActionKey(extendGlobalKeys(s, glK))(n))
    containsOptionKeyExtend(s, n.gkeyOpt, glK)
  }.ensuring(containsActionKey(extendGlobalKeys(s, glK))(n))

  @pure
  @opaque
  def containsNodeKeyExtend(s: State, n: Node, glK: Map[GlobalKey, KeyMapping]): Unit = {
    require(s.globalKeys.submapOf(glK))
    require(containsNodeKey(s)(n))
    unfold(containsNodeKey(s)(n))
    unfold(containsNodeKey(extendGlobalKeys(s, glK))(n))
    n match {
      case a: Node.Action => containsActionKeyExtend(s, a, glK)
      case r: Node.Rollback => Trivial()
    }
  }.ensuring(containsNodeKey(extendGlobalKeys(s, glK))(n))

  /** If the key of a node n is defined and s contains its then any state extending its global keys to the left also contains the key
    */
  @pure
  @opaque
  def containsNodeKeyConcatLeft(s: State, n: Node, glK: Map[GlobalKey, KeyMapping]): Unit = {
    require(containsNodeKey(s)(n))
    unfold(concatLeftGlobalKeys(s, glK))
    containsNodeKeyExtend(s, n, glK ++ s.globalKeys)
  }.ensuring(containsNodeKey(concatLeftGlobalKeys(s, glK))(n))

  @pure
  @opaque
  def containsKeyConcatLeft[T](
      e: Either[T, State],
      n: Node,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsKey(e)(n))

    unfold(containsKey(e)(n))
    unfold(containsKey(concatLeftGlobalKeys(e, glK))(n))
    unfold(concatLeftGlobalKeys(e, glK))

    e match {
      case Right(s) => containsNodeKeyConcatLeft(s, n, glK)
      case _ => Trivial()
    }

  }.ensuring(containsKey(concatLeftGlobalKeys(e, glK))(n))

  /** If the key of a node n1 is defined and e contains it, then adding the key of n2 to the global keys of the state
    * does not change the truth of the statement.
    */
  @pure
  @opaque
  def containsKeyAddKeyBeforeNode[T](e: Either[T, State], n1: Node, n2: Node): Unit = {
    require(containsKey(e)(n2))
    if (containsKey(e)(n1)) {
      Trivial()
    } else {
      containsKeyConcatLeft(e, n2, nodeKeyMap(n1))
    }
  }.ensuring(containsKey(addKeyBeforeNode(e, n1))(n2))

  /** The key set of the activeKeys of a state are the concatenation between the keyset of its globalKeys and its localKeys
    */
  @pure
  @opaque
  def activeKeysKeySet(s: State): Unit = {
    unfold(s.activeKeys)
    MapProperties.mapValuesKeySet(s.keys, keyMappingToActiveMapping(s.activeState.consumedBy))
    SetProperties.equalsTransitivity(
      s.activeKeys.keySet,
      s.keys.keySet,
      s.globalKeys.keySet ++ s.activeState.localKeys.keySet,
    )
  }.ensuring(s.activeKeys.keySet === s.globalKeys.keySet ++ s.activeState.localKeys.keySet)

  /** Getting a mapping in the keys of a state is the same as first looking into the local keys, mapping it to KeyActive
    * (since localKeys contains contractIds) and in case of failure looking in the global keys
    */
  @pure
  @opaque
  def keysGet(s: State, k: GlobalKey): Unit = {
    unfold(s.keys)
    MapAxioms.concatGet(s.globalKeys, s.activeState.localKeys.mapValues(KeyActive), k)
    MapProperties.mapValuesGet(s.activeState.localKeys, KeyActive, k)
  }.ensuring(
    s.keys.get(k) == s.activeState.localKeys.get(k).map(KeyActive).orElse(s.globalKeys.get(k))
  )

  /** Getting a mapping in the activeKeys of a state is the same as first looking into the local keys, mapping it to
    * KeyActive (since localKeys contains contractIds) and afterward filtering it if is consumed, and in case of failure
    * looking in the global keys before filtering it.
    */
  @pure
  @opaque
  def activeKeysGet(s: State, k: GlobalKey): Unit = {
    unfold(s.activeKeys)
    keysGet(s, k)
    MapProperties.mapValuesGet(s.keys, keyMappingToActiveMapping(s.activeState.consumedBy), k)
  }.ensuring(
    s.activeKeys.get(k) == s.activeState.localKeys
      .get(k)
      .map(KeyActive andThen keyMappingToActiveMapping(s.activeState.consumedBy))
      .orElse(s.globalKeys.get(k).map(keyMappingToActiveMapping(s.activeState.consumedBy)))
  )

  /** Getting a mapping in the activeKeys of a state is the same as first looking into the local keys, mapping it to
    * KeyActive (since localKeys contains contractIds) and afterward filtering it if is consumed, and in case of failure
    * looking in the global keys before filtering it.
    */
  @pure
  @opaque
  def activeKeysGetOrElse(s: State, k: GlobalKey, d: KeyMapping): Unit = {
    unfold(s.activeKeys.getOrElse(k, d))
    activeKeysGet(s, k)
  }.ensuring(
    s.activeKeys.getOrElse(k, d) == s.activeState.localKeys
      .get(k)
      .map(KeyActive andThen keyMappingToActiveMapping(s.activeState.consumedBy))
      .orElse(s.globalKeys.get(k).map(keyMappingToActiveMapping(s.activeState.consumedBy)))
      .getOrElse(d)
  )

  /** If the globalKeys of a state already contain a key, then concatenating other keys to the globalKeys does not affect
    * the mapping of that key in the activeKey.
    */
  @pure
  @opaque
  def activeKeysConcatLeftGlobalKeys(
      s: State,
      k: GlobalKey,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsKey(s)(k))
    activeKeysContains(s, k)
    MapAxioms.submapOfGet(s.activeKeys, concatLeftGlobalKeys(s, glK).activeKeys, k)
  }.ensuring(
    concatLeftGlobalKeys(s, glK).activeKeys.get(k) == s.activeKeys.get(k)
  )

  /** Getting a mapping in the activeKeys after having added the key to the globalKeys is the same as searching it in the
    * activeKeys of the state before the addition and in case of failure taking the newly added mapping before filtering
    * it if is consumed.
    */
  @pure
  @opaque
  def activeKeysAddKey(s: State, k: GlobalKey, m: KeyMapping): Unit = {
    activeKeysGet(s, k)
    activeKeysGet(addKey(s, k, m), k)
    unfold(addKey(s, k, m))
    unfold(containsKey(s)(k))
    unfold(s.globalKeys.contains)
    activeKeysContains(addKey(s, k, m), k)
  }.ensuring(
    addKey(s, k, m).activeKeys.contains(k) &&
      (addKey(s, k, m).activeKeys.get(k) == s.activeKeys
        .get(k)
        .orElse(Some(keyMappingToActiveMapping(s.activeState.consumedBy)(m))))
  )

  /** If a state already contains the key of an Create node, then concatenating keys to the globalKeys and
    * visiting the node leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def visitCreateConcatLeftGlobalKeys(
      s: State,
      contractId: ContractId,
      mbKey: Option[GlobalKey],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsOptionKey(s)(mbKey))

    unfold(s.visitCreate(contractId, mbKey))
    unfold(concatLeftGlobalKeys(s, glK))
    unfold(concatLeftGlobalKeys(s, glK).visitCreate(contractId, mbKey))
    unfold(concatLeftGlobalKeys(s.visitCreate(contractId, mbKey), glK))
    unfold(containsOptionKey(s)(mbKey))

    val me =
      s.copy(
        locallyCreated = s.locallyCreated + contractId,
        activeState = s.activeState
          .copy(locallyCreatedThisTimeline = s.activeState.locallyCreatedThisTimeline + contractId),
      )

    mbKey match {
      case Some(k) =>
        activeKeysConcatLeftGlobalKeys(s, k, glK)
        unfold(
          concatLeftGlobalKeys(me.copy(activeState = me.activeState.createKey(k, contractId)), glK)
        )
      case None() =>
        unfold(concatLeftGlobalKeys(me, glK))
    }
  }.ensuring(
    concatLeftGlobalKeys(s, glK).visitCreate(contractId, mbKey) ==
      concatLeftGlobalKeys(s.visitCreate(contractId, mbKey), glK)
  )

  /** If a state already contains the key of a Lookup node, then concatenating keys to the globalKeys and
    * visiting the node leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def visitLookupConcatLeftGlobalKeys(
      s: State,
      gk: GlobalKey,
      keyResolution: Option[ContractId],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsKey(s)(gk))

    unfold(s.visitLookup(gk, keyResolution))
    unfold(concatLeftGlobalKeys(s, glK).visitLookup(gk, keyResolution))
    unfold(concatLeftGlobalKeys(s.visitLookup(gk, keyResolution), glK))

    unfold(s.activeKeys.getOrElse(gk, KeyInactive))
    unfold(concatLeftGlobalKeys(s, glK).activeKeys.getOrElse(gk, KeyInactive))

    activeKeysConcatLeftGlobalKeys(s, gk, glK)

  }.ensuring(
    concatLeftGlobalKeys(s, glK).visitLookup(gk, keyResolution) ==
      concatLeftGlobalKeys(s.visitLookup(gk, keyResolution), glK)
  )

  /** If a state already contains the key of a Fetch node, then concatenating keys to the globalKeys and
    * visiting the node leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def assertKeyMappingConcatLeftGlobalKeys(
      s: State,
      cid: ContractId,
      mbKey: Option[GlobalKey],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsOptionKey(s)(mbKey))

    unfold(s.assertKeyMapping(cid, mbKey))
    unfold(concatLeftGlobalKeys(s, glK).assertKeyMapping(cid, mbKey))
    unfold(concatLeftGlobalKeys(s.assertKeyMapping(cid, mbKey), glK))
    unfold(containsOptionKey(s)(mbKey))

    mbKey match {
      case None() => Trivial()
      case Some(gk) => visitLookupConcatLeftGlobalKeys(s, gk, KeyActive(cid), glK)
    }

  }.ensuring(
    concatLeftGlobalKeys(s, glK).assertKeyMapping(cid, mbKey) ==
      concatLeftGlobalKeys(s.assertKeyMapping(cid, mbKey), glK)
  )

  /** Concatenating keys to the globalKeys and consuming a contract leads to the same result than doing the same
    * operations in the reverse order.
    */
  @pure
  @opaque
  def consumeConcatLeftGlobalKeys(
      s: State,
      cid: ContractId,
      nid: NodeId,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(s.consume(cid, nid))
    unfold(concatLeftGlobalKeys(s, glK).consume(cid, nid))
    unfold(concatLeftGlobalKeys(s.consume(cid, nid), glK))
    unfold(concatLeftGlobalKeys(s, glK))
  }.ensuring(
    concatLeftGlobalKeys(s, glK).consume(cid, nid) ==
      concatLeftGlobalKeys(s.consume(cid, nid), glK)
  )

  /** If a state already contains the key of an Exercise node, then concatenating keys to the globalKeys and
    * visiting the node leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def visitExerciseConcatLeftGlobalKeys(
      s: State,
      nodeId: NodeId,
      targetId: ContractId,
      mbKey: Option[GlobalKey],
      byKey: Boolean,
      consuming: Boolean,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsOptionKey(s)(mbKey))

    unfold(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming))
    unfold(concatLeftGlobalKeys(s, glK).visitExercise(nodeId, targetId, mbKey, byKey, consuming))
    unfold(concatLeftGlobalKeys(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming), glK))
    assertKeyMappingConcatLeftGlobalKeys(s, targetId, mbKey, glK)
    unfold(concatLeftGlobalKeys(s.assertKeyMapping(targetId, mbKey), glK))

    s.assertKeyMapping(targetId, mbKey) match {
      case Right(s) => consumeConcatLeftGlobalKeys(s, targetId, nodeId, glK)
      case _ => Trivial()
    }

  }.ensuring(
    concatLeftGlobalKeys(s, glK).visitExercise(nodeId, targetId, mbKey, byKey, consuming) ==
      concatLeftGlobalKeys(s.visitExercise(nodeId, targetId, mbKey, byKey, consuming), glK)
  )

  /** Concatenating keys to the globalKeys and then mapping the error leads to the same result than doing the same
    * operations in the reverse order.
    */
  @pure
  @opaque
  def toKeyInputErrorConcatLeftGlobalKeys(
      e: Either[InconsistentContractKey, State],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(concatLeftGlobalKeys(e, glK))
    unfold(toKeyInputError(e))
    unfold(toKeyInputError(concatLeftGlobalKeys(e, glK)))
    unfold(concatLeftGlobalKeys(toKeyInputError(e), glK))
  }.ensuring(
    toKeyInputError(concatLeftGlobalKeys(e, glK)) ==
      concatLeftGlobalKeys(toKeyInputError(e), glK)
  )

  /** Concatenating keys to the globalKeys and then mapping the error leads to the same result than doing the same
    * operations in the reverse order.
    */
  @pure
  @opaque
  @targetName("toKeyInputErrorConcatLeftGlobalKeysDuplicateContractKey")
  def toKeyInputErrorConcatLeftGlobalKeys(
      e: Either[CreateError, State],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(concatLeftGlobalKeys(e, glK))
    unfold(toKeyInputError(e))
    unfold(toKeyInputError(concatLeftGlobalKeys(e, glK)))
    unfold(concatLeftGlobalKeys(toKeyInputError(e), glK))
  }.ensuring(
    toKeyInputError(concatLeftGlobalKeys(e, glK)) ==
      concatLeftGlobalKeys(toKeyInputError(e), glK)
  )

  /** If a state already contains the key of a node, then concatenating keys to the globalKeys and handling the node
    * leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def handleNodeConcatLeftGlobalKeys(
      s: State,
      nid: NodeId,
      n: Node.Action,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsActionKey(s)(n))

    unfold(concatLeftGlobalKeys(s, glK).handleNode(nid, n))
    unfold(s.handleNode(nid, n))
    unfold(containsActionKey(s)(n))
    n match {
      case create: Node.Create =>
        visitCreateConcatLeftGlobalKeys(s, create.coid, create.gkeyOpt, glK)
        toKeyInputErrorConcatLeftGlobalKeys(s.visitCreate(create.coid, create.gkeyOpt), glK)
      case fetch: Node.Fetch =>
        assertKeyMappingConcatLeftGlobalKeys(s, fetch.coid, fetch.gkeyOpt, glK)
        toKeyInputErrorConcatLeftGlobalKeys(s.assertKeyMapping(fetch.coid, fetch.gkeyOpt), glK)
      case lookup: Node.LookupByKey =>
        unfold(containsOptionKey(s)(n.gkeyOpt))
        visitLookupConcatLeftGlobalKeys(s, lookup.gkey, lookup.result, glK)
        toKeyInputErrorConcatLeftGlobalKeys(s.visitLookup(lookup.gkey, lookup.result), glK)
      case exe: Node.Exercise =>
        visitExerciseConcatLeftGlobalKeys(
          s,
          nid,
          exe.targetCoid,
          exe.gkeyOpt,
          exe.byKey,
          exe.consuming,
          glK,
        )
        toKeyInputErrorConcatLeftGlobalKeys(
          s.visitExercise(nid, exe.targetCoid, exe.gkeyOpt, exe.byKey, exe.consuming),
          glK,
        )
    }
  }.ensuring(
    concatLeftGlobalKeys(s, glK).handleNode(nid, n) ==
      concatLeftGlobalKeys(s.handleNode(nid, n), glK)
  )

  /** If a state already contains the key of a node, then concatenating keys to the globalKeys and handling the node
    * leads to the same result than doing the same operations in the reverse order.
    */
  @pure
  @opaque
  def handleNodeConcatLeftGlobalKeys(
      e: Either[KeyInputError, State],
      nid: NodeId,
      n: Node.Action,
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    require(containsKey(e)(n))
    unfold(containsKey(e)(n))
    unfold(concatLeftGlobalKeys(e, glK))
    unfold(handleNode(concatLeftGlobalKeys(e, glK), nid, n))
    unfold(handleNode(e, nid, n))
    e match {
      case Right(s) =>
        unfold(containsNodeKey(s)(n))
        unfold(containsActionKey(s)(n))
        handleNodeConcatLeftGlobalKeys(s, nid, n, glK)
      case _ => Trivial()
    }

  }.ensuring(
    handleNode(concatLeftGlobalKeys(e, glK), nid, n) ==
      concatLeftGlobalKeys(handleNode(e, nid, n), glK)
  )

  /** Concatenating keys to the globalKeys and calling beginRollback leads to the same result than doing the same operations in the reverse
    * order
    */
  @pure
  @opaque
  def beginRollbackConcatLeftGlobalKeys(
      e: Either[KeyInputError, State],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(beginRollback(concatLeftGlobalKeys(e, glK)))
    unfold(concatLeftGlobalKeys(e, glK))
    unfold(beginRollback(e))
    unfold(concatLeftGlobalKeys(beginRollback(e), glK))

    e match {
      case Right(s) =>
        unfold(concatLeftGlobalKeys(s, glK).beginRollback())
        unfold(concatLeftGlobalKeys(s, glK))
        unfold(s.beginRollback())
        unfold(concatLeftGlobalKeys(s.beginRollback(), glK))
      case _ => Trivial()
    }

  }.ensuring(
    beginRollback(concatLeftGlobalKeys(e, glK)) ==
      concatLeftGlobalKeys(beginRollback(e), glK)
  )

  /** Concatenating keys to the globalKeys and calling endRollback leads to the same result than doing the same operations in the reverse
    * order
    */
  @pure
  @opaque
  @dropVCs
  def endRollbackConcatLeftGlobalKeys(
      e: Either[KeyInputError, State],
      glK: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(endRollback(concatLeftGlobalKeys(e, glK)))
    unfold(concatLeftGlobalKeys(e, glK))
    unfold(endRollback(e))
    unfold(concatLeftGlobalKeys(endRollback(e), glK))

    e match {
      case Right(s) =>
        unfold(concatLeftGlobalKeys(s, glK).endRollback())
        unfold(concatLeftGlobalKeys(s, glK))
        unfold(s.endRollback())
        unfold(concatLeftGlobalKeys(s.endRollback(), glK))
      case _ => Trivial()
    }

  }.ensuring(
    endRollback(concatLeftGlobalKeys(e, glK)) ==
      concatLeftGlobalKeys(endRollback(e), glK)
  )

  /** Concatenating keys on the left is an associative operation.
    */
  @pure
  @opaque
  def concatLeftGlobalKeysAssociativity(
      s: State,
      glK1: Map[GlobalKey, KeyMapping],
      glK2: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(concatLeftGlobalKeys(concatLeftGlobalKeys(s, glK1), glK2))
    unfold(concatLeftGlobalKeys(s, glK1))
    unfold(concatLeftGlobalKeys(s, glK2 ++ glK1))
    MapProperties.concatAssociativity(glK2, glK1, s.globalKeys)
    MapAxioms.extensionality((glK2 ++ glK1) ++ s.globalKeys, glK2 ++ (glK1 ++ s.globalKeys))
  }.ensuring(
    concatLeftGlobalKeys(concatLeftGlobalKeys(s, glK1), glK2) ==
      concatLeftGlobalKeys(s, glK2 ++ glK1)
  )

  /** Concatenating keys on the left is an associative operation.
    */
  @pure
  @opaque
  def concatLeftGlobalKeysAssociativity[T](
      e: Either[T, State],
      glK1: Map[GlobalKey, KeyMapping],
      glK2: Map[GlobalKey, KeyMapping],
  ): Unit = {
    unfold(concatLeftGlobalKeys(e, glK1))
    unfold(concatLeftGlobalKeys(concatLeftGlobalKeys(e, glK1), glK2))
    unfold(concatLeftGlobalKeys(e, glK2 ++ glK1))
    e match {
      case Right(s) => concatLeftGlobalKeysAssociativity(s, glK1, glK2)
      case Left(_) => Trivial()
    }
  }.ensuring(
    concatLeftGlobalKeys(concatLeftGlobalKeys(e, glK1), glK2) ==
      concatLeftGlobalKeys(e, glK2 ++ glK1)
  )

}
