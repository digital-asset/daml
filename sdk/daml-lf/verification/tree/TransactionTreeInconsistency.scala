// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified.tree

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
import stainless.collection._
import utils.Value.ContractId
import utils.Transaction.{DuplicateContractKey, InconsistentContractKey, KeyInputError}
import utils._
import utils.TreeProperties._

import transaction.{State}
import transaction.CSMInconsistency._
import transaction.CSMInconsistencyDef._
import transaction.CSMInvariantDef._
import transaction.CSMEitherDef._
import transaction.CSMEither._
import transaction.CSMHelpers._
import transaction.CSMKeysProperties._
import transaction.CSMKeysPropertiesDef._
import transaction.ContractStateMachine.{KeyMapping}

import TransactionTreeInvariant._
import TransactionTreeDef._
import TransactionTree._
import TransactionTreeKeysDef._
import TransactionTreeKeys._

import TransactionTreeChecksDef._
import TransactionTreeChecks._

/** The purpose of this file is proving that the advance method is defined if and only if the transaction traversal
  * is defined as well.
  */
object TransactionTreeInconsistency {

  /** If an intermediate state of a transaction traversal is well-defined, then it will stay well-defined if it traverses
    * a node for the second time.
    *
    * @param tr The tree that is being traversed
    * @param init The initial state of the traversal
    * @param i The step number during which we process the node for the second time
    */
  @opaque
  @pure
  def upDefined(tr: Tree[(NodeId, Node)], init: Either[KeyInputError, State], i: BigInt): Unit = {
    require(i >= 0)
    require(i < 2 * tr.size)
    require(tr.isUnique)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(i)._3 == TraversalDirection.Up)

    val s: Either[KeyInputError, State] = scanTransaction(tr, init)(i)._1
    val p = scanTransaction(tr, init)(i)._2
    unfold(traverseOutFun(s, p))

    if (i == 2 * tr.size - 1) {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
      p._2 match {
        case a: Node.Action => Trivial()
        case r: Node.Rollback =>
          val (j, sub) = findBeginRollback(tr, init, i)
          traverseTransactionProp(sub, beginRollback(scanTransaction(tr, init)(j)._1))
          scanTransactionProp(tr, init, j, i)
          unfold(
            sameStack(
              beginRollback(scanTransaction(tr, init)(j)._1),
              traverseTransaction(sub, beginRollback(scanTransaction(tr, init)(j)._1)),
            )
          )
          unfold(propagatesError(scanTransaction(tr, init)(j)._1, s))
          unfold(beginRollback(scanTransaction(tr, init)(j)._1))
          unfold(scanTransaction(tr, init)(j)._1.get.beginRollback())
          unfold(
            sameStack(
              scanTransaction(tr, init)(j)._1.get.beginRollback(),
              traverseTransaction(sub, beginRollback(scanTransaction(tr, init)(j)._1)),
            )
          )
          unfold(endRollback(s))
          unfold(s.get.endRollback())
      }
    } else {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, i + 1)
      p._2 match {
        case a: Node.Action => Trivial()
        case r: Node.Rollback =>
          val (j, sub) = findBeginRollback(tr, init, i)
          traverseTransactionProp(sub, beginRollback(scanTransaction(tr, init)(j)._1))
          scanTransactionProp(tr, init, j, i)
          unfold(
            sameStack(
              beginRollback(scanTransaction(tr, init)(j)._1),
              traverseTransaction(sub, beginRollback(scanTransaction(tr, init)(j)._1)),
            )
          )
          unfold(propagatesError(scanTransaction(tr, init)(j)._1, s))
          unfold(beginRollback(scanTransaction(tr, init)(j)._1))
          unfold(scanTransaction(tr, init)(j)._1.get.beginRollback())
          unfold(
            sameStack(
              scanTransaction(tr, init)(j)._1.get.beginRollback(),
              traverseTransaction(sub, beginRollback(scanTransaction(tr, init)(j)._1)),
            )
          )
          unfold(endRollback(s))
          unfold(s.get.endRollback())

      }
    }

  }.ensuring(
    if (i == 2 * tr.size - 1) {
      traverseTransaction(tr, init).isRight
    } else {
      scanTransaction(tr, init)(i + 1)._1.isRight
    }
  )

  /** The actives keys does not change after entering a rollback node, processing a subtree and leaving
    * the rollback node.
    *
    * @param tr The subtree that is being traversed
    * @param s The initial state
    * @param k The key we are querying in the active keys
    */
  @opaque
  @pure
  def activeKeysGetTraverseTransaction(
      tr: Tree[(NodeId, Node)],
      s: Either[KeyInputError, State],
      k: GlobalKey,
  ): Unit = {
    require(endRollback(traverseTransaction(tr, beginRollback(s))).isRight)
    require(s.isRight)

    traverseTransactionProp(tr, beginRollback(s))
    endRollbackProp(traverseTransaction(tr, beginRollback(s)))

    unfold(propagatesError(beginRollback(s), traverseTransaction(tr, beginRollback(s))))
    unfold(
      propagatesError(
        traverseTransaction(tr, beginRollback(s)),
        endRollback(traverseTransaction(tr, beginRollback(s))),
      )
    )

    sameGlobalKeysTransitivity(s, beginRollback(s), traverseTransaction(tr, beginRollback(s)))
    sameGlobalKeysTransitivity(
      s,
      traverseTransaction(tr, beginRollback(s)),
      endRollback(traverseTransaction(tr, beginRollback(s))),
    )
    unfold(sameGlobalKeys(s, endRollback(traverseTransaction(tr, beginRollback(s)))))
    unfold(sameGlobalKeys(s.get, endRollback(traverseTransaction(tr, beginRollback(s)))))

    unfold(beginRollback(s))
    unfold(s.get.beginRollback())
    unfold(endRollback(traverseTransaction(tr, beginRollback(s))))
    unfold(traverseTransaction(tr, beginRollback(s)).get.endRollback())
    unfold(sameStack(beginRollback(s), traverseTransaction(tr, beginRollback(s))))
    unfold(sameStack(s.get.beginRollback(), traverseTransaction(tr, beginRollback(s))))

    activeKeysGetSameFields(s.get, endRollback(traverseTransaction(tr, beginRollback(s))).get, k)
  }.ensuring(
    endRollback(traverseTransaction(tr, beginRollback(s))).get.activeKeys.get(k) ==
      s.get.activeKeys.get(k)
  )

  /** If a key did not appear at given point in the transaction, then its entry in the active keys will be the same for
    * all intermediate states until that point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param k The key which did not appear yet
    * @param j    The step number of an other intermediate state that appeared before j
    * @param j    The step number for which the key did not appear yet
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def doesNotAppearBeforeSameActiveKeysGet(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      k: GlobalKey,
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(j)
    require(0 <= i)
    require(i <= j)
    require(j < 2 * tr.size)

    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(j)._1.isRight)

    require(doesNotAppearBefore(tr, init, traverseInFun, traverseOutFun, k, j))

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    if (i == j) {
      Trivial()
    } else {
      val (sj, n, dir) = scanTransaction(tr, init)(j - 1)

      unfold(doesNotAppearBefore(tr, init, traverseInFun, traverseOutFun, k, j))
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, j)
      unfold(propagatesError(sj, scanTransaction(tr, init)(j)._1))
      doesNotAppearBeforeSameActiveKeysGet(tr, init, k, i, j - 1)

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFun(sj, n))
        n._2 match {
          case a: Node.Action =>
            scanInvariant(tr, init, j - 1)
            scanStateNodeCompatibility(tr, init, j - 1)
            containsAllKeysImpliesDown(tr, init, j - 1)
            unfold(containsKey(sj)(n._2))
            unfold(containsNodeKey(sj.get)(n._2))

            // required
            assert(!appearsAtIndex(tr, init, traverseInFun, traverseOutFun, k, j - 1))

            unfold(appearsAtIndex(tr, init, traverseInFun, traverseOutFun, k, j - 1))
            handleNodeActiveKeysGet(
              sj.get,
              n._1,
              a,
              k,
              traverseUnbound(tr)._1,
              traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
            )
          case r: Node.Rollback =>
            unfold(beginRollback(scanTransaction(tr, init)(j - 1)._1))
            beginRollbackActiveKeysGet(sj.get, k)
        }
      } else {
        unfold(traverseOutFun(sj, n))
        n._2 match {
          case a: Node.Action => Trivial()
          case r: Node.Rollback =>
            val (j2, sub) = findBeginRollback(tr, init, j - 1)
            scanTransactionProp(tr, init, j2, j - 1)
            unfold(propagatesError(scanTransaction(tr, init)(j2)._1, sj))
            activeKeysGetTraverseTransaction(sub, scanTransaction(tr, init)(j2)._1, k)
            doesNotAppearBeforeSameActiveKeysGet(tr, init, k, j2, j - 1)
        }
      }
    }
  }.ensuring(
    scanTransaction(tr, init)(i)._1.get.activeKeys.get(k) ==
      scanTransaction(tr, init)(j)._1.get.activeKeys.get(k)
  )

  /** If a key appeared for the first time at given point in the transaction, then its entry in the active keys will be
    * the same for all intermediate states until that point.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param k    The key that appeared for the first time at step j
    * @param j    The step number of an other intermediate state that appeared before j
    * @param j    The step number during which the key appeared
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def firstAppearsSameActiveKeysGet(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      k: GlobalKey,
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(j)
    require(0 <= i)
    require(i <= j)
    require(j < 2 * tr.size)

    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(j)._1.isRight)

    require(firstAppears(tr, init, traverseInFun, traverseOutFun, k, j))

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    unfold(firstAppears(tr, init, traverseInFun, traverseOutFun, k, j))
    doesNotAppearBeforeSameActiveKeysGet(tr, init, k, i, j)

  }.ensuring(
    scanTransaction(tr, init)(i)._1.get.activeKeys.get(k) ==
      scanTransaction(tr, init)(j)._1.get.activeKeys.get(k)
  )

  /** If a key of a node appeared for the first time at given point in the transaction, then the state after handling
    * that node is well-defined if and only if the pair key-mapping is in the active keys of the initial state.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param k    The node whose key appeared for the first time.
    * @param j    The step during which the node is processed
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def firstAppearsHandleNodeUndefined(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      n: Node.Action,
      i: BigInt,
  ): Unit = {
    require(0 <= i)
    require(i < 2 * tr.size)

    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(i)._2._2 == n)

    require(n.gkeyOpt.forall(k => firstAppears(tr, init, traverseInFun, traverseOutFun, k, i)))

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    containsAllKeysImpliesDown(tr, init, i)
    unfold(containsKey(scanTransaction(tr, init)(i)._1)(n))
    unfold(containsNodeKey(scanTransaction(tr, init)(i)._1.get)(n))

    handleNodeUndefined(scanTransaction(tr, init)(i)._1.get, scanTransaction(tr, init)(i)._2._1, n)

    unfold(
      inconsistencyCheck(scanTransaction(tr, init)(i)._1.get, n.gkeyOpt, nodeActionKeyMapping(n))
    )
    unfold(inconsistencyCheck(init.get, n.gkeyOpt, nodeActionKeyMapping(n)))

    n.gkeyOpt match {
      case Some(k) =>
        scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
        firstAppearsSameActiveKeysGet(tr, init, k, 0, i)
      case _ => Trivial()
    }

  }.ensuring(
    inconsistencyCheck(init.get, n.gkeyOpt, nodeActionKeyMapping(n)) ==
      scanTransaction(tr, init)(i)._1.get.handleNode(scanTransaction(tr, init)(i)._2._1, n).isLeft
  )

  /** If the key of a node already appeared in the transaction traversal, then its entry in the active keys of the state
    * before that node, is independent of the initial state of the traversal.
    *
    * @param tr    The tree that is being traversed
    * @param init1 The initial state of the first traversal
    * @param init2 The initial state of the first traversal
    * @param node  The node whose key already appeared in the traversals
    * @param i     The step number during which the node is processed
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def appearsBeforeSameActiveKeysGet(
      tr: Tree[(NodeId, Node)],
      init1: Either[KeyInputError, State],
      init2: Either[KeyInputError, State],
      k: GlobalKey,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(0 <= i)
    require(i < 2 * tr.size)

    require(init1.isRight)
    require(init2.isRight)
    require(scanTransaction(tr, init1)(i)._1.isRight)
    require(scanTransaction(tr, init2)(i)._1.isRight)

    require(!doesNotAppearBefore(tr, init1, traverseInFun, traverseOutFun, k, i))

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._3)
    require(
      stateInvariant(init1)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._1,
      )
    )
    require(traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._3)
    require(
      stateInvariant(init2)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init1))
    require(containsAllKeys(tr, init2))

    @pure
    @opaque
    def appearsBeforeSameActiveKeysGetDefined(j: BigInt): Unit = {
      require(0 <= j)
      require(j <= i)
      scanTransactionProp(tr, init1, j, i)
      scanTransactionProp(tr, init2, j, i)
      unfold(propagatesError(scanTransaction(tr, init1)(j)._1, scanTransaction(tr, init1)(i)._1))
      unfold(propagatesError(scanTransaction(tr, init2)(j)._1, scanTransaction(tr, init2)(i)._1))
    }.ensuring(
      scanTransaction(tr, init1)(j)._1.isRight &&
        scanTransaction(tr, init2)(j)._1.isRight
    )

    unfold(doesNotAppearBefore(tr, init1, traverseInFun, traverseOutFun, k, i))

    if (i == 0) {
      Unreachable()
    } else {
      val (s1, n, dir) = scanTransaction(tr, init1)(i - 1)
      val (s2, n2, dir2) = scanTransaction(tr, init2)(i - 1)

      scanIndexingNode(
        tr,
        init1,
        init2,
        traverseInFun,
        traverseOutFun,
        traverseInFun,
        traverseOutFun,
        i - 1,
      )
      scanIndexingState(tr, init1, traverseInFun, traverseOutFun, i)
      scanIndexingState(tr, init2, traverseInFun, traverseOutFun, i)
      appearsBeforeSameActiveKeysGetDefined(i - 1)

      unfold(appearsAtIndex(tr, init1, traverseInFun, traverseOutFun, k, i - 1))

      if (dir == TraversalDirection.Down) {
        unfold(traverseInFun(s1, n))
        unfold(traverseInFun(s2, n2))
        n._2 match {
          case a: Node.Action =>
            // required
            assert(
              appearsAtIndex(
                tr,
                init1,
                traverseInFun,
                traverseOutFun,
                k,
                i - 1,
              ) == (a.gkeyOpt == Some(k))
            )

            scanInvariant(tr, init1, i - 1)
            scanStateNodeCompatibility(tr, init1, i - 1)
            scanInvariant(tr, init2, i - 1)
            scanStateNodeCompatibility(tr, init2, i - 1)

            @pure @opaque
            def appearsBeforeSameActiveKeysGetContainsKey: Unit = {
              containsAllKeysImpliesDown(tr, init1, i - 1)
              unfold(containsKey(s1)(n._2))
              unfold(containsNodeKey(s1.get)(n._2))
              containsAllKeysImpliesDown(tr, init2, i - 1)
              unfold(containsKey(s2)(n._2))
              unfold(containsNodeKey(s2.get)(n._2))

            }.ensuring(containsActionKey(s1.get)(a) && containsActionKey(s2.get)(a))

            appearsBeforeSameActiveKeysGetContainsKey

            if (a.gkeyOpt == Some(k)) {
              handleNodeDifferentStatesActiveKeysGet(
                s1.get,
                s2.get,
                n._1,
                a,
                traverseUnbound(tr)._1,
                traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._1,
                traverseUnbound(tr)._1,
                traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._1,
              )
            } else {
              handleNodeActiveKeysGet(
                s1.get,
                n._1,
                a,
                k,
                traverseUnbound(tr)._1,
                traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._1,
              )
              handleNodeActiveKeysGet(
                s2.get,
                n._1,
                a,
                k,
                traverseUnbound(tr)._1,
                traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._1,
              )
              appearsBeforeSameActiveKeysGet(tr, init1, init2, k, i - 1)
            }

          case r: Node.Rollback =>
            unfold(beginRollback(s1))
            unfold(beginRollback(s2))
            beginRollbackActiveKeysGet(s1.get, k)
            beginRollbackActiveKeysGet(s2.get, k)
            appearsBeforeSameActiveKeysGet(tr, init1, init2, k, i - 1)
        }
      } else {
        unfold(traverseOutFun(s1, n))
        unfold(traverseOutFun(s2, n2))

        n._2 match {
          case a: Node.Action => appearsBeforeSameActiveKeysGet(tr, init1, init2, k, i - 1)
          case r: Node.Rollback =>
            val (i2, sub) = findBeginRollback(tr, init1, init2, i - 1)
            appearsBeforeSameActiveKeysGetDefined(i2)
            activeKeysGetTraverseTransaction(sub, scanTransaction(tr, init1)(i2)._1, k)
            if (!doesNotAppearBefore(tr, init1, traverseInFun, traverseOutFun, k, i2)) {
              appearsBeforeSameActiveKeysGet(tr, init1, init2, k, i2)
              activeKeysGetTraverseTransaction(sub, scanTransaction(tr, init2)(i2)._1, k)
            } else {
              val i1 = findFirstAppears(tr, init1, traverseInFun, traverseOutFun, k, i2, i)

              @pure @opaque
              def appearsBeforeSameActiveKeysGetSameFirstAppears: Unit = {
                appearsBeforeSameActiveKeysGetDefined(i1)
                appearsBeforeSameActiveKeysGetDefined(i1 + 1)

                unfold(firstAppears(tr, init1, traverseInFun, traverseOutFun, k, i1))
                assert(appearsAtIndex(tr, init1, traverseInFun, traverseOutFun, k, i1))
                unfold(appearsAtIndex(tr, init1, traverseInFun, traverseOutFun, k, i1))
                scanIndexingNode(
                  tr,
                  init1,
                  init2,
                  traverseInFun,
                  traverseOutFun,
                  traverseInFun,
                  traverseOutFun,
                  i1,
                )

                scanTransaction(tr, init1)(i1)._2._2 match {
                  case a: Node.Action =>
                    // required
                    assert(
                      scanTransaction(tr, init1)(i1)._2._2 == scanTransaction(tr, init2)(i1)._2._2
                    )

                    @pure @opaque
                    def appearsBeforeSameActiveKeysGetSameFirstAppearsContainsKey: Unit = {
                      containsAllKeysImpliesDown(tr, init1, i1)
                      unfold(containsKey(scanTransaction(tr, init1)(i1)._1)(a))
                      unfold(containsNodeKey(scanTransaction(tr, init1)(i1)._1.get)(a))
                      containsAllKeysImpliesDown(tr, init2, i1)
                      unfold(containsKey(scanTransaction(tr, init2)(i1)._1)(a))
                      unfold(containsNodeKey(scanTransaction(tr, init2)(i1)._1.get)(a))
                    }.ensuring(
                      containsActionKey(scanTransaction(tr, init1)(i1)._1.get)(a) &&
                        containsActionKey(scanTransaction(tr, init2)(i1)._1.get)(a)
                    )

                    appearsBeforeSameActiveKeysGetSameFirstAppearsContainsKey

                    scanIndexingState(tr, init1, traverseInFun, traverseOutFun, i1 + 1)
                    scanIndexingState(tr, init2, traverseInFun, traverseOutFun, i1 + 1)
                    unfold(
                      traverseInFun(
                        scanTransaction(tr, init1)(i1)._1,
                        scanTransaction(tr, init1)(i1)._2,
                      )
                    )
                    unfold(
                      traverseInFun(
                        scanTransaction(tr, init2)(i1)._1,
                        scanTransaction(tr, init2)(i1)._2,
                      )
                    )
                    unfold(
                      handleNode(
                        scanTransaction(tr, init1)(i1)._1,
                        scanTransaction(tr, init1)(i1)._2._1,
                        a,
                      )
                    )
                    unfold(
                      handleNode(
                        scanTransaction(tr, init2)(i1)._1,
                        scanTransaction(tr, init2)(i1)._2._1,
                        a,
                      )
                    )
                    handleSameNodeActiveKeys(
                      scanTransaction(tr, init1)(i1)._1.get,
                      scanTransaction(tr, init2)(i1)._1.get,
                      scanTransaction(tr, init1)(i1)._2._1,
                      a,
                    )

                  case _ => Unreachable()
                }

              }.ensuring(
                scanTransaction(tr, init1)(i1)._1.get.activeKeys.get(k) ==
                  scanTransaction(tr, init2)(i1)._1.get.activeKeys.get(k)
              )

              appearsBeforeSameActiveKeysGetDefined(i1)
              firstAppearsSameActiveKeysGet(tr, init1, k, i2, i1)
              appearsBeforeSameActiveKeysGetSameFirstAppears
              firstAppearsSame(
                tr,
                init1,
                init2,
                traverseInFun,
                traverseOutFun,
                traverseInFun,
                traverseOutFun,
                k,
                i1,
              )
              firstAppearsSameActiveKeysGet(tr, init2, k, i2, i1)
              activeKeysGetTraverseTransaction(sub, scanTransaction(tr, init2)(i2)._1, k)
            }

        }
      }
    }
  }.ensuring(
    scanTransaction(tr, init1)(i)._1.get.activeKeys.get(k) ==
      scanTransaction(tr, init2)(i)._1.get.activeKeys.get(k)
  )

  /** If the key of a node already appeared in the transaction traversal, then the well-definedness of the state after
    * having processed that node is independent of the initial state of the traversal.
    *
    * @param tr    The tree that is being traversed
    * @param init1 The initial state of the first traversal
    * @param init2 The initial state of the first traversal
    * @param n  The node whose key already appeared in the traversals
    * @param i     The step number during which the node is processed
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure @opaque
  def appearsBeforeSameUndefined(
      tr: Tree[(NodeId, Node)],
      init1: Either[KeyInputError, State],
      init2: Either[KeyInputError, State],
      n: Node.Action,
      i: BigInt,
  ): Unit = {
    decreases(i)
    require(0 <= i)
    require(i < 2 * tr.size)

    require(init1.isRight)
    require(init2.isRight)
    require(scanTransaction(tr, init1)(i)._1.isRight)
    require(scanTransaction(tr, init2)(i)._1.isRight)
    require(scanTransaction(tr, init1)(i)._2._2 == n)

    require(
      n.gkeyOpt.forall(k => !doesNotAppearBefore(tr, init1, traverseInFun, traverseOutFun, k, i))
    )

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._3)
    require(
      stateInvariant(init1)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init1.get.locallyCreated, init1.get.consumed, true)._1,
      )
    )
    require(traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._3)
    require(
      stateInvariant(init2)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init2.get.locallyCreated, init2.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init1))
    require(containsAllKeys(tr, init2))

    val (s1, n1, dir) = scanTransaction(tr, init1)(i)
    val (s2, n2, dir2) = scanTransaction(tr, init2)(i)

    scanIndexingNode(
      tr,
      init1,
      init2,
      traverseInFun,
      traverseOutFun,
      traverseInFun,
      traverseOutFun,
      i,
    )

    @pure @opaque
    def appearsBeforeSameUndefinedContainsKey: Unit = {
      containsAllKeysImpliesDown(tr, init1, i)
      unfold(containsKey(s1)(n))
      unfold(containsNodeKey(s1.get)(n))
      containsAllKeysImpliesDown(tr, init2, i)
      unfold(containsKey(s2)(n))
      unfold(containsNodeKey(s2.get)(n))
    }.ensuring(containsActionKey(s1.get)(n) && containsActionKey(s2.get)(n))

    appearsBeforeSameUndefinedContainsKey

    handleNodeUndefined(s1.get, n1._1, n)
    handleNodeUndefined(s2.get, n2._1, n)

    unfold(inconsistencyCheck(s1.get, n.gkeyOpt, nodeActionKeyMapping(n)))
    unfold(inconsistencyCheck(s2.get, n.gkeyOpt, nodeActionKeyMapping(n)))

    n.gkeyOpt match {
      case Some(k) =>
        appearsBeforeSameActiveKeysGet(tr, init1, init2, k, i)
        unfold(inconsistencyCheck(s1.get, n.gkeyOpt, nodeActionKeyMapping(n)))
        unfold(inconsistencyCheck(s2.get, n.gkeyOpt, nodeActionKeyMapping(n)))
      case _ => Trivial()
    }
  }.ensuring(
    scanTransaction(tr, init1)(i)._1.get
      .handleNode(scanTransaction(tr, init1)(i)._2._1, n)
      .isLeft ==
      scanTransaction(tr, init2)(i)._1.get.handleNode(scanTransaction(tr, init2)(i)._2._1, n).isLeft
  )

  /** If the key of a node appears for the first time at a given point of the transaction traversal and the intermediate
    * state at this point is well-defined, then the state after processing the next node will be well-defined as well if
    * and only if the key of the node is mapped to same mapping in the activeKeys of the initial state and the global keys of the
    * empty state (after having collected the keys of the tree).
    *
    * @param tr    The tree that is being traversed
    * @param init The initial state of the traversal
    * @param node  The node whose key appears for the first time
    * @param i     The step number during which the node is processed
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def firstAppearsHandleNodeUndefinedEmpty(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      n: Node.Action,
      i: BigInt,
  ): Unit = {
    require(0 <= i)
    require(i < 2 * tr.size)

    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(i)._2._2 == n)

    require(n.gkeyOpt.isDefined)
    require(firstAppears(tr, init, traverseInFun, traverseOutFun, n.gkeyOpt.get, i))

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    scanIndexingNode(
      tr,
      init,
      Map.empty[GlobalKey, KeyMapping],
      traverseInFun,
      traverseOutFun,
      collectFun,
      (z, t) => z,
      i,
    )
    firstAppearsSame(
      tr,
      init,
      Map.empty[GlobalKey, KeyMapping],
      traverseInFun,
      traverseOutFun,
      collectFun,
      (z, t) => z,
      n.gkeyOpt.get,
      i,
    )
    collectGet(tr, i, n)
    unfold(emptyState(tr))
    unfold(emptyState(tr).globalKeys.contains)
    unfold(emptyState(tr).globalKeys(n.gkeyOpt.get))
    firstAppearsHandleNodeUndefined(tr, init, n, i)

  }.ensuring(
    emptyState(tr).globalKeys.contains(n.gkeyOpt.get) &&
      (inconsistencyCheck(init.get, n.gkeyOpt, emptyState(tr).globalKeys(n.gkeyOpt.get)) ==
        scanTransaction(tr, init)(i)._1.get
          .handleNode(scanTransaction(tr, init)(i)._2._1, n)
          .isLeft)
  )

  /** If the key of a node already appeared in the transaction traversal, the final state of the traversal with the
    * empty state as initial state, and the state before processing the node is well-defined as well, then the state
    * after processing the node will be well-defined as well.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param node The node whose key already appeared
    * @param i    The step number during which the node is processed
    *
    * @see The corresponding latex document for a pen and paper proof
    */
  @pure
  @opaque
  def appearsBeforeHandleNodeUndefinedEmpty(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
      n: Node.Action,
      i: BigInt,
  ): Unit = {
    require(0 <= i)
    require(i < 2 * tr.size)

    require(init.isRight)
    require(scanTransaction(tr, init)(i)._1.isRight)
    require(scanTransaction(tr, init)(i)._2._2 == n)

    require(n.gkeyOpt.isDefined)
    require(!doesNotAppearBefore(tr, init, traverseInFun, traverseOutFun, n.gkeyOpt.get, i))
    require(traverseTransaction(tr, Right[KeyInputError, State](emptyState(tr))).isRight)

    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(
      stateInvariant(Right[KeyInputError, State](emptyState(tr)))(
        traverseUnbound(tr)._1,
        traverseLC(tr, Set.empty[ContractId], Set.empty[ContractId], true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    val rempty: Either[KeyInputError, State] = Right[KeyInputError, State](emptyState(tr))

    scanIndexingNode(
      tr,
      init,
      rempty,
      traverseInFun,
      traverseOutFun,
      traverseInFun,
      traverseOutFun,
      i,
    )

    val si = scanTransaction(tr, init)(i)._1
    val ni = scanTransaction(tr, init)(i)._2
    if (i == 2 * tr.size - 1) {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, 0)
      unfold(traverseOutFun(si, ni))
    } else {
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, i + 1)
      scanIndexingState(tr, rempty, traverseInFun, traverseOutFun, i + 1)
      emptyContainsAllKeys(tr)
      unfold(emptyState(tr))
      SetProperties.emptySubsetOf(init.get.locallyCreated)
      SetProperties.emptySubsetOf(init.get.consumed)
      traverseLCSubsetOf(
        tr,
        Set.empty[ContractId],
        Set.empty[ContractId],
        true,
        init.get.locallyCreated,
        init.get.consumed,
        true,
      )
      traverseTransactionDefined(tr, rempty, i)
      unfold(propagatesError(scanTransaction(tr, rempty)(i)._1, traverseTransaction(tr, rempty)))
      traverseTransactionDefined(tr, rempty, i + 1)
      unfold(
        propagatesError(scanTransaction(tr, rempty)(i + 1)._1, traverseTransaction(tr, rempty))
      )

      unfold(traverseInFun(si, ni))
      unfold(traverseOutFun(si, ni))
      unfold(traverseInFun(scanTransaction(tr, rempty)(i)._1, ni))
      unfold(traverseOutFun(scanTransaction(tr, rempty)(i)._1, ni))

      appearsBeforeSameUndefined(tr, init, rempty, n, i)

//      assert(scanTransaction(tr, init)(i + 1)._1.isRight)
    }

  }.ensuring(
    if (i == 2 * tr.size - 1) {
      traverseTransaction(tr, init).isRight
    } else {
      scanTransaction(tr, init)(i + 1)._1.isRight
    }
  )

  /** If a transaction is defined when it is being traversed with emptyState as the initial state, then it is also defined
    * when starting with a given state if the map obtained by gathering all the key - key mappings of the tree is a submap
    * of the active keys of that state.
    *
    * @param tr The transaction that is being processed
    * @param init The initial state of the traversal
    */
  @pure
  @opaque
  def traverseTransactionEmptyDefined(
      tr: Tree[(NodeId, Node)],
      init: Either[KeyInputError, State],
  ): Unit = {

    require(init.isRight)
    require(traverseTransaction(tr, Right[KeyInputError, State](emptyState(tr))).isRight)
    require(tr.isUnique)
    require(traverseUnbound(tr)._3)
    require(traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._3)
    require(
      stateInvariant(init)(
        traverseUnbound(tr)._1,
        traverseLC(tr, init.get.locallyCreated, init.get.consumed, true)._1,
      )
    )
    require(
      stateInvariant(Right[KeyInputError, State](emptyState(tr)))(
        traverseUnbound(tr)._1,
        traverseLC(tr, Set.empty[ContractId], Set.empty[ContractId], true)._1,
      )
    )
    require(containsAllKeys(tr, init))

    val p: GlobalKey => Boolean =
      k => init.get.activeKeys.get(k) == emptyState(tr).globalKeys.get(k)

    if (
      emptyState(tr).globalKeys.keySet.forall(p) &&
      !traverseTransaction(tr, init).isRight
    ) {
      val i = traverseNotProp(tr, init, traverseInFun, traverseOutFun, x => x.isRight)

      val (si, n, dir) = scanTransaction(tr, init)(i)

      if (dir == TraversalDirection.Up) {
        upDefined(tr, init, i)
      } else {
        scanIndexingState(tr, init, traverseInFun, traverseOutFun, i + 1)
        unfold(traverseInFun(si, n))
        n._2 match {
          case a: Node.Action =>
            a.gkeyOpt match {
              case Some(k) =>
                if (doesNotAppearBefore(tr, init, traverseInFun, traverseOutFun, k, i)) {
                  unfold(appearsAtIndex(tr, init, traverseInFun, traverseOutFun, k, i))
                  unfold(firstAppears(tr, init, traverseInFun, traverseOutFun, k, i))
                  firstAppearsHandleNodeUndefinedEmpty(tr, init, a, i)
                  unfold(inconsistencyCheck(init.get, a.gkeyOpt, emptyState(tr).globalKeys(k)))
                  unfold(inconsistencyCheck(init.get, k, emptyState(tr).globalKeys(k)))
                  MapProperties.keySetContains(emptyState(tr).globalKeys, k)
                  SetProperties.forallContains(emptyState(tr).globalKeys.keySet, p, k)
                } else {
                  appearsBeforeHandleNodeUndefinedEmpty(tr, init, a, i)
                }
              case None() =>
                containsAllKeysImpliesDown(tr, init, i)
                unfold(containsKey(si)(n._2))
                unfold(containsNodeKey(si.get)(n._2))
                handleNodeUndefined(si.get, n._1, a)
                unfold(inconsistencyCheck(si.get, a.gkeyOpt, nodeActionKeyMapping(a)))
            }
          case r: Node.Rollback => unfold(propagatesError(beginRollback(si), si))
        }
      }
    } else if (
      !emptyState(tr).globalKeys.keySet.forall(p) &&
      traverseTransaction(tr, init).isRight
    ) {
      val k: GlobalKey = SetProperties.notForallWitness(emptyState(tr).globalKeys.keySet, p)
      MapProperties.keySetContains(emptyState(tr).globalKeys, k)
      unfold(emptyState(tr))
      val i: BigInt = collectContains(tr, k)

      val (si, n, dir) = scanTransaction(tr, init)(i)
      firstAppearsSame(
        tr,
        Map.empty[GlobalKey, KeyMapping],
        init,
        collectFun,
        (z, t) => z,
        traverseInFun,
        traverseOutFun,
        k,
        i,
      )
      unfold(firstAppears(tr, init, traverseInFun, traverseOutFun, k, i))
      unfold(appearsAtIndex(tr, init, traverseInFun, traverseOutFun, k, i))
      traverseTransactionDefined(tr, init, i)
      traverseTransactionDefined(tr, init, i + 1)
      unfold(propagatesError(si, traverseTransaction(tr, init)))
      unfold(propagatesError(scanTransaction(tr, init)(i + 1)._1, traverseTransaction(tr, init)))
      scanIndexingState(tr, init, traverseInFun, traverseOutFun, i + 1)
      unfold(traverseInFun(si, n))
      n._2 match {
        case a: Node.Action if (dir == TraversalDirection.Down) && (a.gkeyOpt == Some(k)) =>
          @pure
          @opaque
          def traverseTransactionEmptyDefinedContains: Unit = {
            containsAllKeysImpliesDown(tr, init, i)
            unfold(containsKey(si)(n._2))
            scanTransactionProp(tr, init, i)
            unfold(sameGlobalKeys(init, si))
            unfold(sameGlobalKeys(init.get, si))
            containsNodeKeySameGlobalKeys(init.get, si.get, n._2)
            unfold(containsNodeKey(init.get)(n._2))
            activeKeysContainsKey(init.get, a)
          }.ensuring(init.get.activeKeys.contains(k))

          firstAppearsHandleNodeUndefinedEmpty(tr, init, a, i)
          unfold(inconsistencyCheck(init.get, a.gkeyOpt, emptyState(tr).globalKeys(k)))
          unfold(inconsistencyCheck(init.get, k, emptyState(tr).globalKeys(k)))
          traverseTransactionEmptyDefinedContains
          unfold(init.get.activeKeys.contains)
          // required
          assert(!init.get.activeKeys.get(k).exists(_ != emptyState(tr).globalKeys(k)))
          assert(init.get.activeKeys.get(k).isDefined)
        case _ => Unreachable()
      }
    }

  }.ensuring(
    emptyState(tr).globalKeys.keySet.forall(k =>
      init.get.activeKeys.get(k) == emptyState(tr).globalKeys.get(k)
    )
      ==
        traverseTransaction(tr, init).isRight
  )

}
