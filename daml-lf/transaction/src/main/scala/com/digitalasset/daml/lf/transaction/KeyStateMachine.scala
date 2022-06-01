// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import cats.syntax.foldable._
import com.daml.lf.data.Ref.{Identifier, TypeConName}
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.lf.transaction.Transaction.{
  DuplicateContractKey,
  InconsistentKeys,
  KeyCreate,
  KeyInactive,
  KeyInput,
  KeyInputError,
  NegativeKeyLookup,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

/** Mimics the key state machines in [[com.daml.lf.speedy.PartialTransaction]]
  * and [[com.daml.lf.transaction.HasTxNodes.contractKeyInputs]].
  *
  * While interpreting a Daml-LF command or traversing [[com.daml.lf.transaction.HasTxNodes]],
  * the key state machine keeps track of the updates to the [[KeyStateMachine.ActiveLedgerState]]
  * since the beginning of the interpretation or traversal.
  * In execution order, the interpretation or traversal must signal every node (to be)
  * to the [[KeyStateMachine.State]] using its methods.
  * Instead of going through a subtree of the transaction rooted at a node `n`,
  * one can start a new traversal of this subtree `n` in [[KeyStateMachine.State.empty]],
  * complete the traversal, and then [[KeyStateMachine.State.advance]] the original state
  * over the whole subtree using the [[KeyStateMachine.State]] at the end of the subtree traversal.
  *
  * If [[byKeyOnly]] is not set,
  * the [[KeyStateMachine.ActiveLedgerState.keys]] and [[KeyStateMachine.State.keyInputs]]
  * keeps track of all keys that appear in any of the nodes, and errors on any internal key inconsistencies.
  * [[com.daml.lf.transaction.Node.LookupByKey]] can be handled with [[KeyStateMachine.State.handleLookup]].
  *
  * If [[byKeyOnly]] is set, the key state machine does not detect inconsistent key lookups
  * and the [[KeyStateMachine.ActiveLedgerState.keys]] and [[KeyStateMachine.State.keyInputs]]
  * keep track only of keys that have been brought into scope using a by-key node
  * or a [[com.daml.lf.transaction.Node.Create]] node.
  * In this mode, [[com.daml.lf.transaction.Node.LookupByKey]] nodes must be
  * handled with [[KeyStateMachine.State.handleLookupWith]],
  * whose second argument takes the contract key resolution to be given to the Daml interpreter instead of
  * [[com.daml.lf.transaction.Node.LookupByKey.result]].
  * This is because the traversal might currently be within a rollback scope
  * that has already archived a contract with the key without a by-key operation
  * (and for this reason the archival is not tracked in [[KeyStateMachine.ActiveLedgerState.keys]]),
  * i.e., the lookup resolves to [[scala.None$]] but the correct key input is [[scala.Some$]] for some contract ID
  * and this may matter after the rollback scope is left.
  *
  * @tparam Nid Type parameter for [[com.daml.lf.transaction.NodeId]]s during interpretation.
  *             Use [[scala.Unit]] for traversals.
  */
class KeyStateMachine[Nid](mode: ContractKeyUniquenessMode) {
  import KeyStateMachine._

  /** Invariants:
    * - [[globalKeyInputs]]'s keyset is a superset of [[activeState]].[[ActiveLedgerState.keys]]'s keyset
    *   and of all the [[ActiveLedgerState.keys]]'s keysets in [[rollbackStack]].
    *   (the superset can be strict in case of by-key nodes inside an already completed Rollback scope)
    * - In mode ![[byKeyOnly]],
    *   the keys belonging to the contracts in [[activeState]].[[ActiveLedgerState.consumedBy]]'s keyset are in [[activeState]].[[ActiveLedgerState.keys]],
    *   and similarly for all [[ActiveLedgerState]]s in [[rollbackStack]].
    *
    * @param globalKeyInputs Grows monotonically. Entries are never overwritten.
    * @param activeState Summarizes the active state of the partial transaction that was visited so far.
    */
  case class State private (
      globalKeyInputs: Map[GlobalKey, KeyInput],
      activeState: ActiveLedgerState[Nid],
      rollbackStack: List[ActiveLedgerState[Nid]],
  ) {

    // TODO(#9386) We probably don't want this expensive check in here.
    require(
      activeState.keys.keySet subsetOf globalKeyInputs.keySet,
      s"Invariant violation: active keys are not a subset of the key inputs. Missing keys: ${activeState.keys.keySet diff globalKeyInputs.keySet}",
    )

    // Adapted from com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.handleCreate
    def handleCreate(node: Node.Create): Either[KeyInputError, State] =
      visitCreate(node.templateId, node.coid, node.key)

    // Adapted from com.daml.lf.speedy.PartialTransaction.insertCreate
    def visitCreate(
        templateId: TypeConName,
        contractId: ContractId,
        key: Option[KeyWithMaintainers],
    ): Either[KeyInputError, State] = {
      // if we have a contract key being added, include it in the list of
      // active keys
      key match {
        case None => Right(this)
        case Some(kWithM) =>
          val ck = GlobalKey(templateId, kWithM.key)

          // Note (MK) Duplicate key checks in Speedy
          // When run in ContractKeyUniquenessMode.On speedy detects duplicate contract keys errors.
          //
          // Just like for modifying `keys` and `globalKeyInputs` we only consider
          // by-key operations, i.e., lookup, exercise and fetch by key as well as creates
          // and archives if the key has been brought into scope before.
          //
          // In the end, those checks mean that ledgers only have to look at inputs and outputs
          // of the transaction and check for conflicts on that while speedy checks for internal
          // conflicts.
          //
          // We have to consider the following cases for conflicts:
          // 1. Create of a new local contract
          //    1.1. KeyInactive in `keys`. This means we saw an archive so the create is valid.
          //    1.2. KeyActive(_) in `keys`. This can either be local contract or a global contract. Both are an error.
          //    1.3. No entry in `keys` and no entry in `globalKeyInputs`. This is valid. Note that the ledger here will then
          //         have to check when committing that there is no active contract with this key before the transaction.
          //    1.4. No entry in `keys` and `KeyInactive` in `globalKeyInputs`. This is valid. Ledgers need the same check
          //         as for 1.3.
          //    1.5. No entry in `keys` and `KeyActive(_)` in `globalKeyInputs`. This is an error. Note that the case where
          //         the global contract has already been archived falls under 1.2.
          // 2. Global key lookups
          //    2.1. Conflicts with other global contracts cannot arise as we query a key at most once.
          //    2.2. Conflicts with local contracts also cannot arise: A successful create will either
          //         2.2.1: Set `globalKeyInputs` to `KeyInactive`.
          //         2.2.2: Not modify `globalKeyInputs` if there already was an entry.
          //         For both of those cases `globalKeyInputs` already had an entry which means
          //         we would use that as a cached result and not query the ledger.
          val keys = activeState.keys
          val conflict = keys.get(ck) match {
            case Some(keyMapping) => keyMapping.isDefined
            case None => globalKeyInputs.get(ck).exists(_.isActive)
          }

          // See https://github.com/digital-asset/daml/pull/14012: we only need to look in keyInputs.
          val newKeyInputs =
            if (globalKeyInputs.contains(ck)) globalKeyInputs
            else globalKeyInputs.updated(ck, KeyCreate)
          Either.cond(
            !conflict || mode == ContractKeyUniquenessMode.Off,
            this.copy(
              activeState = activeState.copy(keys = keys.updated(ck, KeyActive(contractId))),
              globalKeyInputs = newKeyInputs,
            ),
            DuplicateContractKey(ck),
          )
      }
    }

    // Adapted from com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.handleExercise
    def handleExercise(nid: Nid, exe: Node.Exercise): Either[KeyInputError, State] = {
      for {
        state <-
          if (exe.byKey || !mode.byKeyOnly)
            assertKeyMapping(exe.templateId, exe.targetCoid, exe.key)
          else Right(this)
      } yield state.visitExercise(nid, exe.templateId, exe.targetCoid, exe.key, exe.consuming)
    }

    // extracted from com.daml.lf.speedy.PartialTransaction.beginExercises
    /* Omits the key lookup that are done in com.daml.lf.speedy.Compiler.compileChoiceByKey for byKey nodes,
     * which translates to a resolveKey below.
     * Factors out the stuff common to com.daml.lf.speedy.Compiler.compileTemplateChoice
     */
    def visitExercise(
        nodeId: Nid,
        templateId: TypeConName,
        targetId: ContractId,
        mbKey: Option[KeyWithMaintainers],
        consuming: Boolean,
    ): State = {
      if (consuming) {
        val consumedState = activeState.consume(targetId, nodeId)
        val newActiveState = mbKey match {
          case Some(kWithM) =>
            val gkey = GlobalKey(templateId, kWithM.key)
            val keys = consumedState.keys
            val updatedKeys = keys.updated(gkey, KeyInactive)

            if (mode.byKeyOnly) {
              keys.get(gkey).orElse(globalKeyInputs.get(gkey).map(_.toKeyMapping)) match {
                // An archive can only mark a key as inactive
                // if it was brought into scope before.
                case Some(KeyActive(cid)) if cid == targetId =>
                  consumedState.copy(keys = updatedKeys)
                // If the key was not in scope or mapped to a different cid, we don’t change keys. Instead we will do
                // an activeness check when looking it up later.
                case _ => consumedState
              }
            } else {
              consumedState.copy(keys = updatedKeys)
            }
          case None => consumedState
        }
        this.copy(activeState = newActiveState)
      } else this
    }

    // Adapted from com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.handleLookup
    def handleLookup(lookup: Node.LookupByKey): Either[KeyInputError, State] = {
      // If the key has not yet been resolved, we use the resolution from the lookup node,
      // but this only makes sense if `activeState.keys` is updated by every node and not only by by-key nodes.
      require(!mode.byKeyOnly, "This method can only be used if all key nodes are considered")
      handleLookupWith(lookup, lookup.result)
    }

    def handleLookupWith(
        lookup: Node.LookupByKey,
        keyInput: Option[ContractId],
    ): Either[KeyInputError, State] = {
      val gk = GlobalKey.assertBuild(lookup.templateId, lookup.key.key)
      // Differences to com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.handleLookup
      // - If the key is not in activeState.keys, we look at the key inputs as part of resolving the key rather than setKeyMapping.
      // - If the lookup result is some consumed contract ID, then by the invariant
      //   This will fail the check below with the same reason.
      val (keyMapping, next) = resolveKey(gk) match {
        case Right(result) => result
        case Left(handle) => handle(keyInput)
      }
      Either.cond(
        keyMapping == lookup.result,
        next,
        InconsistentKeys(gk),
      )
    }

    // adapted from com.daml.lf.speedy.SBuiltin.SBUKeyBuiltin.execute
    /* Used by:
     * - com.daml.lf.speedy.Compiler.compileFetchByKey
     * - com.daml.lf.speedy.Compiler.compileChoiceByKey
     * - com.daml.lf.speedy.Compiler.compileLookupByKey
     */
    def resolveKey(
        gkey: GlobalKey
    ): Either[Option[ContractId] => (KeyMapping, State), (KeyMapping, State)] = {
      val keys = activeState.keys
      keys.get(gkey) match {
        case Some(keyMapping) => Right(keyMapping -> this)
        case None =>
          // Check if we have a cached key input.
          globalKeyInputs.get(gkey) match {
            case Some(keyInput) =>
              val keyMapping = keyInput.toKeyMapping
              Right(
                keyMapping -> this.copy(
                  activeState = activeState.copy(keys = keys.updated(gkey, keyMapping))
                )
              )
            case None =>
              // if we cannot find it here, send help, and make sure to update keys after
              // that.
              def handleResult(result: Option[ContractId]): (KeyMapping, State) = {
                // update key inputs. Create nodes never resolve the key, so it's safe to put a NegativeKeyLookup in there
                val keyInput = result.fold[KeyInput](NegativeKeyLookup)(Transaction.KeyActive)
                val newKeyInputs = globalKeyInputs.updated(gkey, keyInput)

                result match {
                  case Some(cid) if !activeState.consumedBy.contains(cid) =>
                    val active = KeyActive(cid)
                    active -> this.copy(
                      activeState = activeState.copy(keys = keys.updated(gkey, active)),
                      globalKeyInputs = newKeyInputs,
                    )
                  case _ =>
                    KeyInactive -> this.copy(
                      activeState = activeState.copy(
                        keys = keys.updated(gkey, KeyInactive)
                      ),
                      globalKeyInputs = newKeyInputs,
                    )
                }
              }
              Left(handleResult)
          }
      }
    }

    def handleFetch(node: Node.Fetch): Either[KeyInputError, State] = {
      if (node.byKey || !mode.byKeyOnly) {
        assertKeyMapping(node.templateId, node.coid, node.key)
      } else Right(this)
    }

    // Adapted from com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.assertKeyMapping
    /* We don't look at the local contract IDs because the local contract IDs do not follow the execution order.
     * So we need additional reasoning to justify that contracts created in nodes not happening before cannot influence the computation.
     * This is not obvious for maliciously crafted transactions.
     * For honestly created transactions (internally consistent), the created contract must be visible to the node
     * and therefore happen before. This means that the create node was visited before
     * and brought the contract key in scope. So activeState.keys contains the expected value
     * and the setKeyMapping reduces to a putIfAbsent, which happens in resolveKey
     */
    private def assertKeyMapping(
        templateId: Identifier,
        cid: Value.ContractId,
        optKey: Option[Node.KeyWithMaintainers],
    ): Either[KeyInputError, State] = {
      optKey match {
        case None => Right(this)
        case Some(kWithM) =>
          val gk = GlobalKey.assertBuild(templateId, kWithM.key)
          val (keyMapping, next) = resolveKey(gk) match {
            case Right(result) => result
            case Left(handle) => handle(Some(cid))
          }
          // Since keys is defined only where keyInputs is defined, we don't need to update keyInputs.
          Either.cond(keyMapping == KeyActive(cid), next, InconsistentKeys(gk))
      }
    }

    def handleLeaf(leaf: Node.LeafOnlyAction): Either[KeyInputError, State] =
      leaf match {
        case create: Node.Create => handleCreate(create)
        case fetch: Node.Fetch => handleFetch(fetch)
        case lookup: Node.LookupByKey => handleLookup(lookup)
      }

    // Takes over the role of com.daml.lf.speedy.PartialTransaction.activeState
    // and com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.beginRollback
    // To be called when interpretation enters a try block or traversal enters a Rollback node
    def beginRollback(): State = {
      this.copy(rollbackStack = activeState +: rollbackStack)
    }

    // Takes over the role of com.daml.lf.speedy.PartialTransaction.resetActiveState
    // and com.daml.lf.transaction.HasTxNodes.contractKeyInputs.State.endRollback
    // To be called when interpretation does insert a Rollback node or traversal exits a Rollback node.
    def endRollback(): State = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case headState :: tailStack => this.copy(activeState = headState, rollbackStack = tailStack)
    }

    // To be called if interpretation notices that a try block did not lead to a Rollback node
    def dropRollback(): State = rollbackStack match {
      case Nil => throw new IllegalStateException("Not inside a rollback scope")
      case _ :: tailStack => this.copy(rollbackStack = tailStack)
    }

    def withinRollbackScope: Boolean = rollbackStack.nonEmpty

    /** Let `resolver` be a [[KeyResolver]] that can be used during interpretation to obtain a transaction `tx`.
      * Let `this` state be the result of traversing `tx` up to a node `n` exclusive using `resolver`.
      * Let `substate` be the state obtained after fully traversing the subtree rooted at `n` starting from [[State.empty]],
      * using the resolver [[projectKeyResolver]](`resolver`).
      * Then, the returned state is the same as if
      * the traversal of tx continued from `this` state through the whole subtree rooted at `n` using `resolver`.
      *
      * If the traversal of the subtree rooted at `n` from `this` using the projected resolver fails,
      * then so does advancing `this` using `substate` with the same error.
      * If traversal of `n` from both `this` and [[State.empty]] fail,
      * the error kind ([[DuplicateContractKey]] vs [[InconsistentKeys]]) may differ.
      * For example, for `tx = [ Create c0 (key = k), Exe c1 [ Create c2 (key = k), LookupByKey k -> None ] ]`
      * with `n` being the `Exe c1` node, traversing all of `tx` errors with [[DuplicateContractKey]] on `Create c2`
      * whereas traversing the `n` subtree from [[State.empty]] errors with [[InconsistentKeys]] on `LookupByKey k`.
      */
    // TODO(#9386) add test cases
    def advance(substate: State): Either[KeyInputError, State] = {
      require(
        !substate.withinRollbackScope,
        "Cannot lift a state over a substate with unfinished rollback scopes",
      )

      for {
        // If we look only at by-key nodes, then we must now check for duplicate keys due to create nodes
        // that brought the key in scope without asking the resolver.
        _ <-
          if (mode == ContractKeyUniquenessMode.On) {
            val keys = this.activeState.keys
            substate.globalKeyInputs.toSeq.traverse_ {
              case (key, KeyCreate) =>
                Either.cond(
                  keys
                    .get(key)
                    .orElse(this.globalKeyInputs.get(key).map(_.toKeyMapping))
                    .forall(_ == KeyInactive),
                  (),
                  DuplicateContractKey(key),
                )
              case _ => Right(())
            }
          } else Right(())
      } yield {
        val next = ActiveLedgerState(
          consumedBy = this.activeState.consumedBy ++ substate.activeState.consumedBy,
          keys = this.activeState.keys.concat(substate.activeState.keys),
        )

        this.copy(
          globalKeyInputs = substate.globalKeyInputs.concat(this.globalKeyInputs),
          activeState = next,
        )
      }
    }

    /** @see advance */
    def projectKeyResolver(resolver: KeyResolver): KeyResolver = {
      val keys = activeState.keys
      val consumed = activeState.consumedBy.keySet
      resolver.map { case (key, keyMapping) =>
        val newKeyInput = keys.getOrElse(key, keyMapping.filterNot(consumed.contains))
        key -> newKeyInput
      }
    }
  }

  object State {
    val empty: State = new State(Map.empty, ActiveLedgerState.empty, List.empty)
  }
}

object KeyStateMachine {

  /** Represents the answers for [[com.daml.lf.engine.ResultNeedKey]] requests
    * that may arise during Daml interpretation.
    */
  type KeyResolver = Map[GlobalKey, KeyMapping]

  type KeyMapping = Option[Value.ContractId]
  // There is no active contract with the given key.
  val KeyInactive = None
  // The contract with the given cid is active and has the given key.
  val KeyActive = Some

  implicit class KeyInputOps(val keyInput: KeyInput) extends AnyVal {
    def isActive: Boolean = keyInput match {
      case _: KeyInactive => false
      case _: Transaction.KeyActive => true
    }

    def toKeyMapping: KeyMapping = keyInput match {
      case _: KeyInactive => KeyInactive
      case Transaction.KeyActive(cid) => KeyActive(cid)
    }
  }

  // Generalized from com.daml.lf.speedy.PartialTransaction.ActiveLedgerState
  final case class ActiveLedgerState[+Nid](
      consumedBy: Map[ContractId, Nid],
      keys: Map[GlobalKey, KeyMapping],
  ) {
    def consume[Nid2 >: Nid](contractId: ContractId, nodeId: Nid2): ActiveLedgerState[Nid2] =
      this.copy(consumedBy = consumedBy.updated(contractId, nodeId))
  }

  object ActiveLedgerState {
    private val EMPTY: ActiveLedgerState[Nothing] = ActiveLedgerState(Map.empty, Map.empty)
    def empty[Nid]: ActiveLedgerState[Nid] = EMPTY
  }
}
