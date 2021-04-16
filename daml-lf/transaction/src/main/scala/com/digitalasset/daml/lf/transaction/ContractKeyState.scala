package com.daml.lf
package transaction

import com.daml.lf.value.Value.ContractId

sealed abstract class ContractKeyState

object ContractKeyState {

  sealed abstract class NonExisting extends ContractKeyState

  sealed abstract class Existing extends ContractKeyState {
    def coid: ContractId
  }

  case object Consumed extends NonExisting

  case object NotOnLedger extends NonExisting

  final case class Created(create: Node.NodeCreate[ContractId]) extends Existing {
    def coid: ContractId = create.coid
  }

  final case class OnLedger(coid: ContractId) extends Existing

  private[this] def updateIfAbsent[X, V](acc: Map[X, V], key: X, value: => V) =
    acc.get(key).fold(acc)(_ => acc.updated(key, value))

  /** Return all the contract keys references by this transaction.
    * This includes the keys created, exercised, fetched, or lookup, even those
    * that refer to transient contracts or that appear under a roolback node.
    */
  def computeState(tx: HasTxNodes[NodeId, ContractId]) = {
    import GlobalKey.{assertBuild => globalKey}

    tx.fold(Map.empty[GlobalKey, ContractKeyState]) {
      case (acc, (_, node: Node.NodeCreate[ContractId])) =>
        node.key.fold(acc) { key =>
          val gKey = globalKey(node.templateId, key.key)
          acc.updated(gKey, ContractKeyState.Created(node))
        }
      case (acc, (_, node: Node.NodeExercises[_, ContractId])) =>
        node.key.fold(acc) { key =>
          val gKey = globalKey(node.templateId, key.key)
          if (node.consuming)
            acc.updated(gKey, ContractKeyState.Consumed)
          else
            updateIfAbsent(acc, gKey, ContractKeyState.OnLedger(node.targetCoid))
        }
      case (acc, (_, node: Node.NodeFetch[ContractId])) =>
        node.key.fold(acc) { key =>
          updateIfAbsent(
            acc,
            globalKey(node.templateId, key.key),
            ContractKeyState.OnLedger(node.coid),
          )
        }
      case (acc, (_, node: Node.NodeLookupByKey[ContractId])) =>
        val gKey = globalKey(node.templateId, node.key.key)
        updateIfAbsent(
          acc,
          gKey,
          node.result.fold[ContractKeyState](ContractKeyState.NotOnLedger)(
            ContractKeyState.OnLedger
          ),
        )
      case (_, (_, _: Node.NodeRollback[_])) =>
        // TODO https://github.com/digital-asset/daml/issues/8020
        sys.error("rollback node not supported")
    }
  }

}
