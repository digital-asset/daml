package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions.{
  absoluteContractIdToLogEntryId,
  absoluteContractIdToStateKey,
  relativeContractIdToStateKey
}
import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId, RelativeContractId}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._

private[kvutils] object InputsAndEffects {

  /** The effects of the transaction, that is what contracts
    * were consumed and created, and what contract keys were updated.
    */
  final case class Effects(
      /** The contracts consumed by this transaction.
        * When committing the transaction these contracts must be marked consumed.
        * A contract should be marked consumed when the transaction is committed,
        * regardless of the ledger effective time of the transaction (e.g. a transaction
        * with an earlier ledger effective time that gets committed later would find the
        * contract inactive).
        */
      consumedContracts: List[DamlStateKey],
      /** The contracts created by this transaction.
        * When the transaction is committed, keys marking the activeness of these
        * contracts should be created. The key should be a combination of the transaction
        * id and the relative contract id (that is, the node index).
        */
      createdContracts: List[DamlStateKey]

      // FIXME(JM): updated contract keys
  )

  def computeInputs(tx: SubmittedTransaction): (List[DamlLogEntryId], List[DamlStateKey]) = {
    // FIXME(JM): Get referenced packages from the transaction (once they're added to it)
    def addInput(inputs: List[DamlLogEntryId], coid: ContractId): List[DamlLogEntryId] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToLogEntryId(acoid)._1 :: inputs
        case _ =>
          inputs
      }

    tx.fold(GenTransaction.TopDown, (List.empty[DamlLogEntryId], List.empty[DamlStateKey])) {
      case ((logEntryInputs, stateInputs), (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            (addInput(logEntryInputs, fetch.coid), stateInputs)
          case create: NodeCreate[_, _] =>
            (logEntryInputs, stateInputs)
          case exe: NodeExercises[_, ContractId, _] =>
            (
              addInput(logEntryInputs, exe.targetCoid),
              (exe.consuming, exe.targetCoid) match {
                case (true, acoid: AbsoluteContractId) =>
                  absoluteContractIdToStateKey(acoid) :: stateInputs
                case _ =>
                  stateInputs
              }
            )
          case l: NodeLookupByKey[_, _] =>
            // FIXME(JM): track fetched keys
            (logEntryInputs, stateInputs)
        }
    }
  }

  def computeEffects(entryId: DamlLogEntryId, tx: SubmittedTransaction): Effects = {
    tx.fold(GenTransaction.TopDown, Effects(List.empty, List.empty)) {
      case (effects, (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            effects
          case create: NodeCreate[_, _] =>
            // FIXME(JM): Track created keys
            effects.copy(
              createdContracts =
                relativeContractIdToStateKey(entryId, create.coid.asInstanceOf[RelativeContractId])
                  :: effects.createdContracts
            )
          case exe: NodeExercises[_, ContractId, _] =>
            if (exe.consuming) {
              exe.targetCoid match {
                case acoid: AbsoluteContractId =>
                  effects.copy(
                    consumedContracts = absoluteContractIdToStateKey(acoid) :: effects.consumedContracts
                  )
                case _ =>
                  effects
              }
            } else {
              effects
            }
          case l: NodeLookupByKey[_, _] =>
            effects
        }
    }
  }

}
