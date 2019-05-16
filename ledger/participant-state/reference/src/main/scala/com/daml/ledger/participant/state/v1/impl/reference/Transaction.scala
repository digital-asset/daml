// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import com.daml.ledger.participant.state.v1.{CommittedTransaction, SubmittedTransaction}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{ContractId, LedgerString, Party, TransactionId}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.transaction.Transaction.TContractId
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, RelativeContractId, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{Value, ValueCoder}
import com.google.protobuf.ByteString

/**
  * This module provides functions that operate on transactions, transaction ids and transaction nodes.
  */
object Transaction {

  case class TxDelta(
      inputs: Set[AbsoluteContractId], // consumed input contracts
      inputs_nc: Set[AbsoluteContractId], // non-consumed input contracts
      outputs: Map[TContractId, Contract]) // new contracts are outputs

  case class Contract(
      contract: Value.ContractInst[VersionedValue[TContractId]],
      witnesses: Set[Party])

  def computeTxDelta(transaction: SubmittedTransaction): TxDelta = {
    val emptyDelta = TxDelta(Set.empty, Set.empty, Map.empty)
    transaction.fold[TxDelta](GenTransaction.TopDown, emptyDelta) {
      case (delta: TxDelta, nodeEntry) =>
        nodeEntry._2 match {
          case node @ NodeCreate(_, _, _, _, _, _) =>
            TxDelta(
              delta.inputs,
              delta.inputs_nc,
              delta.outputs + (node.coid ->
                Contract(node.coinst, node.stakeholders))
            )
          case node @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _) =>
            TxDelta(
              // get consumed external contracts
              if (node.consuming) node.targetCoid match {
                case absId @ AbsoluteContractId(_) => delta.inputs + absId
                case _ => delta.inputs
              } else delta.inputs,
              // get unconsumed external contracts
              if (!node.consuming) node.targetCoid match {
                case absId @ AbsoluteContractId(_) => delta.inputs_nc + absId
                case _ => delta.inputs_nc
              } else delta.inputs_nc,
              // remove any outputs consumed internally
              if (node.consuming) node.targetCoid match {
                case relId @ RelativeContractId(_) => delta.outputs - relId
                case _ => delta.outputs
              } else delta.outputs
            )
          case _ => delta
        }
    }
  }

  def toAbsTx(txId: TransactionId, tx: SubmittedTransaction): CommittedTransaction =
    tx.mapContractIdAndValue(mkAbsContractId(txId), _.mapContractId(mkAbsContractId(txId)))

  def mkAbsContractId(txId: TransactionId): TContractId => AbsoluteContractId = {
    case RelativeContractId(nid) => AbsoluteContractId(toAbsNodeId(txId, nid))
    case c @ AbsoluteContractId(_) => c
  }

  private val `#` = LedgerString.assertFromString("#")
  private val `:` = LedgerString.assertFromString(":")

  def toAbsNodeId(txId: TransactionId, nid: Value.NodeId): ContractId =
    LedgerString.concat(`#`, txId, `:`, nid.name)

  def encodeTransaction(tx: SubmittedTransaction): ByteString =
    TransactionCoder
      .encodeTransactionWithCustomVersion(
        nidEncoder,
        cidEncoder,
        VersionedTransaction(TransactionVersions.assignVersion(tx), tx))
      .right
      .get
      .toByteString

  def decodeTransaction(tx: ByteString): SubmittedTransaction =
    TransactionCoder
      .decodeVersionedTransaction(
        nidDecoder,
        cidDecoder,
        TransactionOuterClass.Transaction.parseFrom(tx)
      )
      .right
      .get
      .transaction

  // TODO (SM): document this and the methods below and why they are used that
  // way https://github.com/digital-asset/daml/issues/388
  val cidEncoder: ValueCoder.EncodeCid[TContractId] =
    ValueCoder.EncodeCid(
      {
        case AbsoluteContractId(coid) => "abs:" + coid
        case RelativeContractId(txnid) => "rel:" + txnid.index.toString()
      }, {
        case AbsoluteContractId(coid) => (coid, false)
        case RelativeContractId(txnid) => (txnid.index.toString(), true)
      }
    )

  private def toAbsoluteContractId(s: String) =
    Ref.LedgerString
      .fromString(s)
      .left
      .map(e => DecodeError("cannot parse contractId $e"))
      .map(AbsoluteContractId)

  val cidDecoder: ValueCoder.DecodeCid[TContractId] =
    ValueCoder.DecodeCid[TContractId](
      s => {
        val (prefix, s2) = s.splitAt(4)
        prefix match {
          case "abs:" =>
            toAbsoluteContractId(s2)
          case "rel:" =>
            Right(RelativeContractId(Value.NodeId.unsafeFromIndex(Integer.parseInt(s2))))
          case _ =>
            Left(DecodeError("Unexpected prefix: " + prefix))
        }
      },
      (s, r) =>
        if (r)
          Right(RelativeContractId(Value.NodeId.unsafeFromIndex(Integer.parseInt(s))))
        else
          toAbsoluteContractId(s)
    )

  val nidDecoder: String => Either[ValueCoder.DecodeError, Value.NodeId] =
    s => Right(Value.NodeId.unsafeFromIndex(Integer.parseInt(s)))

  val nidEncoder: TransactionCoder.EncodeNid[Value.NodeId] =
    nid => nid.index.toString

}
