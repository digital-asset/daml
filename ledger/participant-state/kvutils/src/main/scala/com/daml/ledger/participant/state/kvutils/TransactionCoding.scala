package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.transaction.{
  Transaction,
  TransactionOuterClass,
  TransactionVersions,
  VersionedTransaction
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  NodeId,
  RelativeContractId
}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.ValueOuterClass

import scala.util.Try

private[kvutils] object TransactionCoding {

  import com.digitalasset.daml.lf.transaction.TransactionCoder
  import com.digitalasset.daml.lf.value.ValueCoder

  def encodeTransaction(tx: SubmittedTransaction): TransactionOuterClass.Transaction = {
    TransactionCoder
      .encodeTransactionWithCustomVersion(
        nidEncoder,
        cidEncoder,
        VersionedTransaction(TransactionVersions.assignVersion(tx), tx))
      .fold(err => sys.error(s"encodeTransaction error: $err"), identity)
  }

  def decodeTransaction(tx: TransactionOuterClass.Transaction): SubmittedTransaction = {
    TransactionCoder
      .decodeVersionedTransaction(
        nidDecoder,
        cidDecoder,
        tx
      )
      .fold(err => sys.error(s"decodeTransaction error: $err"), _.transaction)
  }

  // FIXME(JM): Should we have a well-defined schema for this?
  private val cidEncoder: ValueCoder.EncodeCid[ContractId] = {
    val asStruct: ContractId => (String, Boolean) = {
      case RelativeContractId(nid) => (s"~${nid.index}", true)
      case AbsoluteContractId(coid) => (s"$coid", false)
    }
    ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
  }
  private val cidDecoder: ValueCoder.DecodeCid[ContractId] = {
    def fromString(x: String): Either[DecodeError, ContractId] = {
      if (x.startsWith("~"))
        Try(x.tail.toInt).toOption match {
          case None =>
            Left(DecodeError(s"Invalid relative contract id: $x"))
          case Some(i) =>
            Right(RelativeContractId(NodeId.unsafeFromIndex(i)))
        } else
        Right(AbsoluteContractId(x))
    }

    ValueCoder.DecodeCid(
      fromString,
      { case (i, _) => fromString(i) }
    )
  }

  private val nidDecoder: String => Either[ValueCoder.DecodeError, NodeId] =
    nid => Right(NodeId.unsafeFromIndex(nid.toInt))
  private val nidEncoder: TransactionCoder.EncodeNid[NodeId] =
    nid => nid.index.toString
  private val valEncoder: TransactionCoder.EncodeVal[Transaction.Value[ContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(cidEncoder, a).map((a.version, _))
  private val valDecoder: ValueOuterClass.VersionedValue => Either[
    ValueCoder.DecodeError,
    Transaction.Value[ContractId]] =
    a => ValueCoder.decodeVersionedValue(cidDecoder, a)

}
