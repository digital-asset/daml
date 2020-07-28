// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.translation

import java.io.InputStream

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.transaction.{Transaction => Tx, TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}

trait TransactionSerializer {

  def serializeTransaction(
      trId: LedgerString,
      transaction: Tx.CommittedTransaction,
  ): Either[EncodeError, Array[Byte]]

  def deserializeTransaction(
      trId: LedgerString,
      stream: InputStream,
  ): Either[DecodeError, Tx.CommittedTransaction]

}

object TransactionSerializer extends TransactionSerializer {

  override def serializeTransaction(
      trId: LedgerString,
      transaction: Tx.CommittedTransaction,
  ): Either[EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeTransaction(
        TransactionCoder.EventIdEncoder(trId),
        ValueCoder.CidEncoder,
        transaction
      )
      .map(_.toByteArray())

  override def deserializeTransaction(
      trId: LedgerString,
      stream: InputStream): Either[DecodeError, Tx.CommittedTransaction] =
    TransactionCoder
      .decodeTransaction(
        TransactionCoder.EventIdDecoder(trId),
        ValueCoder.CidDecoder,
        TransactionOuterClass.Transaction.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT))
      )
      .map(Tx.CommittedTransaction(_))
}
