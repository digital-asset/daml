// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.translation

import java.io.InputStream

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.transaction.{CommittedTransaction, TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}

private[migration] trait TransactionSerializer {

  def serializeTransaction(
      trId: LedgerString,
      transaction: CommittedTransaction,
  ): Either[EncodeError, Array[Byte]]

  def deserializeTransaction(
      trId: LedgerString,
      stream: InputStream,
  ): Either[DecodeError, CommittedTransaction]

}

private[migration] object TransactionSerializer extends TransactionSerializer {

  override def serializeTransaction(
      trId: LedgerString,
      transaction: CommittedTransaction,
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
      stream: InputStream,
  ): Either[DecodeError, CommittedTransaction] =
    ValueSerializer
      .handleDeprecatedValueVersions(
        TransactionCoder
          .decodeTransaction(
            TransactionCoder.EventIdDecoder(trId),
            ValueCoder.CidDecoder,
            TransactionOuterClass.Transaction.parseFrom(
              Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT))
          )
      )
      .map(CommittedTransaction(_))
}
