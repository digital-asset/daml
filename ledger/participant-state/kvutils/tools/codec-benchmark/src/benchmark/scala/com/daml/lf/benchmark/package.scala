// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.transaction.TransactionCoder.{
  NidDecoder,
  NidEncoder,
  decodeTransaction,
  encodeTransaction
}
import com.daml.lf.transaction.TransactionOuterClass.Transaction
import com.daml.lf.transaction.{NodeId, VersionedTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueCoder._
import com.daml.lf.value.ValueOuterClass.VersionedValue
import com.daml.lf.value.{Value, ValueVersion}
import com.google.protobuf.ByteString

package object benchmark {

  /**
    * This is the output of a successful call to
    * [[com.daml.lf.transaction.TransactionCoder.encodeTransaction]].
    * It's the in-memory representation of the Protobuf message that
    * describes a transaction, not its serialized form.
    */
  private[lf] type EncodedTransaction = Transaction
  private[lf] object EncodedTransaction {
    def deserialize(bytes: ByteString): EncodedTransaction = Transaction.parseFrom(bytes)
  }

  /**
    * This is the output of a successful call to
    * [[com.daml.lf.value.ValueCoder.encodeValue]].
    * It's the in-memory representation of the Protobuf message that
    * describes a value, not its serialized form.
    */
  private[lf] type EncodedValue = VersionedValue
  private[lf] object EncodedValue {
    def deserialize(bytes: ByteString): EncodedValue = VersionedValue.parseFrom(bytes)
  }

  private[lf] type EncodedValueWithType = TypedValue[EncodedValue]
  private[lf] type DecodedValueWithType = TypedValue[DecodedValue]

  /**
    * This is the output of a successful call to
    * [[com.daml.lf.transaction.TransactionCoder.decodeTransaction]].
    * It's the DAML-LF representation of a transaction.
    */
  private[lf] type DecodedTransaction = VersionedTransaction[NodeId, ContractId]

  /**
    * This is the output of a successful call to
    * [[com.daml.lf.value.ValueCoder.decodeValue]].
    * It's the DAML-LF representation of a value.
    */
  private[lf] type DecodedValue = Value[ContractId]

  private[lf] def assertDecode(transaction: EncodedTransaction): DecodedTransaction =
    assertDecode(decode(transaction))

  private[lf] def assertDecode(value: EncodedValue): DecodedValue =
    assertDecode(decode(value))

  private[lf] def assertEncode(transaction: DecodedTransaction): EncodedTransaction =
    assertEncode(encode(transaction))

  private[lf] def assertEncode(value: DecodedValue): EncodedValue =
    assertEncode(encode(value))

  private type DecodeResult[A] = Either[DecodeError, A]
  private type EncodeResult[A] = Either[EncodeError, A]

  private def assertDecode[A](result: DecodeResult[A]): A =
    result.fold(e => sys.error(e.errorMessage), identity)

  private def decode(transaction: EncodedTransaction): DecodeResult[DecodedTransaction] =
    decodeTransaction(NidDecoder, CidDecoder, transaction)

  private def decode(value: EncodedValue): DecodeResult[DecodedValue] =
    decodeValue(CidDecoder, value)

  private def assertEncode[A](result: EncodeResult[A]): A =
    result.fold(e => sys.error(e.errorMessage), identity)

  private def encode(transaction: DecodedTransaction): EncodeResult[EncodedTransaction] =
    encodeTransaction(NidEncoder, CidEncoder, transaction)

  private def encode(value: DecodedValue): EncodeResult[EncodedValue] =
    encodeVersionedValue(CidEncoder, value, ValueVersion.DevOutputVersions)

}
