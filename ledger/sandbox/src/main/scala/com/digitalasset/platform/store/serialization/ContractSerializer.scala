// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.serialization

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder

trait ContractSerializer {
  def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeContractInstance(blob: Array[Byte])
    : Either[ValueCoder.DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ContractSerializer extends ContractSerializer {

  override def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeContractInstance[AbsoluteContractId](TransactionCoder.AbsCidValEncoder, coinst)
      .map(_.toByteArray())

  override def deserializeContractInstance(blob: Array[Byte])
    : Either[ValueCoder.DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]] =
    TransactionCoder
      .decodeContractInstance[AbsoluteContractId](
        TransactionCoder.AbsCidValDecoder,
        TransactionOuterClass.ContractInstance.parseFrom(
          Decode.damlLfCodedInputStreamFromBytes(blob, Reader.PROTOBUF_RECURSION_LIMIT))
      )

  private def toContractId(s: String) =
    Ref.ContractIdString
      .fromString(s)
      .left
      .map(e => ValueCoder.DecodeError(s"cannot decode contractId: $e"))
      .map(AbsoluteContractId)

}
