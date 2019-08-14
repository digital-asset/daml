// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.digitalasset.daml.lf.transaction.TransactionOuterClass

trait ContractSerializer {
  def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeContractInstance(
      blob: Array[Byte]): Either[DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ContractSerializer extends ContractSerializer {

  override def serializeContractInstance(coinst: ContractInst[VersionedValue[AbsoluteContractId]])
    : Either[ValueCoder.EncodeError, Array[Byte]] =
    TransactionCoder
      .encodeContractInstance[VersionedValue[AbsoluteContractId]](defaultValEncode, coinst)
      .map(_.toByteArray())

  override def deserializeContractInstance(
      blob: Array[Byte]): Either[DecodeError, ContractInst[VersionedValue[AbsoluteContractId]]] =
    TransactionCoder
      .decodeContractInstance[VersionedValue[AbsoluteContractId]](
        defaultValDecode,
        TransactionOuterClass.ContractInstance.parseFrom(
          Decode.damlLfCodedInputStreamFromBytes(blob, Reader.PROTOBUF_RECURSION_LIMIT)))

  val defaultCidEncode: ValueCoder.EncodeCid[AbsoluteContractId] = ValueCoder.EncodeCid(
    _.coid,
    acid => (acid.coid, false)
  )

  private def toContractId(s: String) =
    Ref.ContractIdString
      .fromString(s)
      .left
      .map(e => DecodeError(s"cannot decode contractId: $e"))
      .map(AbsoluteContractId)

  val defaultCidDecode: ValueCoder.DecodeCid[AbsoluteContractId] = ValueCoder.DecodeCid(
    toContractId, { (i, r) =>
      if (r)
        sys.error("found relative contract id in stored contract instance")
      else toContractId(i)
    }
  )

  private val defaultValEncode: TransactionCoder.EncodeVal[VersionedValue[AbsoluteContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(defaultCidEncode, a).map((a.version, _))

  private val defaultValDecode: ValueOuterClass.VersionedValue => Either[
    ValueCoder.DecodeError,
    VersionedValue[AbsoluteContractId]] =
    a => ValueCoder.decodeVersionedValue(defaultCidDecode, a)

}
