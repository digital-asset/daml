// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

trait ValueSerializer {
  def serializeValue(
      value: VersionedValue[AbsoluteContractId]): Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeValue(blob: Array[Byte]): Either[DecodeError, VersionedValue[AbsoluteContractId]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ValueSerializer extends ValueSerializer {

  override def serializeValue(
      value: VersionedValue[AbsoluteContractId]): Either[ValueCoder.EncodeError, Array[Byte]] =
    ValueCoder
      .encodeVersionedValueWithCustomVersion(defaultCidEncode, value)
      .map(_.toByteArray())

  override def deserializeValue(
      blob: Array[Byte]): Either[DecodeError, VersionedValue[AbsoluteContractId]] =
    ValueCoder
      .decodeVersionedValue(
        defaultCidDecode,
        ValueOuterClass.VersionedValue.parseFrom(
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

}
