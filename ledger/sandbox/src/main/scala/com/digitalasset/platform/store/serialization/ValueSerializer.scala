// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.InputStream

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.daml.logging.ThreadLogger

object ValueSerializer {

  def serializeValue(
      value: VersionedValue[ContractId],
      errorContext: => String,
  ): Array[Byte] = {
    ThreadLogger.traceThread("ValueSerializer.serializeValue")
    ValueCoder
      .encodeVersionedValueWithCustomVersion(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)
  }

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue[ContractId] =
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.CidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT)))
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity
      )

  def deserializeValue(
      stream: InputStream,
  ): VersionedValue[ContractId] = {
    ThreadLogger.traceThread("ValueSerializer.deserializeValue")
    deserializeValueHelper(stream, None)
  }

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue[ContractId] = {
    ThreadLogger.traceThread("ValueSerializer.deserializeValue (2)")
    deserializeValueHelper(stream, Some(errorContext))
  }

}
