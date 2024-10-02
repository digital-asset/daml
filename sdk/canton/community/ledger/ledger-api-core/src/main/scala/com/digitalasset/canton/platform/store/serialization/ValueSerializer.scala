// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.serialization

import com.digitalasset.daml.lf.value.Value.VersionedValue
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

import java.io.InputStream

private[platform] object ValueSerializer {

  def serializeValue(
      value: VersionedValue,
      errorContext: => String,
  ): Array[Byte] =
    ValueCoder
      .encodeVersionedValue(versionedValue = value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue =
    ValueCoder
      .decodeVersionedValue(
        protoValue0 = ValueOuterClass.VersionedValue.parseFrom(stream)
      )
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity,
      )

  def deserializeValue(
      stream: InputStream
  ): VersionedValue =
    deserializeValueHelper(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue =
    deserializeValueHelper(stream, Some(errorContext))

}
