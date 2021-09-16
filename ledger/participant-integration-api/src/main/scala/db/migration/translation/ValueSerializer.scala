// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform
package db.migration.translation

import java.io.InputStream

import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.google.protobuf.CodedInputStream
import org.slf4j.LoggerFactory

private[migration] object ValueSerializer {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[translation] object DeprecatedValueVersionsError {
    val DeprecatedValueVersions = Set("1", "2", "3", "4", "5")
    val UnsupportedErrorMessage = """Unsupported transaction version '(\d)'""".r

    def unapply[X](arg: Either[ValueCoder.DecodeError, X]): Option[String] =
      arg match {
        case Left(ValueCoder.DecodeError(UnsupportedErrorMessage(version)))
            if DeprecatedValueVersions(version) =>
          Some(version)
        case _ => None
      }
  }

  private[translation] def handleDeprecatedValueVersions[X](
      x: Either[ValueCoder.DecodeError, X]
  ): Either[ValueCoder.DecodeError, X] = {
    x match {
      case DeprecatedValueVersionsError(deprecatedVersion) =>
        logger.error(
          s"*** Deserialization of value version $deprecatedVersion is not supported by the SDK 1.7.0 or later. ***"
        )
        logger.error(
          s"*** Please upgrade your sandbox database by upgrading your SDK to 1.6 first. ***"
        )
      case _ =>
    }
    x
  }

  def serializeValue(
      value: VersionedValue,
      errorContext: => String,
  ): Array[Byte] = store.serialization.ValueSerializer.serializeValue(value, errorContext)

  private[this] val PROTOBUF_RECURSION_LIMIT = 1000

  def lfValueCodedInputStream(stream: InputStream) = {
    val cos = CodedInputStream.newInstance(stream)
    cos.setRecursionLimit(PROTOBUF_RECURSION_LIMIT)
    cos
  }

  private[this] def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue =
    handleDeprecatedValueVersions(
      ValueCoder
        .decodeVersionedValue(
          ValueCoder.CidDecoder,
          ValueOuterClass.VersionedValue.parseFrom(lfValueCodedInputStream(stream)),
        )
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
