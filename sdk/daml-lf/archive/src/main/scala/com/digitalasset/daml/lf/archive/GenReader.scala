// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import com.google.protobuf.{ByteString, CodedInputStream, Message}

import scala.util.Using

final class GenReader[X] private (
    val fromCodedInputStream: CodedInputStream => Either[Error, X]
) {

  def fromInputStream(is: java.io.InputStream): Either[Error, X] =
    fromCodedInputStream(CodedInputStream.newInstance(is))

  def fromByteArray(bytes: Array[Byte]): Either[Error, X] =
    fromCodedInputStream(CodedInputStream.newInstance(bytes))

  @throws[Error]
  def assertFromByteArray(bytes: Array[Byte]): X =
    assertRight(fromByteArray(bytes))

  def fromByteString(bytes: ByteString): Either[Error, X] =
    fromCodedInputStream(CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer()))

  @throws[Error]
  def assertFromByteString(bytes: ByteString) =
    assertRight(fromByteString(bytes))

  def fromBytes(bytes: data.Bytes): Either[Error, X] =
    fromByteString(bytes.toByteString)

  @throws[Error]
  def assertFromBytes(bytes: data.Bytes) =
    assertRight(fromBytes(bytes))

  def fromFile(file: java.nio.file.Path): Either[Error, X] =
    Using.resource(java.nio.file.Files.newInputStream(file))(fromInputStream)

  def fromFile(file: java.io.File): Either[Error, X] =
    fromFile(file.toPath)

  private[archive] def andThen[Y](f: X => Either[Error, Y]): GenReader[Y] =
    new GenReader(fromCodedInputStream(_).flatMap(f))

}

object GenReader {

  private[archive] def apply[M <: Message](
      where: String,
      parse: CodedInputStream => M,
  ): GenReader[M] =
    new GenReader[M](cos =>
      for {
        msg <- attempt(where)(parse(cos))
      } yield msg
    )

  private[archive] def fail[X](err: Error): GenReader[X] = new GenReader[X](_ => Left(err))

}
