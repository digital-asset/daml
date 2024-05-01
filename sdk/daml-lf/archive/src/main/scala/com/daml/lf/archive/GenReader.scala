// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import com.google.protobuf.{ByteString, CodedInputStream}
import com.daml.lf.data

import scala.util.Using

final class GenReader[X] private[archive] (
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

  def fromFile(file: java.nio.file.Path): Either[Error, X] =
    Using.resource(java.nio.file.Files.newInputStream(file))(fromInputStream)

  def fromFile(file: java.io.File): Either[Error, X] =
    fromFile(file.toPath)

  private[archive] def andThen[Y](f: X => Either[Error, Y]): GenReader[Y] =
    new GenReader(fromCodedInputStream(_).flatMap(f))

}
