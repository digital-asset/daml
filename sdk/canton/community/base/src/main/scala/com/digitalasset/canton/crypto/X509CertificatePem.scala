// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import better.files.File
import cats.syntax.either.*
import com.google.protobuf.ByteString

sealed trait X509CertificateEncoder[Encoding] {

  def fromBytes(encoded: ByteString): Either[String, Encoding]

  protected def unwrap(value: Either[String, Encoding]): Encoding =
    value.valueOr(err => throw new IllegalArgumentException(s"Failed to load certificate: $err"))
}

/** A X509 Certificate serialized in PEM format. */
final case class X509CertificatePem private (private val bytes: ByteString) {
  def unwrap: ByteString = bytes

  override def toString: String = bytes.toStringUtf8
}

object X509CertificatePem extends X509CertificateEncoder[X509CertificatePem] {
  def fromString(pem: String): Either[String, X509CertificatePem] =
    fromBytes(ByteString.copyFromUtf8(pem))

  def tryFromString(pem: String): X509CertificatePem = unwrap(fromString(pem))

  def fromFile(pemFile: File): Either[String, X509CertificatePem] = {
    Either
      .catchNonFatal(pemFile.loadBytes)
      .leftMap(err => s"Failed to load PEM file: $err")
      .map(ByteString.copyFrom)
      .flatMap(X509CertificatePem.fromBytes)
  }

  def tryFromFile(pemFile: File): X509CertificatePem = unwrap(fromFile(pemFile))

  override def fromBytes(encoded: ByteString): Either[String, X509CertificatePem] =
    Right(new X509CertificatePem(encoded))
}

/** A X509 Certificate serialized in DER format. */
final case class X509CertificateDer private (private val bytes: ByteString) {
  def unwrap: ByteString = bytes

  override def toString: String = bytes.toStringUtf8
}

object X509CertificateDer extends X509CertificateEncoder[X509CertificateDer] {
  override def fromBytes(der: ByteString): Either[String, X509CertificateDer] = Right(
    new X509CertificateDer(der)
  )
}
