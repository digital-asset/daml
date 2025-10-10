// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.either.*
import com.daml.ledger.api.v2.crypto
import com.daml.ledger.api.v2.crypto.{
  Signature as LAPISignature,
  SignatureFormat as LAPISignatureFormat,
}
import com.digitalasset.canton.crypto.{
  Fingerprint,
  Signature,
  SignatureFormat,
  SigningAlgorithmSpec,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidField
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

import scala.annotation.nowarn

object CryptoValidator {

  def validateSignature(
      cryptoSignatureP: crypto.Signature,
      fieldName: String,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Signature] = {
    val LAPISignature(formatP, signatureP, signedByP, signingAlgorithmSpecP) =
      cryptoSignatureP
    for {
      format <- validateSignatureFormat(formatP, "format")
      signature = signatureP
      signedBy <- Fingerprint
        .fromProtoPrimitive(signedByP)
        .leftMap(err => invalidField(fieldName = fieldName, message = err.message))
      signingAlgorithmSpec <- validateSigningAlgorithmSpec(signingAlgorithmSpecP, fieldName)
    } yield Signature.fromExternalSigning(format, signature, signedBy, signingAlgorithmSpec)
  }

  private def validateSignatureFormat(
      formatP: LAPISignatureFormat,
      fieldName: String,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, SignatureFormat] =
    formatP match {
      case LAPISignatureFormat.SIGNATURE_FORMAT_DER => Right(SignatureFormat.Der)
      case LAPISignatureFormat.SIGNATURE_FORMAT_CONCAT => Right(SignatureFormat.Concat)
      case LAPISignatureFormat.SIGNATURE_FORMAT_RAW =>
        Right(SignatureFormat.Raw: @nowarn("msg=Raw in object SignatureFormat is deprecated"))
      case LAPISignatureFormat.SIGNATURE_FORMAT_SYMBOLIC => Right(SignatureFormat.Symbolic)
      case LAPISignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
        Left(invalidField(fieldName, message = "Signature format must be specified"))
      case other: LAPISignatureFormat.Unrecognized =>
        Left(invalidField(fieldName, message = s"Signing algorithm spec $other not supported"))
    }

  private def validateSigningAlgorithmSpec(
      signingAlgorithmSpecP: crypto.SigningAlgorithmSpec,
      fieldName: String,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, SigningAlgorithmSpec] =
    signingAlgorithmSpecP match {
      case crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519 =>
        Right(SigningAlgorithmSpec.Ed25519)
      case crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256 =>
        Right(SigningAlgorithmSpec.EcDsaSha256)
      case crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384 =>
        Right(SigningAlgorithmSpec.EcDsaSha384)
      case other =>
        Left(invalidField(fieldName, message = s"Signing algorithm spec $other not supported"))
    }

}
