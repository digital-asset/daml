// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.provider.jce.JceJavaConverter
import com.digitalasset.canton.crypto.provider.tink.TinkKeyFormat
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.proto.OutputPrefixType

import java.security.PublicKey as JPublicKey
import scala.collection.concurrent.TrieMap

object CryptoKeyValidation {

  // TODO(#15632): Make this a real cache with an eviction rule
  // keeps track of the public keys that have been validated
  private lazy val validatedPublicKeys: TrieMap[PublicKey, Either[KeyParseAndValidateError, Unit]] =
    TrieMap.empty

  private[crypto] def parseAndValidateTinkKey(
      publicKey: PublicKey
  ): Either[KeyParseAndValidateError, KeysetHandle] =
    for {
      /* deserialize the public key using Tink and only then regenerate fingerprint. If the
       * deserialization fails, the format is not correct and we fail validation.
       */
      handle <- TinkKeyFormat
        .deserializeHandle(publicKey.key)
        .leftMap(err => KeyParseAndValidateError(err.show))
      fingerprint <- TinkKeyFormat
        .fingerprint(handle, HashAlgorithm.Sha256)
        .leftMap(KeyParseAndValidateError)
      _ <- Either.cond(
        fingerprint == publicKey.id,
        (),
        KeyParseAndValidateError(
          s"The regenerated fingerprint $fingerprint does not match the fingerprint of the object: ${publicKey.id}"
        ),
      )
      outputPrefixType = handle.getKeysetInfo.getKeyInfo(0).getOutputPrefixType
      _ <- Either.cond(
        (outputPrefixType == OutputPrefixType.RAW) || (outputPrefixType == OutputPrefixType.TINK),
        (),
        KeyParseAndValidateError(
          s"Wrong output prefix type: expected RAW got $outputPrefixType"
        ),
      )
    } yield handle

  private[crypto] def parseAndValidateDerOrRawKey(
      publicKey: PublicKey
  ): Either[KeyParseAndValidateError, JPublicKey] = {
    val fingerprint = Fingerprint.create(publicKey.key)
    for {
      // the fingerprint must be regenerated before we convert the key
      _ <- Either.cond(
        fingerprint == publicKey.id,
        (),
        KeyParseAndValidateError(
          s"The regenerated fingerprint $fingerprint does not match the fingerprint of the object: ${publicKey.id}"
        ),
      )
      // we try to convert the key to a Java key to ensure the format is correct
      jceJavaConverter = new JceJavaConverter(
        SigningKeyScheme.allSchemes,
        EncryptionKeyScheme.allSchemes,
      )
      javaPublicKeyAndAlgorithm <- jceJavaConverter
        .toJava(publicKey)
        .leftMap(err => KeyParseAndValidateError(err.show))
      (_, javaPublicKey) = javaPublicKeyAndAlgorithm
    } yield javaPublicKey
  }

  // TODO(#15634): Verify crypto scheme as part of key validation
  /** Parses and validates a public key. This includes recomputing the fingerprint and verifying that it matches the
    * id of the key, as well as validating its format.
    * We store the validation results in a cache.
    */
  private[crypto] def parseAndValidatePublicKey[E](
      publicKey: PublicKey,
      errFn: String => E,
  ): Either[E, Unit] = {
    val parseRes = publicKey.format match {
      case CryptoKeyFormat.Tink =>
        /* We check the cache first and if it's not there we:
         * 1. deserialize handle (and consequently check the key format); 2. check fingerprint
         */
        parseAndValidateTinkKey(publicKey).map(_ => ())
      case CryptoKeyFormat.Der | CryptoKeyFormat.Raw =>
        /* We check the cache first and if it's not there we:
         * 1. check fingerprint; 2. convert to Java Key (and consequently check the key format)
         */
        parseAndValidateDerOrRawKey(publicKey).map(_ => ())
      case CryptoKeyFormat.Symbolic =>
        Right(())
    }

    // If the result is already in the cache it means the key has already been validated.
    validatedPublicKeys
      .getOrElseUpdate(publicKey, parseRes)
      .leftMap(err => errFn(s"Failed to deserialize ${publicKey.format} public key: $err"))
  }

  private[crypto] def ensureFormat[E](
      actual: CryptoKeyFormat,
      acceptedFormats: Set[CryptoKeyFormat],
      errFn: String => E,
  ): Either[E, Unit] =
    Either.cond(
      acceptedFormats.contains(actual),
      (),
      errFn(s"Expected key formats $acceptedFormats, but got $actual"),
    )

}
