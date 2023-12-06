// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.jce.JceJavaConverter
import com.digitalasset.canton.crypto.provider.tink.TinkJavaConverter
import com.digitalasset.canton.util.EitherUtil

import scala.collection.concurrent.TrieMap

class CryptoKeyConverter(
    tinkJavaConverter: TinkJavaConverter,
    jceJavaConverter: JceJavaConverter,
) {

  private case class PublicKeyConversion(
      fingerprint: Fingerprint,
      fromFormat: CryptoKeyFormat,
      toFormat: CryptoKeyFormat,
  )

  private val convertedKeys =
    TrieMap.empty[PublicKeyConversion, Either[String, PublicKey]]

  def convert(
      publicKey: PublicKey,
      to: CryptoKeyFormat,
  ): Either[String, PublicKey] = {

    def cachedConvert(
        fromConverter: JavaKeyConverter,
        toConverter: JavaKeyConverter,
    ): Either[String, PublicKey] = {

      // Convert from one public key format to another via Java PublicKey as the intermediate representation,
      // because for each JavaKeyConverter we have to and from Java methods for public keys.
      def convertViaJava(
          fromConverter: JavaKeyConverter,
          toConverter: JavaKeyConverter,
      ): Either[String, PublicKey] =
        for {
          convertedPubKey <- fromConverter
            .toJava(publicKey)
            .flatMap { case (algoId, pubKey) =>
              publicKey.purpose match {
                case KeyPurpose.Signing =>
                  toConverter.fromJavaSigningKey(pubKey, algoId, publicKey.fingerprint)
                case KeyPurpose.Encryption =>
                  toConverter.fromJavaEncryptionKey(pubKey, algoId, publicKey.fingerprint)
              }
            }
            .leftMap(err => s"Failed to convert public key: $err")
          _ <- EitherUtil.condUnitE(
            convertedPubKey.format == to,
            s"Conversion of public key into $to not supported",
          )
        } yield convertedPubKey

      convertedKeys.getOrElseUpdate(
        PublicKeyConversion(publicKey.fingerprint, publicKey.format, to),
        convertViaJava(fromConverter, toConverter),
      )
    }

    (publicKey.format, to) match {
      // Identity case
      case (CryptoKeyFormat.Raw, CryptoKeyFormat.Raw) |
          (CryptoKeyFormat.Tink, CryptoKeyFormat.Tink) |
          (CryptoKeyFormat.Der, CryptoKeyFormat.Der) =>
        Right(publicKey)

      // From Tink to Raw/Der (depending on the algorithm)
      case (CryptoKeyFormat.Tink, CryptoKeyFormat.Raw) |
          (CryptoKeyFormat.Tink, CryptoKeyFormat.Der) =>
        cachedConvert(tinkJavaConverter, jceJavaConverter)

      // From Raw/Der to Tink
      case (CryptoKeyFormat.Raw, CryptoKeyFormat.Tink) |
          (CryptoKeyFormat.Der, CryptoKeyFormat.Tink) =>
        cachedConvert(jceJavaConverter, tinkJavaConverter)

      case (CryptoKeyFormat.Raw, CryptoKeyFormat.Der) |
          (CryptoKeyFormat.Der, CryptoKeyFormat.Raw) =>
        Left("Conversion from Raw to DER or vice-versa is not supported")

      case (CryptoKeyFormat.Symbolic, _) | (_, CryptoKeyFormat.Symbolic) =>
        Left("Symbolic key format not supported")

    }
  }

}
