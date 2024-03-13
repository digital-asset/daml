// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.jce.JceSecurityProvider
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.crypto.tink.aead.AeadKeyTemplates
import com.google.crypto.tink.hybrid.HybridKeyTemplates
import com.google.crypto.tink.proto.KeyData.KeyMaterialType
import com.google.crypto.tink.proto.*
import com.google.crypto.tink.subtle.EllipticCurves
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.crypto.tink.{KeysetHandle, proto as tinkproto}
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers
import org.bouncycastle.jcajce.interfaces.EdDSAPublicKey

import java.security.interfaces.ECPublicKey
import java.security.spec.{InvalidKeySpecException, X509EncodedKeySpec}
import java.security.{KeyFactory, NoSuchAlgorithmException, PublicKey as JPublicKey}

/** Converter methods from Tink to Java security keys and vice versa. */
class TinkJavaConverter extends JavaKeyConverter {
  import com.digitalasset.canton.util.ShowUtil.*

  /** Extract the first key from the keyset. In Canton we only deal with Tink keysets of size 1. */
  private def getFirstKey(
      keysetHandle: KeysetHandle
  ): Either[JavaKeyConversionError, tinkproto.Keyset.Key] =
    for {
      // No other way to access the public key directly than going via protobuf
      keysetProto <- ProtoConverter
        .protoParser(tinkproto.Keyset.parseFrom)(TinkKeyFormat.serializeHandle(keysetHandle))
        .leftMap(err =>
          JavaKeyConversionError.InvalidKey(s"Failed to parser tink keyset proto: $err")
        )
      key <- Either.cond(
        keysetProto.getKeyCount == 1,
        keysetProto.getKey(0),
        JavaKeyConversionError.InvalidKey(
          s"Not exactly one key in the keyset, but ${keysetProto.getKeyCount} keys."
        ),
      )
    } yield key

  /** Map EC-DSA public keys to the correct curves. */
  private def convertCurve(
      curveValue: EllipticCurveType
  ): Either[JavaKeyConversionError, CurveType] =
    curveValue match {
      case com.google.crypto.tink.proto.EllipticCurveType.NIST_P256 =>
        EllipticCurves.CurveType.NIST_P256.asRight
      case com.google.crypto.tink.proto.EllipticCurveType.NIST_P384 =>
        EllipticCurves.CurveType.NIST_P384.asRight
      case unsupportedCurveValue =>
        JavaKeyConversionError
          .InvalidKey(s"Unsupported curve value: $unsupportedCurveValue")
          .asLeft[CurveType]
    }

  /** Sanity check that the given key data is of appropriate type (such as asymmetric public/private key). */
  private def verifyKeyType(
      keyData: KeyData,
      keyTypeValue: Int,
  ): Either[JavaKeyConversionError, Unit] = {
    val keyMaterialTypeValue = keyData.getKeyMaterialTypeValue
    Either.cond(
      keyMaterialTypeValue == keyTypeValue,
      (),
      JavaKeyConversionError.InvalidKey(
        s"Invalid key material type value $keyMaterialTypeValue, must be value: $keyTypeValue"
      ),
    )
  }

  /** Converts a Tink public key to a [[java.security.PublicKey]] with an X509 [[org.bouncycastle.asn1.x509.AlgorithmIdentifier]] for the curve.
    *
    * The conversion has to go via protobuf serialization of the Tink key as Tink does not provide access to the key
    * directly.
    */
  override def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
    for {
      keysetHandle <- TinkKeyFormat
        .deserializeHandle(publicKey.key)
        .leftMap(err => JavaKeyConversionError.InvalidKey(s"Failed to deserialize keyset: $err"))
      keyset0 <- getFirstKey(keysetHandle)
      keyData = keyset0.getKeyData

      // Sanity check that the key is a asymmetric public key and of type EC DSA
      _ <- verifyKeyType(keyData, KeyMaterialType.ASYMMETRIC_PUBLIC_VALUE)

      keyTuple <- keyData.getTypeUrl match {
        case "type.googleapis.com/google.crypto.tink.EcdsaPublicKey" =>
          for {
            protoPublicKey <- ProtoConverter
              .protoParser(tinkproto.EcdsaPublicKey.parseFrom)(keyData.getValue)
              .leftMap(err =>
                JavaKeyConversionError.InvalidKey(
                  s"Failed to parse key proto from keyset as EC DSA: $err"
                )
              )
            curve <- convertCurve(protoPublicKey.getParams.getCurve)
            algoId <- TinkJavaConverter
              .fromCurveType(curve)
              .leftMap(JavaKeyConversionError.InvalidKey)
            // Get the EC public key and verify that the public key is on the supported curve
            pubKey = EllipticCurves.getEcPublicKey(
              curve,
              protoPublicKey.getX.toByteArray,
              protoPublicKey.getY.toByteArray,
            )
          } yield (algoId, pubKey)

        case "type.googleapis.com/google.crypto.tink.Ed25519PublicKey" =>
          for {
            protoPublicKey <- ProtoConverter
              .protoParser(tinkproto.Ed25519PublicKey.parseFrom)(keyData.getValue)
              .leftMap(err =>
                JavaKeyConversionError.InvalidKey(
                  s"Failed to parse key proto from keyset as Ed25519: $err"
                )
              )
            algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
            x509PublicKey = new SubjectPublicKeyInfo(algoId, protoPublicKey.getKeyValue.toByteArray)
            x509KeySpec = new X509EncodedKeySpec(x509PublicKey.getEncoded)
            keyFactory <- Either
              .catchOnly[NoSuchAlgorithmException](
                KeyFactory.getInstance("Ed25519", JceSecurityProvider.bouncyCastleProvider)
              )
              .leftMap(JavaKeyConversionError.GeneralError)
            javaPublicKey <- Either
              .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(x509KeySpec))
              .leftMap(err => JavaKeyConversionError.InvalidKey(show"$err"))
          } yield (algoId, javaPublicKey)

        case "type.googleapis.com/google.crypto.tink.EciesAeadHkdfPublicKey" =>
          for {
            protoPublicKey <- ProtoConverter
              .protoParser(tinkproto.EciesAeadHkdfPublicKey.parseFrom)(keyData.getValue)
              .leftMap(err =>
                JavaKeyConversionError.InvalidKey(
                  s"Failed to parse key proto from keyset as ECIES: $err"
                )
              )
            curve <- convertCurve(protoPublicKey.getParams.getKemParams.getCurveType)
            algoId <- TinkJavaConverter
              .fromCurveType(curve)
              .leftMap(JavaKeyConversionError.InvalidKey)
            // Get the EC public key and verify that the public key is on the supported curve
            pubKey = EllipticCurves.getEcPublicKey(
              curve,
              protoPublicKey.getX.toByteArray,
              protoPublicKey.getY.toByteArray,
            )
          } yield (algoId, pubKey)

        case unsupportedKeyTypeUrl =>
          Left(
            JavaKeyConversionError.InvalidKey(s"Unsupported key type url: $unsupportedKeyTypeUrl")
          )
      }
    } yield keyTuple

  private def fromKeyData(
      keyData: KeyData,
      signingKeyScheme: SigningKeyScheme,
      fingerprint: Fingerprint,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {
    val keyId = 0
    val key = tinkproto.Keyset.Key
      .newBuilder()
      .setKeyData(keyData)
      .setStatus(KeyStatusType.ENABLED)
      .setKeyId(keyId)
      .setOutputPrefixType(OutputPrefixType.RAW)
      .build()

    val keyset = tinkproto.Keyset.newBuilder().setPrimaryKeyId(keyId).addKey(key).build()

    for {
      keysetHandle <- TinkKeyFormat
        .deserializeHandle(keyset.toByteString)
        .leftMap(err => JavaKeyConversionError.InvalidKey(err.toString))
    } yield new SigningPublicKey(
      fingerprint,
      CryptoKeyFormat.Tink,
      TinkKeyFormat.serializeHandle(keysetHandle),
      signingKeyScheme,
    )
  }

  private def fromJavaEd25519Key(
      keyBytes: ByteString,
      fingerprint: Fingerprint,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {
    val edPublicKey = tinkproto.Ed25519PublicKey
      .newBuilder()
      .setVersion(0)
      .setKeyValue(keyBytes)
      .build()

    val keydata = tinkproto.KeyData
      .newBuilder()
      .setTypeUrl("type.googleapis.com/google.crypto.tink.Ed25519PublicKey")
      .setValue(edPublicKey.toByteString)
      .setKeyMaterialType(KeyMaterialType.ASYMMETRIC_PUBLIC)
      .build()

    fromKeyData(keydata, SigningKeyScheme.Ed25519, fingerprint)
  }

  private def fromJavaEcDsaKey(
      ecPubKey: ECPublicKey,
      scheme: SigningKeyScheme,
      fingerprint: Fingerprint,
  ): Either[JavaKeyConversionError, SigningPublicKey] = for {
    hashAndCurve <- scheme match {
      case SigningKeyScheme.EcDsaP256 =>
        (HashType.SHA256, EllipticCurveType.NIST_P256).asRight
      case SigningKeyScheme.EcDsaP384 =>
        (HashType.SHA384, EllipticCurveType.NIST_P384).asRight
      case SigningKeyScheme.Ed25519 =>
        JavaKeyConversionError
          .UnsupportedSigningKeyScheme(scheme, SigningKeyScheme.EcDsaSchemes)
          .asLeft
    }

    (hashType, curve) = hashAndCurve

    // Importing a java public key into Tink has to go via protobuf
    ecdsaParams = tinkproto.EcdsaParams
      .newBuilder()
      .setHashType(hashType)
      .setCurve(curve)
      .setEncoding(tinkproto.EcdsaSignatureEncoding.DER)

    ecdsaPubKey = tinkproto.EcdsaPublicKey
      .newBuilder()
      .setVersion(0)
      .setParams(ecdsaParams)
      .setX(ByteString.copyFrom(ecPubKey.getW.getAffineX.toByteArray))
      .setY(ByteString.copyFrom(ecPubKey.getW.getAffineY.toByteArray))
      .build()

    keydata = tinkproto.KeyData
      .newBuilder()
      .setTypeUrl("type.googleapis.com/google.crypto.tink.EcdsaPublicKey")
      .setValue(ecdsaPubKey.toByteString)
      .setKeyMaterialType(KeyMaterialType.ASYMMETRIC_PUBLIC)
      .build()

    publicKey <- fromKeyData(keydata, scheme, fingerprint)
  } yield publicKey

  override def fromJavaSigningKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
      fingerprint: Fingerprint,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {
    publicKey match {
      case ecPubKey: ECPublicKey =>
        for {
          scheme <- JavaKeyConverter
            .toSigningKeyScheme(algorithmIdentifier)
          publicKey <- scheme match {
            case SigningKeyScheme.EcDsaP256 | SigningKeyScheme.EcDsaP384 =>
              fromJavaEcDsaKey(ecPubKey, scheme, fingerprint)
            case SigningKeyScheme.Ed25519 =>
              JavaKeyConversionError
                .UnsupportedSigningKeyScheme(scheme, SigningKeyScheme.EcDsaSchemes)
                .asLeft
          }
        } yield publicKey

      case edDSAPublicKey: EdDSAPublicKey =>
        for {
          scheme <- JavaKeyConverter
            .toSigningKeyScheme(algorithmIdentifier)
          publicKey <- scheme match {
            case SigningKeyScheme.Ed25519 =>
              fromJavaEd25519Key(ByteString.copyFrom(edDSAPublicKey.getPointEncoding), fingerprint)
            case _ =>
              JavaKeyConversionError
                .UnsupportedSigningKeyScheme(scheme, SigningKeyScheme.EdDsaSchemes)
                .asLeft
          }
        } yield publicKey

      case unsupportedKey =>
        Left(
          JavaKeyConversionError.InvalidKey(
            s"Unsupported Java public key for signing: $unsupportedKey"
          )
        )
    }
  }

  override def fromJavaEncryptionKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
      fingerprint: Fingerprint,
  ): Either[JavaKeyConversionError, EncryptionPublicKey] = {
    publicKey match {
      case ecPubKey: ECPublicKey =>
        for {
          scheme <- JavaKeyConverter
            .toEncryptionKeyScheme(algorithmIdentifier)
          keydata <- scheme match {
            case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
              val eciesParams =
                HybridKeyTemplates.createEciesAeadHkdfParams(
                  EllipticCurveType.NIST_P256,
                  HashType.SHA256,
                  EcPointFormat.UNCOMPRESSED,
                  AeadKeyTemplates.AES128_GCM,
                  Array[Byte](),
                )

              val ecdsaPubKey = tinkproto.EciesAeadHkdfPublicKey
                .newBuilder()
                .setVersion(0)
                .setParams(eciesParams)
                .setX(ByteString.copyFrom(ecPubKey.getW.getAffineX.toByteArray))
                .setY(ByteString.copyFrom(ecPubKey.getW.getAffineY.toByteArray))
                .build()

              val keydata = tinkproto.KeyData
                .newBuilder()
                .setTypeUrl("type.googleapis.com/google.crypto.tink.EciesAeadHkdfPublicKey")
                .setValue(ecdsaPubKey.toByteString)
                .setKeyMaterialType(KeyMaterialType.ASYMMETRIC_PUBLIC)
                .build()

              keydata.asRight

            case unsupported =>
              Left(
                JavaKeyConversionError.InvalidKey(
                  s"Encryption scheme is unsupported by Tink: $unsupported"
                )
              )
          }

          keyId = 0
          key = tinkproto.Keyset.Key
            .newBuilder()
            .setKeyData(keydata)
            .setStatus(KeyStatusType.ENABLED)
            .setKeyId(keyId)
            // Use RAW because we don't have the same key id prefix
            .setOutputPrefixType(OutputPrefixType.RAW)
            .build()

          keyset = tinkproto.Keyset.newBuilder().setPrimaryKeyId(keyId).addKey(key).build()
          keysetHandle <- TinkKeyFormat
            .deserializeHandle(keyset.toByteString)
            .leftMap(err => JavaKeyConversionError.InvalidKey(err.toString))
        } yield new EncryptionPublicKey(
          fingerprint,
          CryptoKeyFormat.Tink,
          TinkKeyFormat.serializeHandle(keysetHandle),
          scheme,
        )

      case unsupportedKey =>
        Left(
          JavaKeyConversionError.InvalidKey(
            s"Unsupported Java public key for encryption: $unsupportedKey"
          )
        )
    }
  }

}

object TinkJavaConverter {

  def fromCurveType(curveType: CurveType): Either[String, AlgorithmIdentifier] =
    for {
      asnObjId <- curveType match {
        // CCF prefers the X9 identifiers (ecdsa-shaX) and not the SEC OIDs (secp384r1)
        case CurveType.NIST_P256 => Right(X9ObjectIdentifiers.ecdsa_with_SHA256)
        case CurveType.NIST_P384 => Right(X9ObjectIdentifiers.ecdsa_with_SHA384)
        case CurveType.NIST_P521 => Left("Elliptic curve NIST-P521 not supported right now")
      }
    } yield new AlgorithmIdentifier(asnObjId)

}
