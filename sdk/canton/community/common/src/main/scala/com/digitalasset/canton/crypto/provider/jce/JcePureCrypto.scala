// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CommunityCryptoProvider.Jce
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.deterministic.encryption.DeterministicRandom
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, ShowUtil}
import com.digitalasset.canton.version.{HasToByteString, HasVersionedToByteString, ProtocolVersion}
import com.google.crypto.tink.hybrid.subtle.AeadOrDaead
import com.google.crypto.tink.subtle.*
import com.google.crypto.tink.subtle.EllipticCurves.EcdsaEncoding
import com.google.crypto.tink.subtle.Enums.HashType
import com.google.crypto.tink.{Aead, PublicKeySign, PublicKeyVerify}
import com.google.protobuf.ByteString
import org.bouncycastle.crypto.DataLengthException
import org.bouncycastle.crypto.generators.Argon2BytesGenerator
import org.bouncycastle.crypto.params.Argon2Parameters
import org.bouncycastle.jcajce.provider.asymmetric.edec.BCEdDSAPublicKey
import org.bouncycastle.jce.spec.IESParameterSpec

import java.security.interfaces.*
import java.security.{
  GeneralSecurityException,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
  SecureRandom,
}
import javax.crypto.Cipher
import scala.collection.concurrent.TrieMap
import scala.util.Try

object JceSecureRandom {

  /** Uses [[ThreadLocal]] here to reduce contention and improve performance. */
  private[crypto] val random: ThreadLocal[SecureRandom] = new ThreadLocal[SecureRandom] {
    override def initialValue(): SecureRandom = newSecureRandom()
  }

  private def newSecureRandom() = {
    val rand = new SecureRandom()
    rand.nextLong()
    rand
  }

  private[jce] def generateRandomBytes(length: Int): Array[Byte] = {
    val randBytes = new Array[Byte](length)
    random.get().nextBytes(randBytes)
    randBytes
  }
}

class JcePureCrypto(
    override val defaultSymmetricKeyScheme: SymmetricKeyScheme,
    override val defaultEncryptionAlgorithmSpec: EncryptionAlgorithmSpec,
    override val supportedEncryptionAlgorithmSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]],
    override val defaultHashAlgorithm: HashAlgorithm,
    override val defaultPbkdfScheme: PbkdfScheme,
    override val loggerFactory: NamedLoggerFactory,
) extends CryptoPureApi
    with ShowUtil
    with NamedLogging {

  // TODO(#15632): Make these real caches with an eviction rule
  // Cache for the java key conversion results
  private val javaPublicKeyCache
      : TrieMap[Fingerprint, Either[KeyParseAndValidateError, JPublicKey]] =
    TrieMap.empty
  private val javaPrivateKeyCache
      : TrieMap[Fingerprint, Either[KeyParseAndValidateError, JPrivateKey]] =
    TrieMap.empty

  /* security parameters for EciesP256HmacSha256Aes128Cbc encryption scheme,
    in particular for the HMAC and symmetric crypto algorithms.
   */
  private object EciesHmacSha256Aes128CbcParams {
    // the internal jce designation for this scheme
    val jceInternalName: String = "ECIESwithSHA256andAES-CBC"
    // the key size in bits for HMACSHA256 is 64bytes (recommended)
    private val macKeySizeInBits: Int = 512
    // the key size in bits for AES-128-CBC is 16 bytes
    private val cipherKeySizeInBits: Int = 128
    // the IV for AES-128-CBC is 16 bytes
    val ivSizeForAesCbcInBytes: Int = 16
    // the parameter specification for this scheme.
    def parameterSpec(iv: Array[Byte]): IESParameterSpec = new IESParameterSpec(
      // we do no use any encoding or derivation vector for the KDF.
      Array[Byte](),
      Array[Byte](),
      macKeySizeInBits,
      cipherKeySizeInBits,
      iv,
    )
  }

  private object RsaOaepSha256Params {
    // the internal jce designation for this scheme
    val jceInternalName: String = "RSA/NONE/OAEPWithSHA256AndMGF1Padding"
  }

  /** Parses and converts a public key to a java public key.
    * We store the deserialization result in a cache.
    *
    * @return Either an error or the converted java private key
    */
  private def parseAndGetPublicKey[E](
      publicKey: PublicKey,
      errFn: String => E,
  ): Either[E, JPublicKey] = {
    val keyFormat = publicKey.format

    def convertToFormatAndGenerateJavaPublicKey: Either[KeyParseAndValidateError, JPublicKey] =
      for {
        // convert key to java key
        jPublicKey <- JceJavaKeyConverter
          .toJava(publicKey)
          .leftMap(err =>
            KeyParseAndValidateError(s"Failed to convert public key to java key: $err")
          )
      } yield jPublicKey

    def getFromCacheOrDeserializeKey: Either[E, JPublicKey] =
      javaPublicKeyCache
        .getOrElseUpdate(
          publicKey.id,
          convertToFormatAndGenerateJavaPublicKey,
        )
        .leftMap(err => errFn(s"Failed to deserialize ${publicKey.format} public key: $err"))

    if (Jce.supportedCryptoKeyFormats.contains(keyFormat))
      getFromCacheOrDeserializeKey
    else Left(errFn(s"$keyFormat key format not supported"))
  }

  /** Parses and converts an asymmetric private key to a java private key.
    * We store the deserialization result in a cache.
    *
    * @return Either an error or the converted java private key
    */
  private def parseAndGetPrivateKey[E, T <: JPrivateKey](
      privateKey: PrivateKey,
      checker: PartialFunction[JPrivateKey, Either[E, T]],
      errFn: String => E,
  ): Either[E, T] =
    for {
      privateKey <- javaPrivateKeyCache
        .getOrElseUpdate(
          privateKey.id,
          JceJavaKeyConverter
            .toJava(privateKey)
            .leftMap(err =>
              KeyParseAndValidateError(s"Failed to convert private key to java key: ${err.show}")
            ),
        )
        .leftMap(err => errFn(s"Failed to deserialize ${privateKey.format} private key: $err"))
      checkedPrivateKey <- checker(privateKey)
    } yield checkedPrivateKey

  private def encryptAes128Gcm(
      plaintext: ByteString,
      symmetricKey: ByteString,
  ): Either[EncryptionError, ByteString] =
    for {
      encrypter <- Either
        .catchOnly[GeneralSecurityException](new AesGcmJce(symmetricKey.toByteArray))
        .leftMap(err => EncryptionError.InvalidSymmetricKey(err.toString))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.encrypt(plaintext.toByteArray, Array[Byte]())
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))
    } yield ByteString.copyFrom(ciphertext)

  private def decryptAes128Gcm(
      ciphertext: ByteString,
      symmetricKey: ByteString,
  ): Either[DecryptionError, ByteString] =
    for {
      decrypter <- Either
        .catchOnly[GeneralSecurityException](new AesGcmJce(symmetricKey.toByteArray))
        .leftMap(err => DecryptionError.InvalidSymmetricKey(err.toString))
      plaintext <- Either
        .catchOnly[GeneralSecurityException](
          decrypter.decrypt(ciphertext.toByteArray, Array[Byte]())
        )
        .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
    } yield ByteString.copyFrom(plaintext)

  // Internal helper class for the symmetric encryption as part of the hybrid encryption scheme.
  private object Aes128GcmDemHelper extends EciesAeadHkdfDemHelper {

    override def getSymmetricKeySizeInBytes: Int = SymmetricKeyScheme.Aes128Gcm.keySizeInBytes

    override def getAeadOrDaead(symmetricKeyValue: Array[Byte]): AeadOrDaead = new AeadOrDaead(
      new Aead {
        override def encrypt(plaintext: Array[Byte], associatedData: Array[Byte]): Array[Byte] = {
          val encrypter = new AesGcmJce(symmetricKeyValue)
          encrypter.encrypt(plaintext, associatedData)
        }

        override def decrypt(ciphertext: Array[Byte], associatedData: Array[Byte]): Array[Byte] = {
          val decrypter = new AesGcmJce(symmetricKeyValue)
          decrypter.decrypt(ciphertext, associatedData)
        }
      }
    )
  }

  private def ecDsaSigner(
      signingKey: SigningPrivateKey,
      hashType: HashType,
  ): Either[SigningError, PublicKeySign] =
    for {
      ecPrivateKey <- parseAndGetPrivateKey(
        signingKey,
        { case k: ECPrivateKey => Right(k) },
        SigningError.InvalidSigningKey.apply,
      )
      signer <- Either
        .catchOnly[GeneralSecurityException](
          new EcdsaSignJce(ecPrivateKey, hashType, EcdsaEncoding.DER)
        )
        .leftMap(err => SigningError.InvalidSigningKey(show"Failed to get signer for EC-DSA: $err"))
    } yield signer

  private def ecDsaVerifier(
      publicKey: SigningPublicKey,
      hashType: HashType,
  ): Either[SignatureCheckError, PublicKeyVerify] =
    for {
      javaPublicKey <- parseAndGetPublicKey(publicKey, SignatureCheckError.InvalidKeyError.apply)
      ecPublicKey <- javaPublicKey match {
        case k: ECPublicKey => Right(k)
        case _ =>
          Left(SignatureCheckError.InvalidKeyError(s"Signing public key is not an EC public key"))
      }

      verifier <- Either
        .catchOnly[GeneralSecurityException](
          new EcdsaVerifyJce(ecPublicKey, hashType, EcdsaEncoding.DER)
        )
        .leftMap(err =>
          SignatureCheckError.InvalidKeyError(s"Failed to get signer for Ed25519: $err")
        )
    } yield verifier

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] =
    scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        val key128 = generateRandomByteString(scheme.keySizeInBytes)
        Right(SymmetricKey(CryptoKeyFormat.Raw, key128, scheme))
    }

  override def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] = {
    val randomnessLength = bytes.unwrap.size()
    val keyLength = scheme.keySizeInBytes

    for {
      _ <- Either.cond(
        randomnessLength == keyLength,
        (),
        EncryptionKeyCreationError.InvalidRandomnessLength(randomnessLength, keyLength),
      )
      key = scheme match {
        case SymmetricKeyScheme.Aes128Gcm =>
          SymmetricKey(CryptoKeyFormat.Raw, bytes.unwrap, scheme)
      }
    } yield key
  }

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature] = {

    def signWithSigner(signer: PublicKeySign): Either[SigningError, Signature] =
      Either
        .catchOnly[GeneralSecurityException](signer.sign(bytes.toByteArray))
        .bimap(
          err => SigningError.FailedToSign(show"$err"),
          signatureBytes =>
            new Signature(SignatureFormat.Raw, ByteString.copyFrom(signatureBytes), signingKey.id),
        )

    signingKey.scheme match {
      case SigningKeyScheme.Ed25519 =>
        for {
          _ <- CryptoKeyValidation.ensureFormat(
            signingKey.format,
            Set(CryptoKeyFormat.Raw),
            SigningError.InvalidSigningKey.apply,
          )
          signer <- Either
            .catchOnly[GeneralSecurityException](new Ed25519Sign(signingKey.key.toByteArray))
            .leftMap(err =>
              SigningError.InvalidSigningKey(show"Failed to get signer for Ed25519: $err")
            )
          signature <- signWithSigner(signer)
        } yield signature

      case SigningKeyScheme.EcDsaP256 =>
        ecDsaSigner(signingKey, HashType.SHA256).flatMap(signWithSigner)
      case SigningKeyScheme.EcDsaP384 =>
        ecDsaSigner(signingKey, HashType.SHA384).flatMap(signWithSigner)
    }
  }

  override protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] = {

    def verify(verifier: PublicKeyVerify): Either[SignatureCheckError, Unit] =
      Either
        .catchOnly[GeneralSecurityException](
          verifier.verify(signature.unwrap.toByteArray, bytes.toByteArray)
        )
        .leftMap(err =>
          SignatureCheckError
            .InvalidSignature(signature, bytes, s"Failed to verify signature: $err")
        )

    for {
      _ <- Either.cond(
        signature.signedBy == publicKey.id,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature signed by ${signature.signedBy} instead of ${publicKey.id}"
        ),
      )

      _ <- publicKey.scheme match {
        case SigningKeyScheme.Ed25519 =>
          for {
            javaPublicKey <- parseAndGetPublicKey(
              publicKey,
              SignatureCheckError.InvalidKeyError.apply,
            )
            ed25519PublicKey <- javaPublicKey match {
              case k: BCEdDSAPublicKey =>
                Right(k.getPointEncoding)
              case _ => Left(SignatureCheckError.InvalidKeyError("Not an Ed25519 public key"))
            }
            verifier <- Either
              .catchOnly[GeneralSecurityException](new Ed25519Verify(ed25519PublicKey))
              .leftMap(err =>
                SignatureCheckError.InvalidKeyError(show"Failed to get signer for Ed25519: $err")
              )
            _ <- verify(verifier)
          } yield ()

        case SigningKeyScheme.EcDsaP256 => ecDsaVerifier(publicKey, HashType.SHA256).flatMap(verify)
        case SigningKeyScheme.EcDsaP384 => ecDsaVerifier(publicKey, HashType.SHA384).flatMap(verify)
      }
    } yield ()
  }

  private def checkEcKeyInCurve[K <: ECKey](key: K, keyId: Fingerprint): Either[String, K] = {
    val curve = EllipticCurves.getNistP256Params.getCurve
    Either.cond(
      key.getParams.getCurve.equals(curve),
      key,
      s"EC key $keyId is not a key in curve $curve",
    )
  }

  private def checkRsaKeySize[K <: RSAKey](
      key: K,
      keyId: Fingerprint,
      size: Int,
  ): Either[String, K] = {
    val keySizeInBits = key.getModulus.bitLength()
    Either.cond(
      keySizeInBits == size,
      key,
      s"RSA key $keyId does not have the correct size. " +
        s"Expected: $size but got: $keySizeInBits.",
    )
  }

  private def encryptWithEciesP256HmacSha256Aes128Gcm[M <: HasVersionedToByteString](
      message: ByteString,
      publicKey: EncryptionPublicKey,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      javaPublicKey <- parseAndGetPublicKey(
        publicKey,
        EncryptionError.InvalidEncryptionKey.apply,
      )
      ecPublicKey <- javaPublicKey match {
        case k: ECPublicKey =>
          checkEcKeyInCurve(k, publicKey.id)
            .leftMap(err => EncryptionError.InvalidEncryptionKey(err))
        case _ => Left(EncryptionError.InvalidEncryptionKey("Not an EC public key"))
      }
      encrypter <- Either
        .catchOnly[GeneralSecurityException](
          new EciesAeadHkdfHybridEncrypt(
            ecPublicKey,
            Array[Byte](),
            "HmacSha256",
            EllipticCurves.PointFormatType.UNCOMPRESSED,
            Aes128GcmDemHelper,
          )
        )
        .leftMap(err => EncryptionError.InvalidEncryptionKey(err.toString))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter
            .encrypt(
              message.toByteArray,
              Array[Byte](),
            )
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))
      encrypted = new AsymmetricEncrypted[M](
        ByteString.copyFrom(ciphertext),
        EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
        publicKey.fingerprint,
      )
    } yield encrypted

  private def encryptWithEciesP256HmacSha256Aes128Cbc[M <: HasVersionedToByteString](
      message: ByteString,
      publicKey: EncryptionPublicKey,
      random: SecureRandom,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      javaPublicKey <- parseAndGetPublicKey(publicKey, EncryptionError.InvalidEncryptionKey.apply)
      ecPublicKey <- javaPublicKey match {
        case k: ECPublicKey =>
          checkEcKeyInCurve(k, publicKey.id).leftMap(err =>
            EncryptionError.InvalidEncryptionKey(err)
          )
        case _ => Left(EncryptionError.InvalidEncryptionKey("Not an EC public key"))
      }
      /* this encryption scheme makes use of AES-128-CBC as a DEM (Data Encapsulation Method)
       * and therefore we need to generate a IV/nonce of 16bytes as the IV for CBC mode.
       */
      iv = new Array[Byte](EciesHmacSha256Aes128CbcParams.ivSizeForAesCbcInBytes)
      _ = random.nextBytes(iv)
      encrypter <- Either
        .catchOnly[GeneralSecurityException] {
          val cipher = Cipher
            .getInstance(
              EciesHmacSha256Aes128CbcParams.jceInternalName,
              JceSecurityProvider.bouncyCastleProvider,
            )
          cipher.init(
            Cipher.ENCRYPT_MODE,
            ecPublicKey,
            EciesHmacSha256Aes128CbcParams.parameterSpec(iv),
            random,
          )
          cipher
        }
        .leftMap(err => EncryptionError.InvalidEncryptionKey(err.toString))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.doFinal(message.toByteArray)
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))
    } yield new AsymmetricEncrypted[M](
      /* Prepend our IV to the ciphertext. On contrary to the Tink library, BouncyCastle's
       * ECIES encryption does not deal with the AES IV by itself and we have to randomly generate it and
       * manually prepend it to the ciphertext.
       */
      ByteString.copyFrom(iv ++ ciphertext),
      EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
      publicKey.fingerprint,
    )

  private def encryptWithRSAOaepSha256[M <: HasVersionedToByteString](
      message: ByteString,
      publicKey: EncryptionPublicKey,
      random: SecureRandom,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      javaPublicKey <- parseAndGetPublicKey(publicKey, EncryptionError.InvalidEncryptionKey.apply)
      rsaPublicKey <- javaPublicKey match {
        case k: RSAPublicKey =>
          for {
            size <- publicKey.keySpec match {
              case EncryptionKeySpec.Rsa2048 => Right(EncryptionKeySpec.Rsa2048.keySizeInBits)
              case wrongKeySpec =>
                Left(
                  EncryptionError.InvalidEncryptionKey(
                    s"Expected a ${EncryptionKeySpec.Rsa2048} public key, but got a $wrongKeySpec public key instead"
                  )
                )
            }
            key <- checkRsaKeySize(k, publicKey.id, size).leftMap(err =>
              EncryptionError.InvalidEncryptionKey(err)
            )
          } yield key
        case _ => Left(EncryptionError.InvalidEncryptionKey("Not a RSA public key"))
      }
      encrypter <- Either
        .catchOnly[GeneralSecurityException] {
          val cipher = Cipher
            .getInstance(RsaOaepSha256Params.jceInternalName, "BC")
          cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey, random)
          cipher
        }
        .leftMap(err => EncryptionError.InvalidEncryptionKey(err.toString))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.doFinal(message.toByteArray)
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))
    } yield new AsymmetricEncrypted[M](
      ByteString.copyFrom(ciphertext),
      EncryptionAlgorithmSpec.RsaOaepSha256,
      publicKey.fingerprint,
    )

  private def encryptWithInternal[M](
      bytes: ByteString,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    CryptoKeyValidation
      .selectEncryptionAlgorithmSpec(
        publicKey.keySpec,
        encryptionAlgorithmSpec,
        supportedEncryptionAlgorithmSpecs,
        algorithmSpec =>
          EncryptionError.UnsupportedAlgorithmSpec(algorithmSpec, supportedEncryptionAlgorithmSpecs),
      )
      .flatMap {
        case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
          encryptWithEciesP256HmacSha256Aes128Gcm(
            bytes,
            publicKey,
          )
        case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
          encryptWithEciesP256HmacSha256Aes128Cbc(
            bytes,
            publicKey,
            JceSecureRandom.random.get(),
          )
        case EncryptionAlgorithmSpec.RsaOaepSha256 =>
          encryptWithRSAOaepSha256(
            bytes,
            publicKey,
            JceSecureRandom.random.get(),
          )
      }

  override def encryptWithVersion[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWithInternal(message.toByteString(version), publicKey, encryptionAlgorithmSpec)

  override def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWithInternal(message.toByteString, publicKey, encryptionAlgorithmSpec)

  override def encryptDeterministicWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    CryptoKeyValidation.selectEncryptionAlgorithmSpec(
      publicKey.keySpec,
      encryptionAlgorithmSpec,
      supportedEncryptionAlgorithmSpecs,
      algorithmSpec =>
        EncryptionError.UnsupportedAlgorithmSpec(algorithmSpec, supportedEncryptionAlgorithmSpecs),
    ) match {
      case Right(spec) if !spec.supportDeterministicEncryption =>
        Left(
          EncryptionError.UnsupportedSchemeForDeterministicEncryption(
            s"$spec does not support deterministic asymmetric/hybrid encryption"
          )
        )
      case Right(scheme) =>
        lazy val messageSerialized = message.toByteString(version)
        lazy val deterministicRandomGenerator = DeterministicRandom.getDeterministicRandomGenerator(
          messageSerialized,
          publicKey.fingerprint,
          loggerFactory,
        )

        scheme match {
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
            Left(
              EncryptionError.UnsupportedSchemeForDeterministicEncryption(
                s"${EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm.name} does not support deterministic asymmetric/hybrid encryption"
              )
            )
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
            encryptWithEciesP256HmacSha256Aes128Cbc(
              messageSerialized,
              publicKey,
              deterministicRandomGenerator,
            )
          case EncryptionAlgorithmSpec.RsaOaepSha256 =>
            encryptWithRSAOaepSha256(
              message.toByteString(version),
              publicKey,
              deterministicRandomGenerator,
            )
        }
      case Left(err) => Left(err)
    }

  override protected[crypto] def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] = {
    CryptoKeyValidation
      .ensureEncryptionSpec(
        privateKey.keySpec,
        encrypted.encryptionAlgorithmSpec,
        supportedEncryptionAlgorithmSpecs,
        algorithmSpec =>
          DecryptionError
            .UnsupportedAlgorithmSpec(algorithmSpec, supportedEncryptionAlgorithmSpecs),
        keySpec =>
          DecryptionError.UnsupportedKeySpec(
            keySpec,
            encrypted.encryptionAlgorithmSpec.supportedEncryptionKeySpecs,
          ),
      )
      .flatMap { _ =>
        encrypted.encryptionAlgorithmSpec match {
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
            for {
              ecPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                { case k: ECPrivateKey =>
                  checkEcKeyInCurve(k, privateKey.id)
                    .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              decrypter <- Either
                .catchOnly[GeneralSecurityException](
                  new EciesAeadHkdfHybridDecrypt(
                    ecPrivateKey,
                    Array[Byte](),
                    "HmacSha256",
                    EllipticCurves.PointFormatType.UNCOMPRESSED,
                    Aes128GcmDemHelper,
                  )
                )
                .leftMap(err => DecryptionError.InvalidEncryptionKey(err.toString))
              plaintext <- Either
                .catchOnly[GeneralSecurityException](
                  decrypter.decrypt(encrypted.ciphertext.toByteArray, Array[Byte]())
                )
                .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
            for {
              ecPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                { case k: ECPrivateKey =>
                  checkEcKeyInCurve(k, privateKey.id)
                    .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              /* we split at 'ivSizeForAesCbc' (=16) because that is the size of our iv (for AES-128-CBC)
               * that gets  pre-appended to the ciphertext.
               */
              ciphertextSplit <- DeterministicEncoding
                .splitAt(
                  EciesHmacSha256Aes128CbcParams.ivSizeForAesCbcInBytes,
                  encrypted.ciphertext,
                )
                .leftMap(err =>
                  DecryptionError.FailedToDeserialize(DefaultDeserializationError(err.show))
                )
              (iv, ciphertext) = ciphertextSplit
              decrypter <- Either
                .catchOnly[GeneralSecurityException] {
                  val cipher = Cipher
                    .getInstance(
                      EciesHmacSha256Aes128CbcParams.jceInternalName,
                      JceSecurityProvider.bouncyCastleProvider,
                    )
                  cipher.init(
                    Cipher.DECRYPT_MODE,
                    ecPrivateKey,
                    EciesHmacSha256Aes128CbcParams.parameterSpec(iv.toByteArray),
                  )
                  cipher
                }
                .leftMap(err => DecryptionError.InvalidEncryptionKey(err.toString))
              plaintext <- Either
                .catchOnly[GeneralSecurityException](
                  decrypter.doFinal(ciphertext.toByteArray)
                )
                .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
          case EncryptionAlgorithmSpec.RsaOaepSha256 =>
            for {
              rsaPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                { case k: RSAPrivateKey =>
                  for {
                    size <- privateKey.keySpec match {
                      case EncryptionKeySpec.Rsa2048 =>
                        Right(EncryptionKeySpec.Rsa2048.keySizeInBits)
                      case wrongKeySpec =>
                        Left(
                          DecryptionError.InvalidEncryptionKey(
                            s"Expected a ${EncryptionKeySpec.Rsa2048} private key, but got a $wrongKeySpec private key instead"
                          )
                        )
                    }
                    key <- checkRsaKeySize(k, privateKey.id, size)
                      .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                  } yield key
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              decrypter <- Either
                .catchOnly[GeneralSecurityException] {
                  val cipher = Cipher
                    .getInstance(
                      RsaOaepSha256Params.jceInternalName,
                      "BC",
                    )
                  cipher.init(
                    Cipher.DECRYPT_MODE,
                    rsaPrivateKey,
                  )
                  cipher
                }
                .leftMap(err => DecryptionError.InvalidEncryptionKey(err.toString))
              plaintext <- Try[Array[Byte]](
                decrypter.doFinal(encrypted.ciphertext.toByteArray)
              ).toEither.leftMap {
                case err: DataLengthException =>
                  DecryptionError
                    .FailedToDecrypt(
                      s"Most probably using a wrong secret key to decrypt the ciphertext: ${err.toString}"
                    )
                case err =>
                  DecryptionError.FailedToDecrypt(ErrorUtil.messageWithStacktrace(err))
              }
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
        }
      }
  }

  private def encryptWith[M](
      bytes: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          _ <- CryptoKeyValidation.ensureFormat(
            symmetricKey.format,
            Set(CryptoKeyFormat.Raw),
            EncryptionError.InvalidSymmetricKey.apply,
          )
          encryptedBytes <- encryptAes128Gcm(bytes, symmetricKey.key)
          encrypted = new Encrypted[M](encryptedBytes)
        } yield encrypted
    }

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] =
    encryptWith(message.toByteString(version), symmetricKey)

  override def encryptWith[M <: HasToByteString](
      message: M,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]] =
    encryptWith(message.toByteString, symmetricKey)

  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          _ <- CryptoKeyValidation.ensureFormat(
            symmetricKey.format,
            Set(CryptoKeyFormat.Raw),
            DecryptionError.InvalidSymmetricKey.apply,
          )
          plaintext <- decryptAes128Gcm(encrypted.ciphertext, symmetricKey.key)
          message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize.apply)
        } yield message
    }

  override protected[crypto] def generateRandomBytes(length: Int): Array[Byte] =
    JceSecureRandom.generateRandomBytes(length)

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] =
    pbkdfScheme match {
      case mode: PbkdfScheme.Argon2idMode1.type =>
        val salt = saltO.getOrElse(generateSecureRandomness(pbkdfScheme.defaultSaltLengthInBytes))

        val params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
          .withIterations(mode.iterations)
          .withMemoryAsKB(mode.memoryInKb)
          .withParallelism(mode.parallelism)
          .withSalt(salt.unwrap.toByteArray)
          .build()

        val argon2 = new Argon2BytesGenerator()
        argon2.init(params)

        val keyLength = symmetricKeyScheme.keySizeInBytes
        val keyBytes = new Array[Byte](keyLength)
        val keyLen = argon2.generateBytes(password.toCharArray, keyBytes)

        val key =
          SymmetricKey(CryptoKeyFormat.Raw, ByteString.copyFrom(keyBytes), symmetricKeyScheme)

        Either.cond(
          keyLen == keyLength,
          PasswordBasedEncryptionKey(key = key, salt = salt),
          PasswordBasedEncryptionError.PbkdfOutputLengthInvalid(
            expectedLength = keyLength,
            actualLength = keyLen,
          ),
        )
    }
}
