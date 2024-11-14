// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.crypto.kms.driver.api.v1

import com.digitalasset.canton.crypto.kms.driver.api
import io.opentelemetry.context.Context

import scala.concurrent.Future

/** The interface for a pluggable KMS implementation, that is, a KMS Driver.
  *
  * Cryptographic operations are async, i.e., they return a Future.
  * In case of failures, the future must be failed with a [[KmsDriverException]].
  *
  * Each KMS operation takes an OpenTelemetry [[io.opentelemetry.context.Context]] as a trace context that can optionally be propagated to the external KMS.
  */
trait KmsDriver extends api.KmsDriver with AutoCloseable {

  /** Returns the current health of the driver.
    *
    * If the driver reports itself as unhealthy, Canton will close the current driver instance and create a new one to recover from the unhealthy state.
    * Transient failures should be reported by throwing an [[com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverException]] with `retryable` true on driver operations.
    *
    * @return A future that completes with the driver's health.
    */
  def health: Future[KmsDriverHealth]

  /** The supported signing key specifications by the driver */
  def supportedSigningKeySpecs: Set[SigningKeySpec]

  /** The supported signing algorithm specifications by the driver */
  def supportedSigningAlgoSpecs: Set[SigningAlgoSpec]

  /** The supported encryption key specifications by the driver */
  def supportedEncryptionKeySpecs: Set[EncryptionKeySpec]

  /** The supported encryption algorithm specifications by the driver */
  def supportedEncryptionAlgoSpecs: Set[EncryptionAlgoSpec]

  /** Generate a new signing key pair.
    *
    * @param signingKeySpec The key specification for the new signing key pair. The caller ensures it is a [[supportedSigningKeySpecs]].
    * @param keyName An optional descriptive name for the key pair, max 300 characters long.
    *
    * @return A future that completes with the unique KMS key identifier, max 300 characters long.
    */
  def generateSigningKeyPair(
      signingKeySpec: SigningKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String]

  /** Generate a new asymmetric encryption key pair.
    *
    * @param encryptionKeySpec The key specification of the new encryption key pair. The caller ensures it is a [[supportedEncryptionKeySpecs]].
    * @param keyName An optional descriptive name for the key pair, max 300 characters long.
    *
    * @return A future that completes with the unique KMS key identifier, max 300 characters long.
    */
  def generateEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String]

  /** Generate a new symmetric encryption key.
    * The default symmetric key specification of the KMS is used.
    *
    * @param keyName An optional descriptive name for the symmetric key, max 300 characters long.
    *
    * @return A future that completes with the unique KMS key identifier, max 300 characters long.
    */
  def generateSymmetricKey(keyName: Option[String])(traceContext: Context): Future[String]

  /** Sign the given data using the private key identified by the keyId with the given signing algorithm specification.
    * If the `algoSpec` is not compatible with the key spec of `keyId` then this method must fail with a non-retryable exception.
    *
    * @param data The data to be signed with the specified signature algorithm. The upper bound of the data size is 4kb.
    * @param keyId The identifier of the private signing key.
    * @param algoSpec The signature algorithm specification. The caller ensures it is a [[supportedSigningAlgoSpecs]].
    *
    * @return A future that completes with the signature.
    */
  def sign(data: Array[Byte], keyId: String, algoSpec: SigningAlgoSpec)(
      traceContext: Context
  ): Future[Array[Byte]]

  /** Asymmetrically decrypt the given ciphertext using the private key identified by the keyId with the given asymmetric encryption algorithm specification.
    * If the `algoSpec` is not compatible with the key spec of `keyId` then this method must fail with a non-retryable exception.
    *
    * @param ciphertext The asymmetrically encrypted ciphertext that needs to be decrypted. The length of the ciphertext depends on the parameters of the asymmetric encryption algorithm. Implementations may assume that the length of the ciphertext is at most 6144 bytes in any case.
    * @param keyId The identifier of the private encryption key to perform the asymmetric decryption with.
    * @param algoSpec The asymmetric encryption algorithm specification. The caller ensures it is a [[supportedEncryptionAlgoSpecs]].
    *
    * @return A future that completes with the plaintext.
    */
  def decryptAsymmetric(
      ciphertext: Array[Byte],
      keyId: String,
      algoSpec: EncryptionAlgoSpec,
  )(traceContext: Context): Future[Array[Byte]]

  /** Symmetrically encrypt the given plaintext using the symmetric encryption key identified by the keyId.
    * The same/default symmetric encryption algorithm of the KMS must be used for both symmetric encryption and decryption.
    *
    * @param data The plaintext to symmetrically encrypt. The upper bound of the data size is 4kb.
    * @param keyId The identifier of the symmetric encryption key.
    *
    * @return A future that completes with the ciphertext.
    */
  def encryptSymmetric(data: Array[Byte], keyId: String)(traceContext: Context): Future[Array[Byte]]

  /** Symmetrically decrypt the given ciphertext using the symmetric encryption key identified by the keyId.
    * The same/default symmetric encryption algorithm of the KMS must be used for both symmetric encryption and decryption.
    *
    * @param ciphertext The ciphertext to symmetrically decrypt. The upper bound of the ciphertext size is 6144 bytes.
    * @param keyId The identifier of the symmetric encryption key.
    *
    * @return A future that completes with the plaintext.
    */
  def decryptSymmetric(ciphertext: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]]

  /** Exports a public key from the KMS for the given key pair identified by keyId.
    *
    * @param keyId The identifier of the key pair.
    *
    * @return A future that completes with the exported [[PublicKey]]
    */
  def getPublicKey(keyId: String)(traceContext: Context): Future[PublicKey]

  /** Asserts that the key given by its identifier exists and is active.
    *
    * @param keyId The identifier of the key to be checked.
    *
    * @return A future that completes successfully if the key exists and is active. Otherwise the future must have been failed.
    */
  def keyExistsAndIsActive(keyId: String)(traceContext: Context): Future[Unit]

  /** Deletes a key given by its identifier from the KMS.
    *
    * @param keyId The identifier of the key to be deleted.
    *
    * @return A future that completes when the key has been deleted or the deletion of the key has been scheduled.
    */
  def deleteKey(keyId: String)(traceContext: Context): Future[Unit]

}

/** A public key exported from the KMS.
  *
  * @param key The DER-encoded X.509 public key (SubjectPublicKeyInfo)
  * @param spec The key specification of the key pair
  */
final case class PublicKey(key: Array[Byte], spec: KeySpec)
