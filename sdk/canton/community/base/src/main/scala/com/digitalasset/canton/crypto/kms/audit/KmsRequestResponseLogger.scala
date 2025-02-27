// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.audit

trait KmsRequestResponseLogger {

  protected val publicKeyMessagePlaceholder = "** Public key text placeholder **"
  protected val signMessagePlaceholder = "** Sign message text placeholder **"
  protected val signatureMessagePlaceholder = "** Signature message text placeholder **"
  protected val cipherTextPlaceholder = "** Ciphertext placeholder **"
  protected val plaintextPlaceholder = "** Redacted plaintext placeholder **"

  protected def metadataMessagePlaceHolder(keySchemeStr: String, keyStateStr: String) =
    s"Metadata[KeyScheme: $keySchemeStr, State: $keyStateStr, ** Rest of metadata placeholder **]"

  def createKeyRequestMsg(keyPurposeStr: String, keySchemeStr: String): String =
    s"CreateKeyRequest(KeyPurpose=$keyPurposeStr, KeyScheme=$keySchemeStr)"

  def createKeyResponseMsg(
      keyIdStr: String,
      keyPurposeStr: String,
      keySchemeStr: String,
  ): String =
    s"CreateKeyResponse(KeyId=$keyIdStr, KeyPurpose=$keyPurposeStr, KeyScheme=$keySchemeStr)"

  def getPublicKeyRequestMsg(keyIdStr: String): String =
    s"GetPublicKeyRequest(KeyId=$keyIdStr)"

  def getPublicKeyResponseMsg(keyIdStr: String, keySchemeStr: String): String =
    s"GetPublicKeyResponse(KeyId=$keyIdStr, PublicKey=$publicKeyMessagePlaceholder, KeyScheme=$keySchemeStr)"

  def retrieveKeyMetadataRequestMsg(keyIdStr: String): String =
    s"RetrieveMetadataRequest(KeyId=$keyIdStr)"

  def retrieveKeyMetadataResponseMsg(
      keyIdStr: String,
      keySchemeStr: String,
      stateStr: String,
  ): String =
    s"RetrieveMetadataResponse(KeyId=$keyIdStr, Metadata=${metadataMessagePlaceHolder(keySchemeStr, stateStr)})"

  def encryptRequestMsg(keyIdStr: String, encryptionAlgorithmStr: String): String =
    s"EncryptRequest(Plaintext=$plaintextPlaceholder, KeyId=$keyIdStr, EncryptionAlgorithm=$encryptionAlgorithmStr)"

  def encryptResponseMsg(keyIdStr: String, encryptionAlgorithmStr: String): String =
    s"EncryptResponse(CiphertextBlob=$cipherTextPlaceholder, KeyId=$keyIdStr, EncryptionAlgorithm=$encryptionAlgorithmStr)"

  def decryptRequestMsg(keyIdStr: String, encryptionAlgorithmStr: String): String =
    s"DecryptRequest(CiphertextBlob=$cipherTextPlaceholder, KeyId=$keyIdStr, EncryptionAlgorithm=$encryptionAlgorithmStr)"

  def decryptResponseMsg(keyIdStr: String, encryptionAlgorithmStr: String): String =
    s"DecryptResponse(Plaintext=$plaintextPlaceholder, KeyId=$keyIdStr, EncryptionAlgorithm=$encryptionAlgorithmStr)"

  def signRequestMsg(
      keyIdStr: String,
      messageTypeStr: String,
      signingAlgorithmStr: String,
  ): String =
    s"SignRequest(KeyId=$keyIdStr, Message=$signMessagePlaceholder, MessageType=$messageTypeStr, SigningAlgorithm=$signingAlgorithmStr)"

  def signResponseMsg(keyIdStr: String, signingAlgorithmStr: String): String =
    s"SignResponse(KeyId=$keyIdStr, Signature=$signatureMessagePlaceholder, SigningAlgorithm=$signingAlgorithmStr)"

  def deleteKeyRequestMsg(keyIdStr: String): String =
    s"DeleteKeyRequest(KeyId=$keyIdStr)"

  def deleteKeyResponseMsg(keyIdStr: String): String =
    s"DeleteKeyResponse(KeyId=$keyIdStr)"
}
