// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.admin.v30
import com.digitalasset.canton.crypto.{
  Fingerprint,
  KeyName,
  KeyPurpose,
  PublicKey,
  PublicKeyWithName,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class PrivateKeyMetadata(
    publicKeyWithName: PublicKeyWithName,
    wrapperKeyId: Option[String300],
    kmsKeyId: Option[String300],
) {

  require(
    wrapperKeyId.forall(!_.unwrap.isBlank) || kmsKeyId.forall(!_.unwrap.isBlank),
    "the wrapper key or KMS key ID cannot be an empty or blank string",
  )

  def id: Fingerprint = publicKey.id

  def publicKey: PublicKey = publicKeyWithName.publicKey

  def name: Option[KeyName] = publicKeyWithName.name

  def purpose: KeyPurpose = publicKey.purpose

  def encrypted: Boolean = wrapperKeyId.isDefined

  def toProtoV30: v30.PrivateKeyMetadata =
    v30.PrivateKeyMetadata(
      publicKeyWithName = Some(publicKeyWithName.toProtoV30),
      wrapperKeyId = wrapperKeyId.map(_.toProtoPrimitive),
      kmsKeyId = kmsKeyId.map(_.toProtoPrimitive),
    )
}

object PrivateKeyMetadata {

  def fromProtoV30(key: v30.PrivateKeyMetadata): ParsingResult[PrivateKeyMetadata] =
    for {
      publicKeyWithName <- ProtoConverter.parseRequired(
        PublicKeyWithName.fromProto30,
        "public_key_with_name",
        key.publicKeyWithName,
      )
      wrapperKeyId <- key.wrapperKeyId
        .traverse { keyId =>
          if (keyId.isBlank)
            Left(
              ProtoDeserializationError
                .InvariantViolation("wrapper_key_id", "empty or blank wrapper key ID")
            )
          else String300.fromProtoPrimitive(keyId, "wrapper_key_id")
        }
      kmsKeyId <- key.kmsKeyId
        .traverse { keyId =>
          if (keyId.isBlank)
            Left(
              ProtoDeserializationError
                .InvariantViolation("kms_key_id", "empty or blank KMS key ID")
            )
          else String300.fromProtoPrimitive(keyId, "kms_key_id")
        }
    } yield PrivateKeyMetadata(
      publicKeyWithName,
      wrapperKeyId,
      kmsKeyId,
    )
}
