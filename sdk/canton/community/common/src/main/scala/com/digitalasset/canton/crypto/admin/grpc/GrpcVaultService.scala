// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.admin.v30
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.crypto.{v30 as cryptoproto, *}
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, StaticGrpcServices}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  ProtoConverter,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcVaultService(
    crypto: Crypto,
    enablePreviewFeatures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext, err: ErrorLoggingContext)
    extends v30.VaultServiceGrpc.VaultService
    with NamedLogging {

  private def listPublicKeys(
      filters: Option[v30.ListKeysFilters],
      pool: Iterable[PublicKeyWithName],
  ): Seq[PublicKeyWithName] =
    pool
      .filter(entry =>
        filters.forall { filter =>
          entry.publicKey.fingerprint.unwrap.startsWith(filter.fingerprint) && entry.name
            .map(_.unwrap)
            .getOrElse("")
            .contains(filter.name) && filter.purpose
            .forall(_ == entry.publicKey.purpose.toProtoEnum)
        }
      )
      .toSeq

  // returns public keys of which we have private keys
  override def listMyKeys(request: v30.ListMyKeysRequest): Future[v30.ListMyKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      keys <- EitherT.right(crypto.cryptoPublicStore.publicKeysWithName)
      publicKeys <-
        keys.toList.parFilterA(pk =>
          crypto.cryptoPrivateStore
            .existsPrivateKey(pk.publicKey.id, pk.publicKey.purpose)
            .leftMap { err =>
              CryptoPrivateStoreError.ErrorCode
                .WrapStr(s"Failed to check key ${pk.publicKey.id}'s existence: $err")
            }
        )
      filteredPublicKeys = listPublicKeys(request.filters, publicKeys)
      keysMetadata <-
        crypto.cryptoPrivateStore.toExtended match {
          case Some(extended) =>
            filteredPublicKeys.parTraverse { pk =>
              for {
                encrypted <- extended
                  .encrypted(pk.publicKey.id)
                  .leftMap[BaseCantonError] { err =>
                    CryptoPrivateStoreError.ErrorCode.WrapStr(
                      s"Failed to retrieve encrypted status for key ${pk.publicKey.id}: $err"
                    )
                  }
              } yield PrivateKeyMetadata(pk, encrypted, None).toProtoV30
            }
          case None =>
            filteredPublicKeys.parTraverse { pk =>
              crypto.cryptoPrivateStore
                .queryKmsKeyId(pk.id)
                .map(PrivateKeyMetadata(pk, None, _).toProtoV30)
                .leftMap[BaseCantonError] { err =>
                  CryptoPrivateStoreError.ErrorCode.WrapStr(
                    s"Failed to retrieve KMS key id for key ${pk.publicKey.id}: $err"
                  )
                }
            }
        }
    } yield v30.ListMyKeysResponse(keysMetadata)

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v30.ImportPublicKeyRequest
  ): Future[v30.ImportPublicKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      publicKey <-
        FutureUnlessShutdown.wrap(
          ProtoConverter
            .parse(
              cryptoproto.PublicKey.parseFrom,
              PublicKey.fromProtoPublicKeyV30,
              request.publicKey,
            )
            .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
        )
      name <- FutureUnlessShutdown.wrap(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      _ <- crypto.cryptoPublicStore.storePublicKey(publicKey, name.emptyStringAsNone)
    } yield v30.ImportPublicKeyResponse(fingerprint = publicKey.fingerprint.unwrap)
  }.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

  override def listPublicKeys(
      request: v30.ListPublicKeysRequest
  ): Future[v30.ListPublicKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    crypto.cryptoPublicStore.publicKeysWithName
      .map { keys =>
        v30.ListPublicKeysResponse(listPublicKeys(request.filters, keys).map(_.toProtoV30))
      }
      .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
  }

  /** Generates a new signing key. If there is an empty usage in the request (i.e. and old key request),
    * it defaults to all.
    */
  override def generateSigningKey(
      request: v30.GenerateSigningKeyRequest
  ): Future[v30.GenerateSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keyScheme.isSigningKeySchemeUnspecified)
          Future.successful(crypto.privateCrypto.defaultSigningKeyScheme)
        else
          Future(
            SigningKeyScheme
              .fromProtoEnum("key_scheme", request.keyScheme)
              .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
          )
      usage <- Future(
        // for commands, we should not default to All; instead, the request should fail because usage is now a mandatory parameter.
        SigningKeyUsage
          .fromProtoListWithoutDefault(request.usage)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      name <- Future(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      key <- CantonGrpcUtil.mapErrNewEUS(
        crypto
          .generateSigningKey(scheme, usage, name.emptyStringAsNone)
          .leftMap(err => SigningKeyGenerationError.ErrorCode.Wrap(err))
      )
    } yield v30.GenerateSigningKeyResponse(publicKey = Some(key.toProtoV30))
  }

  override def generateEncryptionKey(
      request: v30.GenerateEncryptionKeyRequest
  ): Future[v30.GenerateEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keySpec.isEncryptionKeySpecUnspecified)
          Future.successful(crypto.privateCrypto.defaultEncryptionKeySpec)
        else
          Future(
            EncryptionKeySpec
              .fromProtoEnum("key_spec", request.keySpec)
              .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
          )
      name <- Future(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      key <- CantonGrpcUtil.mapErrNewEUS(
        crypto
          .generateEncryptionKey(scheme, name.emptyStringAsNone)
          .leftMap(err => EncryptionKeyGenerationError.ErrorCode.Wrap(err))
      )
    } yield v30.GenerateEncryptionKeyResponse(publicKey = Some(key.toProtoV30))
  }

  override def registerKmsSigningKey(
      request: v30.RegisterKmsSigningKeyRequest
  ): Future[v30.RegisterKmsSigningKeyResponse] =
    Future.failed[v30.RegisterKmsSigningKeyResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def registerKmsEncryptionKey(
      request: v30.RegisterKmsEncryptionKeyRequest
  ): Future[v30.RegisterKmsEncryptionKeyResponse] =
    Future.failed[v30.RegisterKmsEncryptionKeyResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def rotateWrapperKey(
      request: v30.RotateWrapperKeyRequest
  ): Future[v30.RotateWrapperKeyResponse] =
    Future.failed[v30.RotateWrapperKeyResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def getWrapperKeyId(
      request: v30.GetWrapperKeyIdRequest
  ): Future[v30.GetWrapperKeyIdResponse] =
    Future.failed[v30.GetWrapperKeyIdResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def exportKeyPair(
      request: v30.ExportKeyPairRequest
  ): Future[v30.ExportKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      // TODO(i13613): Remove feature flag check in favor of exportable keys check
      _ <-
        if (enablePreviewFeatures) FutureUnlessShutdown.unit
        else
          FutureUnlessShutdown.failed(
            Status.FAILED_PRECONDITION
              .withDescription("Remote export of private keys only allowed when preview is enabled")
              .asRuntimeException()
          )
      cryptoPrivateStore <-
        FutureUnlessShutdown.wrap(
          crypto.cryptoPrivateStore.toExtended.getOrElse(
            throw Status.FAILED_PRECONDITION
              .withDescription(
                "The selected crypto provider does not support exporting of private keys."
              )
              .asRuntimeException()
          )
        )
      fingerprint <-
        FutureUnlessShutdown.wrap(
          Fingerprint
            .fromProtoPrimitive(request.fingerprint)
            .valueOr(err =>
              throw ProtoDeserializationFailure
                .WrapNoLoggingStr(s"Failed to deserialize fingerprint: $err")
                .asGrpcError
            )
        )
      protocolVersion <-
        FutureUnlessShutdown.wrap(
          ProtocolVersion
            .fromProtoPrimitive(request.protocolVersion)
            .valueOr(err =>
              throw ProtoDeserializationFailure
                .WrapNoLoggingStr(s"Protocol version failure: $err")
                .asGrpcError
            )
        )
      privateKey <-
        EitherTUtil.toFutureUnlessShutdown(
          cryptoPrivateStore
            .exportPrivateKey(fingerprint)
            .leftMap(_.toString)
            .subflatMap(_.toRight(s"no private key found for [$fingerprint]"))
            .leftMap(err =>
              Status.FAILED_PRECONDITION
                .withDescription(s"Error retrieving private key [$fingerprint] $err")
                .asRuntimeException()
            )
        )
      publicKey <- EitherTUtil.toFutureUnlessShutdown(
        crypto.cryptoPublicStore
          .publicKey(fingerprint)
          .toRight(s"no public key found for [$fingerprint]")
          .leftMap(err =>
            Status.FAILED_PRECONDITION
              .withDescription(s"Error retrieving public key [$fingerprint] $err")
              .asRuntimeException()
          )
      )
      keyPair <- (publicKey, privateKey) match {
        case (pub: SigningPublicKey, pkey: SigningPrivateKey) =>
          FutureUnlessShutdown.pure(new SigningKeyPair(pub, pkey))
        case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
          FutureUnlessShutdown.pure(new EncryptionKeyPair(pub, pkey))
        case _ =>
          FutureUnlessShutdown.failed(
            Status.INVALID_ARGUMENT
              .withDescription(
                "public and private keys must have same purpose"
              )
              .asRuntimeException()
          )
      }

      // Encrypt keypair if password is provided
      resultE = OptionUtil.emptyStringAsNone(request.password) match {
        case Some(password) =>
          for {
            encryptedKeyPair <- crypto.pureCrypto
              .encryptWithPassword(
                keyPair,
                password,
                protocolVersion,
              )
          } yield v30.ExportKeyPairResponse(keyPair =
            encryptedKeyPair.toByteString(protocolVersion)
          )
        case None =>
          Right(v30.ExportKeyPairResponse(keyPair = keyPair.toByteString(protocolVersion)))
      }

      result <- FutureUnlessShutdown.outcomeF(resultE.toFuture { err =>
        Status.FAILED_PRECONDITION
          .withDescription(s"Failed to encrypt exported keypair with password: $err")
          .asRuntimeException()
      })
    } yield result
  }.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

  override def importKeyPair(
      request: v30.ImportKeyPairRequest
  ): Future[v30.ImportKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    def parseKeyPair(
        keyPairBytes: ByteString
    ): Either[DeserializationError, CryptoKeyPair[PublicKey, PrivateKey]] =
      CryptoKeyPair
        .fromTrustedByteString(keyPairBytes)
        .leftFlatMap { firstErr =>
          // Fallback to parse the old protobuf message format
          ProtoConverter
            .parse(
              cryptoproto.CryptoKeyPair.parseFrom,
              CryptoKeyPair.fromProtoCryptoKeyPairV30,
              keyPairBytes,
            )
            .leftMap(secondErr => s"Failed to parse crypto key pair: $firstErr, $secondErr")
        }
        .leftMap(err => DefaultDeserializationError(err))

    def loadKeyPair(
        validatedName: Option[KeyName],
        keyPair: CryptoKeyPair[PublicKey, PrivateKey],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
      for {
        cryptoPrivateStore <- FutureUnlessShutdown.wrap(
          crypto.cryptoPrivateStore.toExtended.getOrElse(
            throw Status.FAILED_PRECONDITION
              .withDescription(
                "The selected crypto provider does not support importing of private keys."
              )
              .asRuntimeException()
          )
        )
        _ <- crypto.cryptoPublicStore.storePublicKey(keyPair.publicKey, validatedName)
        _ = logger.info(s"Uploading key $validatedName")
        _ <- cryptoPrivateStore
          .storePrivateKey(keyPair.privateKey, validatedName)
          .valueOr(err => throw CryptoPrivateStoreError.ErrorCode.Wrap(err).asGrpcError)
      } yield ()

    for {
      validatedName <- FutureUnlessShutdown.wrap(
        OptionUtil
          .emptyStringAsNone(request.name)
          .traverse(KeyName.create)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )

      // Decrypt the keypair if a password is provided
      keyPair <-
        OptionUtil.emptyStringAsNone(request.password) match {
          case Some(password) =>
            val resultE = for {
              encrypted <- PasswordBasedEncrypted
                .fromTrustedByteString(request.keyPair)
                .leftMap(err => ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)

              keyPair <- crypto.pureCrypto
                .decryptWithPassword(encrypted, password)(parseKeyPair)
                .leftMap(err =>
                  Status.FAILED_PRECONDITION
                    .withDescription(s"Failed to decrypt encrypted keypair with password: $err")
                    .asRuntimeException()
                )
            } yield keyPair

            resultE.toFutureUS(identity)

          case None =>
            parseKeyPair(request.keyPair).toFutureUS { err =>
              ProtoDeserializationFailure.WrapNoLoggingStr(err.message).asGrpcError
            }
        }

      _ <- loadKeyPair(validatedName, keyPair)
    } yield v30.ImportKeyPairResponse()
  }.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)

  override def deleteKeyPair(
      request: v30.DeleteKeyPairRequest
  ): Future[v30.DeleteKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      fingerprint <- FutureUnlessShutdown.wrap(
        Fingerprint
          .fromProtoPrimitive(request.fingerprint)
          .valueOr { err =>
            throw ProtoDeserializationFailure
              .WrapNoLoggingStr(s"Failed to parse key fingerprint: $err")
              .asGrpcError
          }
      )
      _ <- crypto.cryptoPrivateStore
        .removePrivateKey(fingerprint)
        .valueOr { err =>
          throw Status.FAILED_PRECONDITION
            .withDescription(s"Failed to remove private key: $err")
            .asRuntimeException()
        }
      _ <- crypto.cryptoPublicStore.deleteKey(fingerprint)
    } yield v30.DeleteKeyPairResponse()
  }.failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
}

object GrpcVaultService {
  trait GrpcVaultServiceFactory {
    def create(
        crypto: Crypto,
        enablePreviewFeatures: Boolean,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, err: ErrorLoggingContext): GrpcVaultService
  }

  class CommunityGrpcVaultServiceFactory extends GrpcVaultServiceFactory {
    override def create(
        crypto: Crypto,
        enablePreviewFeatures: Boolean,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, err: ErrorLoggingContext): GrpcVaultService =
      new GrpcVaultService(crypto, enablePreviewFeatures, loggerFactory)
  }
}

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
