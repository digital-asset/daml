// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.admin.v30
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{v30 as cryptoproto, *}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  ProtoConverter,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcVaultService(
    crypto: Crypto,
    enablePreviewFeatures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.VaultServiceGrpc.VaultService
    with NamedLogging {

  private def listPublicKeys(
      filters: Option[v30.ListKeysFilters],
      pool: Iterable[PublicKeyWithName],
  ): Seq[PublicKeyWithName] =
    pool
      .filter(entry =>
        filters.forall { filter =>
          entry.publicKey.fingerprint.unwrap.startsWith(filter.fingerprint)
          && entry.name.map(_.unwrap).getOrElse("").contains(filter.name)
          && filter.purpose.forall(_ == entry.publicKey.purpose.toProtoEnum)
        }
      )
      .toSeq

  // returns public keys of which we have private keys
  override def listMyKeys(request: v30.ListMyKeysRequest): Future[v30.ListMyKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      keys <- crypto.cryptoPublicStore.publicKeysWithName.valueOr(err =>
        throw CryptoPublicStoreError.ErrorCode
          .WrapStr(s"Failed to retrieve public keys: $err")
          .asGrpcError
      )
      publicKeys <-
        keys.toList.parFilterA(pk =>
          crypto.cryptoPrivateStore
            .existsPrivateKey(pk.publicKey.id, pk.publicKey.purpose)
            .valueOr(err =>
              throw CryptoPrivateStoreError.ErrorCode
                .WrapStr(s"Failed to check key ${pk.publicKey.id}'s existence: $err")
                .asGrpcError
            )
        )
      filteredPublicKeys = listPublicKeys(request.filters, publicKeys)
      keysMetadata <-
        filteredPublicKeys.parTraverse { pk =>
          (crypto.cryptoPrivateStore.toExtended match {
            case Some(extended) =>
              extended
                .encrypted(pk.publicKey.id)
                .valueOr(err =>
                  throw CryptoPrivateStoreError.ErrorCode
                    .WrapStr(
                      s"Failed to retrieve encrypted status for key ${pk.publicKey.id}: $err"
                    )
                    .asGrpcError
                )
            case None => Future.successful(None)
          }).map(encrypted => PrivateKeyMetadata(pk, encrypted).toProtoV30)
        }
    } yield v30.ListMyKeysResponse(keysMetadata)
  }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v30.ImportPublicKeyRequest
  ): Future[v30.ImportPublicKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      publicKey <-
        Future(
          ProtoConverter
            .parse(
              cryptoproto.PublicKey.parseFrom,
              PublicKey.fromProtoPublicKeyV30,
              request.publicKey,
            )
            .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
        )
      name <- Future(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      _ <- crypto.cryptoPublicStore
        .storePublicKey(publicKey, name.emptyStringAsNone)
        .valueOr(err => throw CryptoPublicStoreError.ErrorCode.Wrap(err).asGrpcError)
    } yield v30.ImportPublicKeyResponse(fingerprint = publicKey.fingerprint.unwrap)
  }

  override def listPublicKeys(
      request: v30.ListPublicKeysRequest
  ): Future[v30.ListPublicKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    crypto.cryptoPublicStore.publicKeysWithName
      .map(keys =>
        v30.ListPublicKeysResponse(listPublicKeys(request.filters, keys).map(_.toProtoV30))
      )
      .valueOr(err =>
        throw CryptoPublicStoreError.ErrorCode
          .WrapStr(s"Failed to retrieve public keys: $err")
          .asGrpcError
      )
  }

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
      name <- Future(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      key <- crypto
        .generateSigningKey(scheme, name.emptyStringAsNone)
        .valueOr(err => throw SigningKeyGenerationError.ErrorCode.Wrap(err).asGrpcError)
    } yield v30.GenerateSigningKeyResponse(publicKey = Some(key.toProtoV30))
  }

  override def generateEncryptionKey(
      request: v30.GenerateEncryptionKeyRequest
  ): Future[v30.GenerateEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keyScheme.isEncryptionKeySchemeUnspecified)
          Future.successful(crypto.privateCrypto.defaultEncryptionKeyScheme)
        else
          Future(
            EncryptionKeyScheme
              .fromProtoEnum("key_scheme", request.keyScheme)
              .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
          )
      name <- Future(
        KeyName
          .fromProtoPrimitive(request.name)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      key <- crypto
        .generateEncryptionKey(scheme, name.emptyStringAsNone)
        .valueOr(err => throw EncryptionKeyGenerationError.ErrorCode.Wrap(err).asGrpcError)
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
        if (enablePreviewFeatures) Future.unit
        else
          Future.failed(
            Status.FAILED_PRECONDITION
              .withDescription("Remote export of private keys only allowed when preview is enabled")
              .asRuntimeException()
          )
      cryptoPrivateStore <-
        Future(
          crypto.cryptoPrivateStore.toExtended.getOrElse(
            throw Status.FAILED_PRECONDITION
              .withDescription(
                "The selected crypto provider does not support exporting of private keys."
              )
              .asRuntimeException()
          )
        )
      fingerprint <-
        Future(
          Fingerprint
            .fromProtoPrimitive(request.fingerprint)
            .valueOr(err =>
              throw ProtoDeserializationFailure
                .WrapNoLoggingStr(s"Failed to deserialize fingerprint: $err")
                .asGrpcError
            )
        )
      protocolVersion <-
        Future(
          ProtocolVersion
            .fromProtoPrimitive(request.protocolVersion)
            .valueOr(err =>
              throw ProtoDeserializationFailure
                .WrapNoLoggingStr(s"Protocol version failure: $err")
                .asGrpcError
            )
        )
      privateKey <-
        cryptoPrivateStore
          .exportPrivateKey(fingerprint)
          .leftMap(_.toString)
          .subflatMap(_.toRight(s"no private key found for [$fingerprint]"))
          .valueOr(err =>
            throw Status.FAILED_PRECONDITION
              .withDescription(s"Error retrieving private key [$fingerprint] $err")
              .asRuntimeException()
          )
      publicKey <- crypto.cryptoPublicStore
        .publicKey(fingerprint)
        .leftMap(_.toString)
        .subflatMap(_.toRight(s"no public key found for [$fingerprint]"))
        .valueOr(err =>
          throw Status.FAILED_PRECONDITION
            .withDescription(s"Error retrieving public key [$fingerprint] $err")
            .asRuntimeException()
        )
      keyPair <- (publicKey, privateKey) match {
        case (pub: SigningPublicKey, pkey: SigningPrivateKey) =>
          Future.successful(new SigningKeyPair(pub, pkey))
        case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
          Future.successful(new EncryptionKeyPair(pub, pkey))
        case _ =>
          Future.failed(
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

      result <- resultE.toFuture { err =>
        Status.FAILED_PRECONDITION
          .withDescription(s"Failed to encrypt exported keypair with password: $err")
          .asRuntimeException()
      }
    } yield result
  }

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
    )(implicit traceContext: TraceContext): Future[Unit] =
      for {
        cryptoPrivateStore <- Future(
          crypto.cryptoPrivateStore.toExtended.getOrElse(
            throw Status.FAILED_PRECONDITION
              .withDescription(
                "The selected crypto provider does not support importing of private keys."
              )
              .asRuntimeException()
          )
        )
        _ <- crypto.cryptoPublicStore
          .storePublicKey(keyPair.publicKey, validatedName)
          .recoverWith {
            // if the existing key is the same, then ignore error
            case error: CryptoPublicStoreError.KeyAlreadyExists =>
              for {
                existing <- crypto.cryptoPublicStore.publicKey(keyPair.publicKey.fingerprint)
                _ <-
                  if (existing.contains(keyPair.publicKey))
                    EitherT.rightT[Future, CryptoPublicStoreError](())
                  else EitherT.leftT[Future, Unit](error: CryptoPublicStoreError)
              } yield ()
          }
          .valueOr(err => throw CryptoPublicStoreError.ErrorCode.Wrap(err).asGrpcError)
        _ = logger.info(s"Uploading key ${validatedName}")
        _ <- cryptoPrivateStore
          .storePrivateKey(keyPair.privateKey, validatedName)
          .valueOr(err => throw CryptoPrivateStoreError.ErrorCode.Wrap(err).asGrpcError)
      } yield ()

    for {
      validatedName <- Future(
        OptionUtil
          .emptyStringAsNone(request.name)
          .traverse(KeyName.create)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )

      // Decrypt the keypair if a password is provided
      keyPair <- OptionUtil.emptyStringAsNone(request.password) match {
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

          resultE.toFuture(identity)

        case None =>
          parseKeyPair(request.keyPair).toFuture { err =>
            ProtoDeserializationFailure.WrapNoLoggingStr(err.message).asGrpcError
          }
      }

      _ <- loadKeyPair(validatedName, keyPair)
    } yield v30.ImportKeyPairResponse()
  }

  override def deleteKeyPair(
      request: v30.DeleteKeyPairRequest
  ): Future[v30.DeleteKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      fingerprint <- Future(
        Fingerprint
          .fromProtoPrimitive(request.fingerprint)
          .valueOr(err =>
            throw ProtoDeserializationFailure
              .WrapNoLoggingStr(s"Failed to parse key fingerprint: $err")
              .asGrpcError
          )
      )
      _ <-
        crypto.cryptoPrivateStore
          .removePrivateKey(fingerprint)
          .valueOr(err =>
            throw ProtoDeserializationFailure
              .WrapNoLoggingStr(s"Failed to remove private key: $err")
              .asGrpcError
          )
    } yield v30.DeleteKeyPairResponse()
  }
}

object GrpcVaultService {
  trait GrpcVaultServiceFactory {
    def create(
        crypto: Crypto,
        enablePreviewFeatures: Boolean,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): GrpcVaultService
  }

  class CommunityGrpcVaultServiceFactory extends GrpcVaultServiceFactory {
    override def create(
        crypto: Crypto,
        enablePreviewFeatures: Boolean,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): GrpcVaultService =
      new GrpcVaultService(crypto, enablePreviewFeatures, loggerFactory)
  }
}

final case class PrivateKeyMetadata(
    publicKeyWithName: PublicKeyWithName,
    wrapperKeyId: Option[String300],
) {

  def id: Fingerprint = publicKey.id

  def publicKey: PublicKey = publicKeyWithName.publicKey

  def name: Option[KeyName] = publicKeyWithName.name

  def purpose: KeyPurpose = publicKey.purpose

  def encrypted: Boolean = wrapperKeyId.isDefined

  def toProtoV30: v30.PrivateKeyMetadata =
    v30.PrivateKeyMetadata(
      publicKeyWithName = Some(publicKeyWithName.toProtoV30),
      wrapperKeyId = OptionUtil.noneAsEmptyString(wrapperKeyId.map(_.toProtoPrimitive)),
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
      wrapperKeyId <- OptionUtil
        .emptyStringAsNone(key.wrapperKeyId)
        .traverse(keyId => String300.fromProtoPrimitive(keyId, "wrapper_key_id"))
    } yield PrivateKeyMetadata(
      publicKeyWithName,
      wrapperKeyId,
    )
}
