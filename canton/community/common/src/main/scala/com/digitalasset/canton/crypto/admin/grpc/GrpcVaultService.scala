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
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{v0 as cryptoproto, *}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcVaultService(
    crypto: Crypto,
    enablePreviewFeatures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v0.VaultServiceGrpc.VaultService
    with NamedLogging {

  private def listPublicKeys(
      request: v0.ListKeysRequest,
      pool: Iterable[PublicKeyWithName],
  ): Seq[PublicKeyWithName] =
    pool
      .filter(entry =>
        entry.publicKey.fingerprint.unwrap.startsWith(request.filterFingerprint)
          && entry.name.map(_.unwrap).getOrElse("").contains(request.filterName)
          && request.filterPurpose.forall(_ == entry.publicKey.purpose.toProtoEnum)
      )
      .toSeq

  // returns public keys of which we have private keys
  override def listMyKeys(request: v0.ListKeysRequest): Future[v0.ListMyKeysResponse] = {
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
      filteredPublicKeys = listPublicKeys(request, publicKeys)
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
          }).map(encrypted => PrivateKeyMetadata(pk, encrypted).toProtoV0)
        }
    } yield v0.ListMyKeysResponse(keysMetadata)
  }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v0.ImportPublicKeyRequest
  ): Future[v0.ImportPublicKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      publicKey <-
        Future(
          ProtoConverter
            .parse(
              cryptoproto.PublicKey.parseFrom,
              PublicKey.fromProtoPublicKeyV0,
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
    } yield v0.ImportPublicKeyResponse(fingerprint = publicKey.fingerprint.unwrap)
  }

  override def listPublicKeys(request: v0.ListKeysRequest): Future[v0.ListKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    crypto.cryptoPublicStore.publicKeysWithName
      .map(keys => v0.ListKeysResponse(listPublicKeys(request, keys).map(_.toProtoV0)))
      .valueOr(err =>
        throw CryptoPublicStoreError.ErrorCode
          .WrapStr(s"Failed to retrieve public keys: $err")
          .asGrpcError
      )
  }

  override def generateSigningKey(
      request: v0.GenerateSigningKeyRequest
  ): Future[v0.GenerateSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keyScheme.isMissingSigningKeyScheme)
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
    } yield v0.GenerateSigningKeyResponse(publicKey = Some(key.toProtoV0))
  }

  override def generateEncryptionKey(
      request: v0.GenerateEncryptionKeyRequest
  ): Future[v0.GenerateEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keyScheme.isMissingEncryptionKeyScheme)
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
    } yield v0.GenerateEncryptionKeyResponse(publicKey = Some(key.toProtoV0))
  }

  override def registerKmsSigningKey(
      request: v0.RegisterKmsSigningKeyRequest
  ): Future[v0.RegisterKmsSigningKeyResponse] =
    Future.failed[v0.RegisterKmsSigningKeyResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def registerKmsEncryptionKey(
      request: v0.RegisterKmsEncryptionKeyRequest
  ): Future[v0.RegisterKmsEncryptionKeyResponse] =
    Future.failed[v0.RegisterKmsEncryptionKeyResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def rotateWrapperKey(
      request: v0.RotateWrapperKeyRequest
  ): Future[Empty] =
    Future.failed[Empty](StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException())

  override def getWrapperKeyId(
      request: v0.GetWrapperKeyIdRequest
  ): Future[v0.GetWrapperKeyIdResponse] =
    Future.failed[v0.GetWrapperKeyIdResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

  override def exportKeyPair(request: v0.ExportKeyPairRequest): Future[v0.ExportKeyPairResponse] = {
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
    } yield v0.ExportKeyPairResponse(keyPair = keyPair.toByteString(protocolVersion))
  }

  override def importKeyPair(request: v0.ImportKeyPairRequest): Future[v0.ImportKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    def parseKeyPair(
        keyPairBytes: ByteString
    ): Future[CryptoKeyPair[PublicKey, PrivateKey]] = {
      Future(
        CryptoKeyPair
          .fromByteString(keyPairBytes)
          .leftFlatMap { firstErr =>
            // Fallback to parse the old protobuf message format
            ProtoConverter
              .parse(
                cryptoproto.CryptoKeyPair.parseFrom,
                CryptoKeyPair.fromProtoCryptoKeyPairV0,
                keyPairBytes,
              )
              .leftMap(secondErr => s"Failed to parse crypto key pair: $firstErr, $secondErr")
          }
          .valueOr(err =>
            throw ProtoDeserializationFailure
              .WrapNoLoggingStr(err)
              .asGrpcError
          )
      )
    }

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
      keyPair <- parseKeyPair(request.keyPair)
      _ <- loadKeyPair(validatedName, keyPair)
    } yield v0.ImportKeyPairResponse()
  }

  override def deleteKeyPair(request: v0.DeleteKeyPairRequest): Future[v0.DeleteKeyPairResponse] = {
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
    } yield v0.DeleteKeyPairResponse()
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

  def toProtoV0: v0.PrivateKeyMetadata =
    v0.PrivateKeyMetadata(
      publicKeyWithName = Some(publicKeyWithName.toProtoV0),
      wrapperKeyId = OptionUtil.noneAsEmptyString(wrapperKeyId.map(_.toProtoPrimitive)),
    )
}

object PrivateKeyMetadata {

  def fromProtoV0(key: v0.PrivateKeyMetadata): ParsingResult[PrivateKeyMetadata] =
    for {
      publicKeyWithName <- ProtoConverter.parseRequired(
        PublicKeyWithName.fromProtoV0,
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
