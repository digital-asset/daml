// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.store.CryptoPublicStoreError
import com.digitalasset.canton.crypto.{v0 as cryptoproto, *}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty

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
      keys <- EitherTUtil.toFuture(
        mapErr(
          crypto.cryptoPublicStore.publicKeysWithName.leftMap(err =>
            s"Failed to retrieve public keys: $err"
          )
        )
      )
      publicKeys <- EitherTUtil.toFuture(
        mapErr(
          keys.toList.parFilterA(pk =>
            crypto.cryptoPrivateStore
              .existsPrivateKey(pk.publicKey.id, pk.publicKey.purpose)
              .leftMap[String](err => s"Failed to check key ${pk.publicKey.id}'s existence: $err")
          )
        )
      )
      filteredPublicKeys = listPublicKeys(request, publicKeys)
      keysMetadata <- EitherTUtil.toFuture(
        mapErr(
          filteredPublicKeys.parTraverse { pk =>
            (crypto.cryptoPrivateStore.toExtended match {
              case Some(extended) =>
                extended
                  .encrypted(pk.publicKey.id)
                  .leftMap[String](err =>
                    s"Failed to retrieve encrypted status for key ${pk.publicKey.id}: $err"
                  )
              case None => EitherT.rightT[Future, String](None)
            }).map(encrypted => PrivateKeyMetadata(pk, encrypted).toProtoV0)
          }
        )
      )
    } yield v0.ListMyKeysResponse(keysMetadata)
  }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v0.ImportPublicKeyRequest
  ): Future[v0.ImportPublicKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      publicKey <- mapErr(
        ProtoConverter
          .parse(
            cryptoproto.PublicKey.parseFrom,
            PublicKey.fromProtoPublicKeyV0,
            request.publicKey,
          )
          .leftMap(err => s"Failed to parse public key from protobuf: $err")
          .toEitherT[Future]
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      _ <- mapErr(
        crypto.cryptoPublicStore
          .storePublicKey(publicKey, name.emptyStringAsNone)
          .leftMap(err => s"Failed to store public key: $err")
      )
    } yield v0.ImportPublicKeyResponse(fingerprint = publicKey.fingerprint.unwrap)

    EitherTUtil.toFuture(res)
  }

  override def listPublicKeys(request: v0.ListKeysRequest): Future[v0.ListKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture(
      mapErr(
        crypto.cryptoPublicStore.publicKeysWithName
          .map(keys => v0.ListKeysResponse(listPublicKeys(request, keys).map(_.toProtoV0)))
          .leftMap(err => s"Failed to retrieve public keys: $err")
      )
    )
  }

  override def generateSigningKey(
      request: v0.GenerateSigningKeyRequest
  ): Future[v0.GenerateSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      scheme <- mapErr(
        if (request.keyScheme.isMissingSigningKeyScheme)
          Right(crypto.privateCrypto.defaultSigningKeyScheme)
        else
          SigningKeyScheme.fromProtoEnum("key_scheme", request.keyScheme)
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      key <- mapErr(crypto.generateSigningKey(scheme, name.emptyStringAsNone))
    } yield v0.GenerateSigningKeyResponse(publicKey = Some(key.toProtoV0))
    EitherTUtil.toFuture(res)
  }

  override def generateEncryptionKey(
      request: v0.GenerateEncryptionKeyRequest
  ): Future[v0.GenerateEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      scheme <- mapErr(
        if (request.keyScheme.isMissingEncryptionKeyScheme)
          Right(crypto.privateCrypto.defaultEncryptionKeyScheme)
        else
          EncryptionKeyScheme.fromProtoEnum("key_scheme", request.keyScheme)
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      key <- mapErr(crypto.generateEncryptionKey(scheme, name.emptyStringAsNone))
    } yield v0.GenerateEncryptionKeyResponse(publicKey = Some(key.toProtoV0))
    EitherTUtil.toFuture(res)
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
    val res = for {
      // TODO(i13613): Remove feature flag check in favor of exportable keys check
      _ <- EitherTUtil.condUnitET[Future](
        enablePreviewFeatures,
        "Remote export of private keys only allowed when preview is enabled",
      )
      cryptoPrivateStore <- crypto.cryptoPrivateStore.toExtended
        .toRight(
          "The selected crypto provider does not support exporting of private keys."
        )
        .toEitherT[Future]
      fingerprint <- Fingerprint
        .fromProtoPrimitive(request.fingerprint)
        .leftMap(err => s"Failed to deserialize fingerprint: $err")
        .toEitherT[Future]
      protocolVersion <- ProtocolVersion
        .fromProtoPrimitive(request.protocolVersion)
        .leftMap(err => s"Protocol version failure: $err")
        .toEitherT[Future]
      privateKey <- cryptoPrivateStore
        .exportPrivateKey(fingerprint)
        .leftMap(_.toString)
        .subflatMap(_.toRight(s"no private key found for [$fingerprint]"))
        .leftMap(err => s"Error retrieving private key [$fingerprint] $err")
      publicKey <- crypto.cryptoPublicStore
        .publicKey(fingerprint)
        .leftMap(_.toString)
        .subflatMap(_.toRight(s"no public key found for [$fingerprint]"))
        .leftMap(err => s"Error retrieving public key [$fingerprint] $err")
      keyPair <- (publicKey, privateKey) match {
        case (pub: SigningPublicKey, pkey: SigningPrivateKey) =>
          EitherT.rightT[Future, String](new SigningKeyPair(pub, pkey))
        case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
          EitherT.rightT[Future, String](new EncryptionKeyPair(pub, pkey))
        case _ =>
          EitherT.leftT[Future, CryptoKeyPair[PublicKey, PrivateKey]](
            "public and private keys must have same purpose"
          )
      }
    } yield v0.ExportKeyPairResponse(keyPair = keyPair.toByteString(protocolVersion))

    EitherTUtil.toFuture(mapErr(res))
  }

  override def importKeyPair(request: v0.ImportKeyPairRequest): Future[v0.ImportKeyPairResponse] = {
    def parseKeyPair(
        keyPairBytes: ByteString
    ): Either[String, CryptoKeyPair[PublicKey, PrivateKey]] = {
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
    }

    def loadKeyPair(
        validatedName: Option[KeyName],
        keyPair: CryptoKeyPair[PublicKey, PrivateKey],
    )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
      for {
        cryptoPrivateStore <- crypto.cryptoPrivateStore.toExtended
          .toRight(
            "The selected crypto provider does not support importing of private keys."
          )
          .toEitherT[Future]
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
          .leftMap(_.toString)
        _ <- cryptoPrivateStore
          .storePrivateKey(keyPair.privateKey, validatedName)
          .leftMap(_.toString)
      } yield ()

    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      validatedName <- OptionUtil
        .emptyStringAsNone(request.name)
        .traverse(KeyName.create)
        .toEitherT[Future]
      keyPair <- parseKeyPair(request.keyPair).toEitherT[Future]
      _ <- loadKeyPair(validatedName, keyPair)
    } yield v0.ImportKeyPairResponse()

    EitherTUtil.toFuture(mapErr(res))
  }

  override def deleteKeyPair(request: v0.DeleteKeyPairRequest): Future[v0.DeleteKeyPairResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      fingerprint <- Fingerprint
        .fromProtoPrimitive(request.fingerprint)
        .leftMap(err => s"Failed to parse key fingerprint: $err")
        .toEitherT[Future]
      _ <- crypto.cryptoPrivateStore
        .removePrivateKey(fingerprint)
        .leftMap(err => s"Failed to remove private key: $err")
    } yield v0.DeleteKeyPairResponse()

    EitherTUtil.toFuture(mapErr(res))
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
