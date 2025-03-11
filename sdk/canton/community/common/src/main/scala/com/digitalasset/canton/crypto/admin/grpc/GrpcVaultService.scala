// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.admin.v30
import com.digitalasset.canton.crypto.kms.KmsError.{KmsCannotFindKeyError, KmsKeyDisabledError}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.provider.kms.KmsPrivateCrypto
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.WrapperKeyAlreadyInUse
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, EncryptedCryptoPrivateStore}
import com.digitalasset.canton.crypto.{v30 as cryptoproto, *}
import com.digitalasset.canton.error.{CantonBaseError, CantonError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
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

  private case class ListKeysFilter(
      fingerprint: Option[Fingerprint],
      name: String,
      purpose: Option[Seq[KeyPurpose]],
      usage: Option[NonEmpty[Set[SigningKeyUsage]]],
  )

  private def listPublicKeys(
      filter: ListKeysFilter,
      pool: Iterable[PublicKeyWithName],
  ): Seq[PublicKeyWithName] = {

    def filterFingerprint(key: PublicKeyWithName): Boolean =
      filter.fingerprint.fold(true)(f => key.publicKey.fingerprint == f)

    def filterName(key: PublicKeyWithName): Boolean =
      key.name.map(_.unwrap).getOrElse("").contains(filter.name)

    def filterPurpose(key: PublicKeyWithName): Boolean =
      filter.purpose.fold(true)(purposes => purposes.contains(key.publicKey.purpose))

    def filterUsage(key: PublicKeyWithName): Boolean =
      filter.usage.fold(true)(filterUsage =>
        key match {
          case SigningPublicKeyWithName(publicSigningKey, _) =>
            SigningKeyUsage.matchesRelevantUsages(publicSigningKey.usage, filterUsage)
          case _ => true
        }
      )

    pool
      .filter(entry =>
        filterFingerprint(entry) && filterName(entry) && filterPurpose(entry) && filterUsage(entry)
      )
      .toSeq
  }

  private def parseFilters(
      filtersO: Option[v30.ListKeysFilters]
  ): ListKeysFilter = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    filtersO.fold(
      ListKeysFilter(None, "", None, None)
    ) { filters =>
      (
        for {
          fingerprintO <- OptionUtil
            .emptyStringAsNone(filters.fingerprint)
            .traverse(Fingerprint.fromProtoPrimitive)
          name = filters.name
          purposeO <- filters.purpose
            .traverse(purpose => KeyPurpose.fromProtoEnum("purpose", purpose))
            .map(keyPurposeList => NonEmpty.from(keyPurposeList))
          usageO <- filters.usage
            .traverse(usage => SigningKeyUsage.fromProtoEnum("usage", usage))
            .map(keyUsageList => NonEmpty.from(keyUsageList.toSet))
          _ = if (purposeO.exists(_.contains(KeyPurpose.Encryption)) && usageO.exists(_.nonEmpty))
            throw ProtoDeserializationFailure
              .WrapNoLoggingStr("Cannot specify a usage when listing encryption keys")
              .asGrpcError
        } yield ListKeysFilter(fingerprintO, name, purposeO, usageO)
      ).valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
    }
  }

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
      listKeysFilters = parseFilters(request.filters)
      filteredPublicKeys = listPublicKeys(listKeysFilters, publicKeys)
      keysMetadata <-
        crypto.cryptoPrivateStore.toExtended match {
          case Some(extended) =>
            filteredPublicKeys.parTraverse { pk =>
              for {
                encrypted <- extended
                  .encrypted(pk.publicKey.id)
                  .leftMap[CantonBaseError] { err =>
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
                .leftMap[CantonBaseError] { err =>
                  CryptoPrivateStoreError.ErrorCode.WrapStr(
                    s"Failed to retrieve KMS key id for key ${pk.publicKey.id}: $err"
                  )
                }
            }
        }
    } yield v30.ListMyKeysResponse(keysMetadata)

    CantonGrpcUtil.mapErrNewEUS(result.leftMap(_.toCantonError))
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
    val listKeysFilters = parseFilters(request.filters)
    crypto.cryptoPublicStore.publicKeysWithName
      .map { keys =>
        v30.ListPublicKeysResponse(listPublicKeys(listKeysFilters, keys).map(_.toProtoV30))
      }
      .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
  }

  /** Generates a new signing key. If there is an empty usage in the request (i.e. and old key
    * request), it defaults to all.
    */
  override def generateSigningKey(
      request: v30.GenerateSigningKeyRequest
  ): Future[v30.GenerateSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheme <-
        if (request.keySpec.isSigningKeySpecUnspecified)
          Future.successful(crypto.privateCrypto.defaultSigningKeySpec)
        else
          Future(
            SigningKeySpec
              .fromProtoEnum("key_spec", request.keySpec)
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
          .leftMap(err => SigningKeyGenerationError.ErrorCode.Wrap(err).toCantonError)
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
          .leftMap(err => EncryptionKeyGenerationError.ErrorCode.Wrap(err).toCantonError)
      )
    } yield v30.GenerateEncryptionKeyResponse(publicKey = Some(key.toProtoV30))
  }

  private def getEncryptedPrivateStore: Future[EncryptedCryptoPrivateStore] =
    crypto.cryptoPrivateStore match {
      case encStore: EncryptedCryptoPrivateStore =>
        Future.successful(encStore)
      case _ =>
        CantonGrpcUtil.mapErrNew(
          EitherT.leftT[Future, EncryptedCryptoPrivateStore](
            GrpcVaultServiceError.NoEncryptedPrivateKeyStoreError.Failure()
          )
        )
    }

  private def getKmsPrivateApi: Either[CantonError, KmsPrivateCrypto] =
    crypto.privateCrypto match {
      case kmsCrypto: KmsPrivateCrypto =>
        Right(kmsCrypto)
      case _ =>
        Left(GrpcVaultServiceError.NoEncryptedPrivateKeyStoreError.Failure())
    }

  private def registerKmsKey[A <: PublicKey](
      kmsKeyId: String,
      name: String,
      registerFunc: (KmsKeyId, Option[KeyName]) => EitherT[FutureUnlessShutdown, CantonBaseError, A],
  ): EitherT[FutureUnlessShutdown, CantonBaseError, A] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      name <- EitherT.fromEither[FutureUnlessShutdown](
        KeyName
          .fromProtoPrimitive(name)
          .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)
      )
      key <- String300
        .create(kmsKeyId)
        .leftMap(err => GrpcVaultServiceError.InvalidKmsKeyId.Failure(err))
        .toEitherT[FutureUnlessShutdown]
        .flatMap(key => registerFunc(KmsKeyId(key), Some(name)))
    } yield key
  }

  override def registerKmsSigningKey(
      request: v30.RegisterKmsSigningKeyRequest
  ): Future[v30.RegisterKmsSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      kmsCrypto <- getKmsPrivateApi.toEitherT[FutureUnlessShutdown]
      pubKey <- registerKmsKey[SigningPublicKey](
        request.kmsKeyId,
        request.name,
        (key, name) => {
          /* Fail the request if we end up deserializing the request from a proto version that does not
           * have any usage.
           */
          EitherT
            .fromEither[FutureUnlessShutdown](
              SigningKeyUsage
                .fromProtoListWithoutDefault(request.usage)
                .leftMap(ProtoDeserializationFailure.WrapNoLogging.apply)
            )
            .flatMap(usage =>
              kmsCrypto.registerSigningKey(key, usage, name).leftMap { err =>
                GrpcVaultServiceError.RegisterKmsKeyInternalError
                  .Failure(err.show)
              }
            )
        },
      ).map(key => v30.RegisterKmsSigningKeyResponse(publicKey = Some(key.toProtoV30)))
        .leftMap(_.toCantonError)
    } yield pubKey

    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def registerKmsEncryptionKey(
      request: v30.RegisterKmsEncryptionKeyRequest
  ): Future[v30.RegisterKmsEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      kmsCrypto <- getKmsPrivateApi.toEitherT[FutureUnlessShutdown]
      pubKey <- registerKmsKey[EncryptionPublicKey](
        request.kmsKeyId,
        request.name,
        (key, name) =>
          kmsCrypto.registerEncryptionKey(key, name).leftMap { err =>
            GrpcVaultServiceError.RegisterKmsKeyInternalError
              .Failure(err.show)
          },
      ).map(key => v30.RegisterKmsEncryptionKeyResponse(publicKey = Some(key.toProtoV30)))
        .leftMap(_.toCantonError)
    } yield pubKey

    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def rotateWrapperKey(
      request: v30.RotateWrapperKeyRequest
  ): Future[v30.RotateWrapperKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    def rotateWrapperKeyInStore(
        encStore: EncryptedCryptoPrivateStore,
        keyId: Option[KmsKeyId],
    ): Future[v30.RotateWrapperKeyResponse] =
      for {
        newWrapperKeyToUse <- CantonGrpcUtil.mapErrNewEUS {
          encStore
            .checkWrapperKeyExistsOrCreateNewOne(
              encStore.kms,
              keyId,
            )
            .leftMap {
              case KmsCannotFindKeyError(keyId, _) =>
                GrpcVaultServiceError.WrapperKeyNotExistError
                  .Failure(keyId)
              case KmsKeyDisabledError(keyId, _, _) =>
                GrpcVaultServiceError.WrapperKeyDisabledOrDeletedError
                  .Failure(keyId)
              case err =>
                GrpcVaultServiceError.WrapperKeyRotationInternalError
                  .Failure(err.show)
            }
        }
        _ <- CantonGrpcUtil.mapErrNewEUS {
          encStore
            .rotateWrapperKey(newWrapperKeyToUse)
            .leftMap {
              case _: WrapperKeyAlreadyInUse =>
                GrpcVaultServiceError.WrapperKeyAlreadyInUseError
                  .Failure(newWrapperKeyToUse)
              case err =>
                GrpcVaultServiceError.WrapperKeyRotationInternalError
                  .Failure(err.show)
            }
        }
      } yield v30.RotateWrapperKeyResponse()

    getEncryptedPrivateStore.flatMap { encStore =>
      OptionUtil
        .emptyStringAsNone(request.newWrapperKeyId)
        .map(x => String300.create(x)) match {
        case Some(Left(err)) =>
          val invalidIdError = GrpcVaultServiceError.InvalidKmsKeyId.Failure(err)
          CantonGrpcUtil.mapErrNew(
            EitherT.leftT[Future, v30.RotateWrapperKeyResponse](invalidIdError)
          )
        case Some(Right(key)) =>
          rotateWrapperKeyInStore(encStore, Some(KmsKeyId(key)))
        case None =>
          rotateWrapperKeyInStore(encStore, None)
      }
    }

  }

  override def getWrapperKeyId(
      request: v30.GetWrapperKeyIdRequest
  ): Future[v30.GetWrapperKeyIdResponse] =
    getEncryptedPrivateStore.map { encStore =>
      val keyId = encStore.wrapperKeyId
      v30.GetWrapperKeyIdResponse(wrapperKeyId = keyId.str.toProtoPrimitive)
    }

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
                keyPair.toByteString(protocolVersion),
                password,
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
        _ = logger.info(
          s"Uploading key $validatedName with fingerprint ${keyPair.publicKey.fingerprint}"
        )
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

sealed trait GrpcVaultServiceError extends CantonError with Product with Serializable

object GrpcVaultServiceError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("Internal error emitted upon internal wrapper key rotation errors")
  @Resolution("Contact support")
  object WrapperKeyRotationInternalError
      extends ErrorCode(
        "WRAPPER_KEY_ROTATION_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(error: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(error)
        with GrpcVaultServiceError
  }

  @Explanation("Selected wrapper key id for rotation is already in use")
  @Resolution("Select a different key id and retry.")
  object WrapperKeyAlreadyInUseError
      extends ErrorCode(
        "WRAPPER_KEY_ALREADY_IN_USE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(wrapperKeyId: KmsKeyId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"Wrapper key id [$wrapperKeyId] selected for rotation is already being used."
        )
        with GrpcVaultServiceError
  }

  @Explanation("Selected wrapper key id for rotation does not match any existing KMS key")
  @Resolution("Specify a key id that matches an existing KMS key and retry.")
  object WrapperKeyNotExistError
      extends ErrorCode(
        "WRAPPER_KEY_NOT_EXIST_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(wrapperKeyId: KmsKeyId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"Wrapper key id [$wrapperKeyId] selected for rotation does not match an existing KMS key id."
        )
        with GrpcVaultServiceError
  }

  @Explanation(
    "Selected wrapper key id for rotation cannot be used " +
      "because key is disabled or set to be deleted"
  )
  @Resolution("Specify a key id from an active key and retry.")
  object WrapperKeyDisabledOrDeletedError
      extends ErrorCode(
        "WRAPPER_KEY_DISABLED_OR_DELETED_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(wrapperKeyId: KmsKeyId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          s"Wrapper key id [$wrapperKeyId] selected for rotation cannot be used " +
            s"because key is disabled or set to be deleted."
        )
        with GrpcVaultServiceError
  }

  @Explanation("Selected KMS key id is invalid")
  @Resolution("Specify a valid key id and retry.")
  object InvalidKmsKeyId
      extends ErrorCode(
        "INVALID_KMS_KEY_ID",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Failure(err: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(s"Invalid KMS key id: $err")
        with GrpcVaultServiceError
  }

  @Explanation("Node is not running an encrypted private store")
  @Resolution("Verify that an encrypted store and KMS config are set for this node.")
  object NoEncryptedPrivateKeyStoreError
      extends ErrorCode(
        "NO_ENCRYPTED_PRIVATE_KEY_STORE_ERROR",
        ErrorCategory.InternalUnsupportedOperation,
      ) {
    final case class Failure()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl("Node is not running an encrypted private store")
        with GrpcVaultServiceError
  }

  @Explanation("Internal error emitted upon failing to register a KMS key in Canton")
  @Resolution("Contact support")
  object RegisterKmsKeyInternalError
      extends ErrorCode(
        "REGISTER_KMS_KEY_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(error: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(error)
        with GrpcVaultServiceError
  }

}
