// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.admin.v0.VaultServiceGrpc.VaultServiceStub
import com.digitalasset.canton.crypto.{PublicKeyWithName, v0 as cryptoproto, *}
import com.digitalasset.canton.util.{EitherUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object VaultAdminCommands {

  abstract class BaseVaultAdminCommand[Req, Res, Result]
      extends GrpcAdminCommand[Req, Res, Result] {
    override type Svc = VaultServiceStub
    override def createService(channel: ManagedChannel): VaultServiceStub =
      v0.VaultServiceGrpc.stub(channel)
  }

  abstract class ListKeys[R, T](
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends BaseVaultAdminCommand[v0.ListKeysRequest, R, Seq[T]] {

    override def createRequest(): Either[String, v0.ListKeysRequest] =
      Right(
        v0.ListKeysRequest(
          filterFingerprint = filterFingerprint,
          filterName = filterName,
          filterPurpose = filterPurpose.map(_.toProtoEnum).toSeq,
        )
      )
  }

  // list keys in my key vault
  final case class ListMyKeys(
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends ListKeys[v0.ListMyKeysResponse, PrivateKeyMetadata](
        filterFingerprint,
        filterName,
        filterPurpose,
      ) {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ListKeysRequest,
    ): Future[v0.ListMyKeysResponse] =
      service.listMyKeys(request)

    override def handleResponse(
        response: v0.ListMyKeysResponse
    ): Either[String, Seq[PrivateKeyMetadata]] =
      response.privateKeysMetadata.traverse(PrivateKeyMetadata.fromProtoV0).leftMap(_.toString)
  }

  // list public keys in key registry
  final case class ListPublicKeys(
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends ListKeys[v0.ListKeysResponse, PublicKeyWithName](
        filterFingerprint,
        filterName,
        filterPurpose,
      ) {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ListKeysRequest,
    ): Future[v0.ListKeysResponse] =
      service.listPublicKeys(request)

    override def handleResponse(
        response: v0.ListKeysResponse
    ): Either[String, Seq[PublicKeyWithName]] =
      response.publicKeys.traverse(PublicKeyWithName.fromProtoV0).leftMap(_.toString)
  }

  abstract class BaseImportPublicKey
      extends BaseVaultAdminCommand[
        v0.ImportPublicKeyRequest,
        v0.ImportPublicKeyResponse,
        Fingerprint,
      ] {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ImportPublicKeyRequest,
    ): Future[v0.ImportPublicKeyResponse] =
      service.importPublicKey(request)

    override def handleResponse(response: v0.ImportPublicKeyResponse): Either[String, Fingerprint] =
      Fingerprint.fromProtoPrimitive(response.fingerprint).leftMap(_.toString)
  }

  // upload a public key into the key registry
  final case class ImportPublicKey(publicKey: ByteString, name: Option[String])
      extends BaseImportPublicKey {

    override def createRequest(): Either[String, v0.ImportPublicKeyRequest] =
      Right(v0.ImportPublicKeyRequest(publicKey = publicKey, name = name.getOrElse("")))
  }

  final case class GenerateSigningKey(name: String, scheme: Option[SigningKeyScheme])
      extends BaseVaultAdminCommand[
        v0.GenerateSigningKeyRequest,
        v0.GenerateSigningKeyResponse,
        SigningPublicKey,
      ] {

    override def createRequest(): Either[String, v0.GenerateSigningKeyRequest] =
      Right(
        v0.GenerateSigningKeyRequest(
          name = name,
          keyScheme = scheme.fold[cryptoproto.SigningKeyScheme](
            cryptoproto.SigningKeyScheme.MissingSigningKeyScheme
          )(_.toProtoEnum),
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GenerateSigningKeyRequest,
    ): Future[v0.GenerateSigningKeyResponse] = {
      service.generateSigningKey(request)
    }

    override def handleResponse(
        response: v0.GenerateSigningKeyResponse
    ): Either[String, SigningPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => SigningPublicKey.fromProtoV0(k).leftMap(_.toString))

    // may take some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class GenerateEncryptionKey(name: String, scheme: Option[EncryptionKeyScheme])
      extends BaseVaultAdminCommand[
        v0.GenerateEncryptionKeyRequest,
        v0.GenerateEncryptionKeyResponse,
        EncryptionPublicKey,
      ] {

    override def createRequest(): Either[String, v0.GenerateEncryptionKeyRequest] =
      Right(
        v0.GenerateEncryptionKeyRequest(
          name = name,
          keyScheme = scheme.fold[cryptoproto.EncryptionKeyScheme](
            cryptoproto.EncryptionKeyScheme.MissingEncryptionKeyScheme
          )(_.toProtoEnum),
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GenerateEncryptionKeyRequest,
    ): Future[v0.GenerateEncryptionKeyResponse] = {
      service.generateEncryptionKey(request)
    }

    override def handleResponse(
        response: v0.GenerateEncryptionKeyResponse
    ): Either[String, EncryptionPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => EncryptionPublicKey.fromProtoV0(k).leftMap(_.toString))

    // may time some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class RegisterKmsSigningKey(kmsKeyId: String, name: String)
      extends BaseVaultAdminCommand[
        v0.RegisterKmsSigningKeyRequest,
        v0.RegisterKmsSigningKeyResponse,
        SigningPublicKey,
      ] {

    override def createRequest(): Either[String, v0.RegisterKmsSigningKeyRequest] =
      Right(
        v0.RegisterKmsSigningKeyRequest(
          kmsKeyId = kmsKeyId,
          name = name,
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.RegisterKmsSigningKeyRequest,
    ): Future[v0.RegisterKmsSigningKeyResponse] = {
      service.registerKmsSigningKey(request)
    }

    override def handleResponse(
        response: v0.RegisterKmsSigningKeyResponse
    ): Either[String, SigningPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => SigningPublicKey.fromProtoV0(k).leftMap(_.toString))

  }

  final case class RegisterKmsEncryptionKey(kmsKeyId: String, name: String)
      extends BaseVaultAdminCommand[
        v0.RegisterKmsEncryptionKeyRequest,
        v0.RegisterKmsEncryptionKeyResponse,
        EncryptionPublicKey,
      ] {

    override def createRequest(): Either[String, v0.RegisterKmsEncryptionKeyRequest] =
      Right(
        v0.RegisterKmsEncryptionKeyRequest(
          kmsKeyId = kmsKeyId,
          name = name,
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.RegisterKmsEncryptionKeyRequest,
    ): Future[v0.RegisterKmsEncryptionKeyResponse] = {
      service.registerKmsEncryptionKey(request)
    }

    override def handleResponse(
        response: v0.RegisterKmsEncryptionKeyResponse
    ): Either[String, EncryptionPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => EncryptionPublicKey.fromProtoV0(k).leftMap(_.toString))

  }

  final case class RotateWrapperKey(newWrapperKeyId: String)
      extends BaseVaultAdminCommand[
        v0.RotateWrapperKeyRequest,
        Empty,
        Unit,
      ] {

    override def createRequest(): Either[String, v0.RotateWrapperKeyRequest] =
      Right(
        v0.RotateWrapperKeyRequest(
          newWrapperKeyId = newWrapperKeyId
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.RotateWrapperKeyRequest,
    ): Future[Empty] = {
      service.rotateWrapperKey(request)
    }

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

  }

  final case class GetWrapperKeyId()
      extends BaseVaultAdminCommand[
        v0.GetWrapperKeyIdRequest,
        v0.GetWrapperKeyIdResponse,
        String,
      ] {

    override def createRequest(): Either[String, v0.GetWrapperKeyIdRequest] =
      Right(
        v0.GetWrapperKeyIdRequest()
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GetWrapperKeyIdRequest,
    ): Future[v0.GetWrapperKeyIdResponse] = {
      service.getWrapperKeyId(request)
    }

    override def handleResponse(
        response: v0.GetWrapperKeyIdResponse
    ): Either[String, String] =
      Right(response.wrapperKeyId)

  }

  final case class ImportKeyPair(keyPair: ByteString, name: Option[String])
      extends BaseVaultAdminCommand[
        v0.ImportKeyPairRequest,
        v0.ImportKeyPairResponse,
        Unit,
      ] {

    override def createRequest(): Either[String, v0.ImportKeyPairRequest] =
      Right(v0.ImportKeyPairRequest(keyPair = keyPair, name = OptionUtil.noneAsEmptyString(name)))

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ImportKeyPairRequest,
    ): Future[v0.ImportKeyPairResponse] =
      service.importKeyPair(request)

    override def handleResponse(response: v0.ImportKeyPairResponse): Either[String, Unit] =
      EitherUtil.unit
  }

  final case class ExportKeyPair(fingerprint: Fingerprint, protocolVersion: ProtocolVersion)
      extends BaseVaultAdminCommand[
        v0.ExportKeyPairRequest,
        v0.ExportKeyPairResponse,
        ByteString,
      ] {

    override def createRequest(): Either[String, v0.ExportKeyPairRequest] = {
      Right(
        v0.ExportKeyPairRequest(
          fingerprint = fingerprint.toProtoPrimitive,
          protocolVersion = protocolVersion.toProtoPrimitive,
        )
      )
    }

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ExportKeyPairRequest,
    ): Future[v0.ExportKeyPairResponse] =
      service.exportKeyPair(request)

    override def handleResponse(response: v0.ExportKeyPairResponse): Either[String, ByteString] =
      Right(response.keyPair)
  }

  final case class DeleteKeyPair(fingerprint: Fingerprint)
      extends BaseVaultAdminCommand[
        v0.DeleteKeyPairRequest,
        v0.DeleteKeyPairResponse,
        Unit,
      ] {

    override def createRequest(): Either[String, v0.DeleteKeyPairRequest] = {
      Right(v0.DeleteKeyPairRequest(fingerprint = fingerprint.toProtoPrimitive))
    }

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.DeleteKeyPairRequest,
    ): Future[v0.DeleteKeyPairResponse] =
      service.deleteKeyPair(request)

    override def handleResponse(response: v0.DeleteKeyPairResponse): Either[String, Unit] =
      EitherUtil.unit
  }
}
