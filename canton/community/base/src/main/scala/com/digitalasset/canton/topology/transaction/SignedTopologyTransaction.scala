// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.PrettyInstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithOptionalValidationCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

/** A signed topology transaction
  *
  * Every topology transaction needs to be authorized by an appropriate key. This object represents such
  * an authorization, where there is a signature of a given key of the given topology transaction.
  *
  * Whether the key is eligible to authorize the topology transaction depends on the topology state
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SignedTopologyTransaction[+Op <: TopologyChangeOp] private (
    transaction: TopologyTransaction[Op],
    key: SigningPublicKey,
    signature: Signature,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SignedTopologyTransaction.type
    ],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[SignedTopologyTransaction[TopologyChangeOp]]
    with ProtocolVersionedMemoizedEvidence
    with Product
    with Serializable
    with PrettyPrinting {

  override protected def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: SignedTopologyTransaction.type =
    SignedTopologyTransaction

  private def toProtoV30: v30.SignedTopologyTransaction =
    v30.SignedTopologyTransaction(
      transaction = transaction.getCryptographicEvidence,
      key = Some(key.toProtoV30),
      signature = Some(signature.toProtoV30),
    )

  def verifySignature(pureApi: CryptoPureApi): Either[SignatureCheckError, Unit] = {
    val hash = transaction.hashToSign(pureApi)
    pureApi.verifySignature(hash, key, signature)
  }

  @VisibleForTesting
  def update[NewOp >: Op <: TopologyChangeOp](
      transaction: TopologyTransaction[NewOp] = transaction,
      key: SigningPublicKey = key,
      signature: Signature = signature,
  ): SignedTopologyTransaction[NewOp] =
    this.copy(transaction = transaction, key = key, signature = signature)(
      representativeProtocolVersion,
      None,
    )

  override def pretty: Pretty[SignedTopologyTransaction.this.type] =
    prettyOfClass(unnamedParam(_.transaction), param("key", _.key))

  def uniquePath: UniquePath = transaction.element.uniquePath

  def operation: Op = transaction.op

  def restrictedToDomain: Option[DomainId] = transaction.element.mapping.restrictedToDomain
}

object SignedTopologyTransaction
    extends HasMemoizedProtocolVersionedWithOptionalValidationCompanion[
      SignedTopologyTransaction[
        TopologyChangeOp
      ]
    ] {
  override val name: String = "SignedTopologyTransaction"

  type GenericSignedTopologyTransaction = SignedTopologyTransaction[TopologyChangeOp]

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.SignedTopologyTransaction)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  import com.digitalasset.canton.resource.DbStorage.Implicits.*

  def apply[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      key: SigningPublicKey,
      signature: Signature,
      rpv: RepresentativeProtocolVersion[SignedTopologyTransaction.type],
  ): SignedTopologyTransaction[Op] =
    SignedTopologyTransaction(transaction, key, signature)(rpv, None)

  /** Sign the given topology transaction. */
  def create[Op <: TopologyChangeOp](
      transaction: TopologyTransaction[Op],
      signingKey: SigningPublicKey,
      hashOps: HashOps,
      crypto: CryptoPrivateApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, SigningError, SignedTopologyTransaction[Op]] =
    for {
      signature <- crypto.sign(transaction.hashToSign(hashOps), signingKey.id)
      representativeProtocolVersion = supportedProtoVersions.protocolVersionRepresentativeFor(
        protocolVersion
      )
    } yield SignedTopologyTransaction(transaction, signingKey, signature)(
      representativeProtocolVersion,
      None,
    )

  def asVersion[Op <: TopologyChangeOp](
      signedTx: SignedTopologyTransaction[Op],
      protocolVersion: ProtocolVersion,
  )(
      crypto: Crypto
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[Future, String, SignedTopologyTransaction[Op]] = {
    val originTx = signedTx.transaction

    // Convert and resign the transaction if the topology transaction version does not match the expected version
    if (!originTx.hasEquivalentVersion(protocolVersion)) {
      val convertedTx = originTx.asVersion(protocolVersion)
      SignedTopologyTransaction
        .create(
          convertedTx,
          signedTx.key,
          crypto.pureCrypto,
          crypto.privateCrypto,
          protocolVersion,
        )
        .leftMap { err =>
          s"Failed to resign topology transaction $originTx (${originTx.representativeProtocolVersion}) for domain version $protocolVersion: $err"
        }
    } else
      EitherT.rightT(signedTx)
  }

  private def fromProtoV30(
      protocolVersionValidation: ProtocolVersionValidation,
      transactionP: v30.SignedTopologyTransaction,
  )(
      bytes: ByteString
  ): ParsingResult[SignedTopologyTransaction[TopologyChangeOp]] = {
    for {
      transaction <-
        TopologyTransaction.fromByteString(protocolVersionValidation)(
          transactionP.transaction
        )
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "key",
        transactionP.key,
      )
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30,
        "signature",
        transactionP.signature,
      )
      rpv <- supportedProtoVersions.protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SignedTopologyTransaction(transaction, publicKey, signature)(
      rpv,
      Some(bytes),
    )
  }

  def createGetResultDomainTopologyTransaction
      : GetResult[SignedTopologyTransaction[TopologyChangeOp]] =
    GetResult { r =>
      fromByteStringUnsafe(r.<<[ByteString]).valueOr(err =>
        throw new DbSerializationException(s"Failed to deserialize TopologyTransaction: $err")
      )
    }

  implicit def setParameterTopologyTransaction(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[SignedTopologyTransaction[TopologyChangeOp]] = {
    (d: SignedTopologyTransaction[TopologyChangeOp], pp: PositionedParameters) =>
      pp >> d.toByteArray
  }
}
