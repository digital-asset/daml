// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.admin.api.client.data.crypto.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveDurationSeconds}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DynamicDomainParameters.InvalidDynamicDomainParameters
import com.digitalasset.canton.protocol.{
  CatchUpConfig,
  DynamicDomainParameters as DynamicDomainParametersInternal,
  StaticDomainParameters as StaticDomainParametersInternal,
  v0 as protocolV0,
  v1 as protocolV1,
}
import com.digitalasset.canton.topology.admin.v0
import com.digitalasset.canton.topology.admin.v0.DomainParametersChangeAuthorization
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, crypto as DomainCrypto}
import com.google.common.annotations.VisibleForTesting
import io.scalaland.chimney.dsl.*

import scala.Ordering.Implicits.*
import scala.annotation.nowarn

/** Companion object [[com.digitalasset.canton.protocol.StaticDomainParameters]] indicates
  * when the different version were introduces.
  */
sealed trait StaticDomainParameters {
  def uniqueContractKeys: Boolean
  def requiredSigningKeySchemes: Set[SigningKeyScheme]
  def requiredEncryptionKeySchemes: Set[EncryptionKeyScheme]
  def requiredSymmetricKeySchemes: Set[SymmetricKeyScheme]
  def requiredHashAlgorithms: Set[HashAlgorithm]
  def requiredCryptoKeyFormats: Set[CryptoKeyFormat]
  def protocolVersion: ProtocolVersion

  private[canton] def toInternal: StaticDomainParametersInternal

  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toInternal.toByteString)

  protected def toInternal(
      maxRatePerParticipant: NonNegativeInt,
      reconciliationInterval: PositiveDurationSeconds,
      maxRequestSize: MaxRequestSize,
      catchUpParameters: Option[CatchUpConfig],
  ): StaticDomainParametersInternal =
    StaticDomainParametersInternal.create(
      maxRatePerParticipant = maxRatePerParticipant,
      maxRequestSize = maxRequestSize,
      uniqueContractKeys = uniqueContractKeys,
      requiredSigningKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredSigningKeySchemes.map(_.transformInto[DomainCrypto.SigningKeyScheme])
      ),
      requiredEncryptionKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredEncryptionKeySchemes.map(_.transformInto[DomainCrypto.EncryptionKeyScheme])
      ),
      requiredSymmetricKeySchemes = NonEmptyUtil.fromUnsafe(
        requiredSymmetricKeySchemes.map(_.transformInto[DomainCrypto.SymmetricKeyScheme])
      ),
      requiredHashAlgorithms = NonEmptyUtil.fromUnsafe(
        requiredHashAlgorithms.map(_.transformInto[DomainCrypto.HashAlgorithm])
      ),
      requiredCryptoKeyFormats = NonEmptyUtil.fromUnsafe(
        requiredCryptoKeyFormats.map(_.transformInto[DomainCrypto.CryptoKeyFormat])
      ),
      protocolVersion = protocolVersion,
      reconciliationInterval = reconciliationInterval.toInternal,
      catchUpParameters = catchUpParameters,
    )
}

sealed abstract case class StaticDomainParametersV0(
    reconciliationInterval: PositiveDurationSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxInboundMessageSize: NonNegativeInt,
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: Set[SigningKeyScheme],
    requiredEncryptionKeySchemes: Set[EncryptionKeyScheme],
    requiredSymmetricKeySchemes: Set[SymmetricKeyScheme],
    requiredHashAlgorithms: Set[HashAlgorithm],
    requiredCryptoKeyFormats: Set[CryptoKeyFormat],
    protocolVersion: ProtocolVersion,
) extends StaticDomainParameters {
  override private[canton] def toInternal: StaticDomainParametersInternal =
    toInternal(
      maxRatePerParticipant,
      reconciliationInterval,
      MaxRequestSize(maxInboundMessageSize),
      None,
    )
}

sealed abstract case class StaticDomainParametersV1(
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: Set[SigningKeyScheme],
    requiredEncryptionKeySchemes: Set[EncryptionKeyScheme],
    requiredSymmetricKeySchemes: Set[SymmetricKeyScheme],
    requiredHashAlgorithms: Set[HashAlgorithm],
    requiredCryptoKeyFormats: Set[CryptoKeyFormat],
    protocolVersion: ProtocolVersion,
) extends StaticDomainParameters {
  override private[canton] def toInternal: StaticDomainParametersInternal = toInternal(
    StaticDomainParametersInternal.defaultMaxRatePerParticipant,
    StaticDomainParametersInternal.defaultReconciliationInterval.toConfig,
    StaticDomainParametersInternal.defaultMaxRequestSize,
    None,
  )
}

sealed abstract case class StaticDomainParametersV2(
    uniqueContractKeys: Boolean,
    requiredSigningKeySchemes: Set[SigningKeyScheme],
    requiredEncryptionKeySchemes: Set[EncryptionKeyScheme],
    requiredSymmetricKeySchemes: Set[SymmetricKeyScheme],
    requiredHashAlgorithms: Set[HashAlgorithm],
    requiredCryptoKeyFormats: Set[CryptoKeyFormat],
    protocolVersion: ProtocolVersion,
    catchUpParameters: Option[CatchUpConfig],
) extends StaticDomainParameters {
  override private[canton] def toInternal: StaticDomainParametersInternal = toInternal(
    StaticDomainParametersInternal.defaultMaxRatePerParticipant,
    StaticDomainParametersInternal.defaultReconciliationInterval.toConfig,
    StaticDomainParametersInternal.defaultMaxRequestSize,
    catchUpParameters,
  )
}

object StaticDomainParameters {

  @nowarn("msg=deprecated")
  def apply(
      domain: StaticDomainParametersInternal
  ): Either[ProtoDeserializationError.VersionError, StaticDomainParameters] = {
    val protoVersion = domain.protoVersion.v

    if (protoVersion == 0)
      Right(
        new StaticDomainParametersV0(
          reconciliationInterval = domain.reconciliationInterval.toConfig,
          maxRatePerParticipant = domain.maxRatePerParticipant,
          maxInboundMessageSize = domain.maxRequestSize.value,
          uniqueContractKeys = domain.uniqueContractKeys,
          requiredSigningKeySchemes =
            domain.requiredSigningKeySchemes.forgetNE.map(_.transformInto[SigningKeyScheme]),
          requiredEncryptionKeySchemes =
            domain.requiredEncryptionKeySchemes.forgetNE.map(_.transformInto[EncryptionKeyScheme]),
          requiredSymmetricKeySchemes =
            domain.requiredSymmetricKeySchemes.forgetNE.map(_.transformInto[SymmetricKeyScheme]),
          requiredHashAlgorithms =
            domain.requiredHashAlgorithms.forgetNE.map(_.transformInto[HashAlgorithm]),
          requiredCryptoKeyFormats =
            domain.requiredCryptoKeyFormats.forgetNE.map(_.transformInto[CryptoKeyFormat]),
          protocolVersion = domain.protocolVersion,
        ) {}
      )
    else if (protoVersion == 1)
      Right(
        new StaticDomainParametersV1(
          uniqueContractKeys = domain.uniqueContractKeys,
          requiredSigningKeySchemes =
            domain.requiredSigningKeySchemes.forgetNE.map(_.transformInto[SigningKeyScheme]),
          requiredEncryptionKeySchemes =
            domain.requiredEncryptionKeySchemes.forgetNE.map(_.transformInto[EncryptionKeyScheme]),
          requiredSymmetricKeySchemes =
            domain.requiredSymmetricKeySchemes.forgetNE.map(_.transformInto[SymmetricKeyScheme]),
          requiredHashAlgorithms =
            domain.requiredHashAlgorithms.forgetNE.map(_.transformInto[HashAlgorithm]),
          requiredCryptoKeyFormats =
            domain.requiredCryptoKeyFormats.forgetNE.map(_.transformInto[CryptoKeyFormat]),
          protocolVersion = domain.protocolVersion,
        ) {}
      )
    else if (protoVersion == 2)
      Right(
        new StaticDomainParametersV2(
          uniqueContractKeys = domain.uniqueContractKeys,
          requiredSigningKeySchemes =
            domain.requiredSigningKeySchemes.forgetNE.map(_.transformInto[SigningKeyScheme]),
          requiredEncryptionKeySchemes =
            domain.requiredEncryptionKeySchemes.forgetNE.map(_.transformInto[EncryptionKeyScheme]),
          requiredSymmetricKeySchemes =
            domain.requiredSymmetricKeySchemes.forgetNE.map(_.transformInto[SymmetricKeyScheme]),
          requiredHashAlgorithms =
            domain.requiredHashAlgorithms.forgetNE.map(_.transformInto[HashAlgorithm]),
          requiredCryptoKeyFormats =
            domain.requiredCryptoKeyFormats.forgetNE.map(_.transformInto[CryptoKeyFormat]),
          protocolVersion = domain.protocolVersion,
          catchUpParameters = domain.catchUpParameters,
        ) {}
      )
    else
      Left(ProtoDeserializationError.VersionError("StaticDomainParameters", protoVersion))
  }

  def tryReadFromFile(inputFile: String): StaticDomainParameters = {
    val staticDomainParametersInternal = StaticDomainParametersInternal
      .readFromFileUnsafe(inputFile)
      .valueOr(err =>
        throw new IllegalArgumentException(
          s"Reading static domain parameters from file $inputFile failed: $err"
        )
      )

    StaticDomainParameters(staticDomainParametersInternal).valueOr(err =>
      throw new RuntimeException(s"Unable to convert static domain parameters: $err")
    )
  }
}

/** Companion object [[com.digitalasset.canton.protocol.DynamicDomainParameters]] indicates
  * when the different version were introduces.
  */
sealed trait DynamicDomainParameters {

  def participantResponseTimeout: NonNegativeFiniteDuration
  def mediatorReactionTimeout: NonNegativeFiniteDuration
  def transferExclusivityTimeout: NonNegativeFiniteDuration
  def topologyChangeDelay: NonNegativeFiniteDuration
  def ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration

  /** max request size is only available in V1 */
  def maxRequestSizeV1: Option[NonNegativeInt] = None

  /** Checks if it is safe to change the `ledgerTimeRecordTimeTolerance` to the given new value.
    */
  private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean

  /** Convert the parameters to the Protobuf representation
    * @param protocolVersion Protocol version used on the domain; used to ensure that we don't send parameters
    *                        that are not dynamic yet (e.g., reconciliation interval in a domain running an old
    *                        protocol version).
    */
  def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, v0.DomainParametersChangeAuthorization.Parameters]

  protected def protoVersion(protocolVersion: ProtocolVersion): Int =
    DynamicDomainParametersInternal.protoVersionFor(protocolVersion).v

  def update(
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters
}

final case class DynamicDomainParametersV0(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
) extends DynamicDomainParameters {

  override private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean = true // always safe, because we don't have mediatorDeduplicationTimeout

  override def update(
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters = this.copy(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
  )

  override def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, DomainParametersChangeAuthorization.Parameters] =
    if (protoVersion(protocolVersion) == 0)
      Right(
        protocolV0.DynamicDomainParameters(
          participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
          mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
          transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
          topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
          ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
        )
      ).map(DomainParametersChangeAuthorization.Parameters.ParametersV0)
    else
      Left(
        s"Cannot convert DynamicDomainParametersV0 to Protobuf when domain protocol version is $protocolVersion"
      )
}

final case class DynamicDomainParametersV1(
    participantResponseTimeout: NonNegativeFiniteDuration,
    mediatorReactionTimeout: NonNegativeFiniteDuration,
    transferExclusivityTimeout: NonNegativeFiniteDuration,
    topologyChangeDelay: NonNegativeFiniteDuration,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration,
    mediatorDeduplicationTimeout: NonNegativeFiniteDuration,
    reconciliationInterval: PositiveDurationSeconds,
    maxRatePerParticipant: NonNegativeInt,
    maxRequestSize: NonNegativeInt,
) extends DynamicDomainParameters {

  override def maxRequestSizeV1: Option[NonNegativeInt] = Some(maxRequestSize)

  if (ledgerTimeRecordTimeTolerance * 2 > mediatorDeduplicationTimeout)
    throw new InvalidDynamicDomainParameters(
      s"The ledgerTimeRecordTimeTolerance ($ledgerTimeRecordTimeTolerance) must be at most half of the " +
        s"mediatorDeduplicationTimeout ($mediatorDeduplicationTimeout)."
    )

  // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
  override private[canton] def compatibleWithNewLedgerTimeRecordTimeTolerance(
      newLedgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
  ): Boolean = {
    // If false, a new request may receive the same ledger time as a previous request and the previous
    // request may be evicted too early from the mediator's deduplication store.
    // Thus, an attacker may assign the same UUID to both requests.
    // See i9028 for a detailed design. (This is the second clause of item 2 of Lemma 2).
    ledgerTimeRecordTimeTolerance + newLedgerTimeRecordTimeTolerance <= mediatorDeduplicationTimeout
  }

  override def update(
      participantResponseTimeout: NonNegativeFiniteDuration = participantResponseTimeout,
      mediatorReactionTimeout: NonNegativeFiniteDuration = mediatorReactionTimeout,
      transferExclusivityTimeout: NonNegativeFiniteDuration = transferExclusivityTimeout,
      topologyChangeDelay: NonNegativeFiniteDuration = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration = ledgerTimeRecordTimeTolerance,
  ): DynamicDomainParameters = this.copy(
    participantResponseTimeout = participantResponseTimeout,
    mediatorReactionTimeout = mediatorReactionTimeout,
    transferExclusivityTimeout = transferExclusivityTimeout,
    topologyChangeDelay = topologyChangeDelay,
    ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
  )

  override def toProto(
      protocolVersion: ProtocolVersion
  ): Either[String, DomainParametersChangeAuthorization.Parameters] = {
    val protoV = protoVersion(protocolVersion)
    // TODO(#15152) Adapt when support for pv < 6 is dropped
    // TODO(#15153) Adapt when support for pv=6 is dropped
    if (protoV == 1 || protoV == 2)
      Right(
        protocolV1.DynamicDomainParameters(
          participantResponseTimeout = Some(participantResponseTimeout.toProtoPrimitive),
          mediatorReactionTimeout = Some(mediatorReactionTimeout.toProtoPrimitive),
          transferExclusivityTimeout = Some(transferExclusivityTimeout.toProtoPrimitive),
          topologyChangeDelay = Some(topologyChangeDelay.toProtoPrimitive),
          ledgerTimeRecordTimeTolerance = Some(ledgerTimeRecordTimeTolerance.toProtoPrimitive),
          mediatorDeduplicationTimeout = Some(mediatorDeduplicationTimeout.toProtoPrimitive),
          reconciliationInterval = Some(reconciliationInterval.toProtoPrimitive),
          maxRatePerParticipant = maxRatePerParticipant.unwrap,
          maxRequestSize = maxRequestSize.unwrap,
        )
      ).map(DomainParametersChangeAuthorization.Parameters.ParametersV1)
    else
      Left(
        s"Cannot convert DynamicDomainParametersV1 to Protobuf when domain protocol version is $protocolVersion"
      )
  }
}

object DynamicDomainParameters {

  /** Default dynamic domain parameters for non-static clocks */
  @VisibleForTesting
  def defaultValues(protocolVersion: ProtocolVersion): DynamicDomainParameters =
    DynamicDomainParameters(
      DynamicDomainParametersInternal.defaultValues(protocolVersion)
    ).fold(err => throw new RuntimeException(err.message), identity)

  def apply(
      domain: DynamicDomainParametersInternal
  ): Either[ProtoDeserializationError.VersionError, DynamicDomainParameters] = {
    val protoVersion = domain.protoVersion.v

    if (protoVersion == 0)
      Right(domain.transformInto[DynamicDomainParametersV0])
    // TODO(#15152) Adapt when support for pv < 6 is dropped
    // TODO(#15153) Adapt when support for pv=6 is dropped
    else if (protoVersion == 1 || protoVersion == 2)
      Right(domain.transformInto[DynamicDomainParametersV1])
    else
      Left(ProtoDeserializationError.VersionError("DynamicDomainParameters", protoVersion))
  }
}
