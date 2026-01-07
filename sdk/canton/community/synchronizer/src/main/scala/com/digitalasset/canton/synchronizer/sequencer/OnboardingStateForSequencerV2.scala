// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.version.*

final case class OnboardingStateForSequencerV2(
    topologySnapshot: Option[GenericStoredTopologyTransaction],
    staticSynchronizerParameters: Option[StaticSynchronizerParameters],
    sequencerSnapshot: Option[SequencerSnapshot],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      OnboardingStateForSequencerV2.type
    ]
) extends HasProtocolVersionedWrapper[OnboardingStateForSequencerV2] {

  override protected val companionObj: OnboardingStateForSequencerV2.type =
    OnboardingStateForSequencerV2

  private def toProtoV30: v30.OnboardingStateForSequencerV2 =
    v30.OnboardingStateForSequencerV2(
      topologySnapshot.map(_.toByteString(representativeProtocolVersion.representative)),
      staticSynchronizerParameters.map(_.toByteString),
      sequencerSnapshot.map(_.toProtoV30),
    )
}

object OnboardingStateForSequencerV2 extends VersioningCompanion[OnboardingStateForSequencerV2] {
  override def name: String = "onboarding state for sequencer"

  def apply(
      topologySnapshot: Option[GenericStoredTopologyTransaction],
      staticSynchronizerParameters: Option[StaticSynchronizerParameters],
      sequencerSnapshot: Option[SequencerSnapshot],
      protocolVersion: ProtocolVersion,
  ): OnboardingStateForSequencerV2 = OnboardingStateForSequencerV2(
    topologySnapshot,
    staticSynchronizerParameters,
    sequencerSnapshot,
  )(protocolVersionRepresentativeFor(protocolVersion))

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.OnboardingStateForSequencerV2
    )(supportedProtoVersion(_)(fromProtoV30), _.toProtoV30)
  )
  import cats.syntax.traverse.*
  private def fromProtoV30(
      value: v30.OnboardingStateForSequencerV2
  ): ParsingResult[OnboardingStateForSequencerV2] =
    for {
      topologySnapshot <-
        value.topologyTransaction.traverse(StoredTopologyTransaction.fromTrustedByteString)
      staticSynchronizerParams <-
        value.staticSynchronizerParameters.traverse(
          StaticSynchronizerParameters.fromTrustedByteString
        )

      sequencerSnapshot <-
        value.sequencerSnapshot.traverse(SequencerSnapshot.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield OnboardingStateForSequencerV2(
      topologySnapshot,
      staticSynchronizerParams,
      sequencerSnapshot,
    )(rpv)
}
