// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.version.*

final case class OnboardingStateForSequencer(
    topologySnapshot: GenericStoredTopologyTransactions,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    sequencerSnapshot: SequencerSnapshot,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      OnboardingStateForSequencer.type
    ]
) extends HasProtocolVersionedWrapper[OnboardingStateForSequencer] {

  override protected val companionObj: OnboardingStateForSequencer.type =
    OnboardingStateForSequencer

  private def toProtoV30: v30.OnboardingStateForSequencer = v30.OnboardingStateForSequencer(
    Some(topologySnapshot.toProtoV30),
    Some(staticSynchronizerParameters.toProtoV30),
    Some(sequencerSnapshot.toProtoV30),
  )
}

object OnboardingStateForSequencer
    extends HasProtocolVersionedCompanion[OnboardingStateForSequencer] {
  override def name: String = "onboarding state for sequencer"

  def apply(
      topologySnapshot: GenericStoredTopologyTransactions,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      sequencerSnapshot: SequencerSnapshot,
      protocolVersion: ProtocolVersion,
  ): OnboardingStateForSequencer =
    OnboardingStateForSequencer(topologySnapshot, staticSynchronizerParameters, sequencerSnapshot)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(
      v30.OnboardingStateForSequencer
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def fromProtoV30(
      value: v30.OnboardingStateForSequencer
  ): ParsingResult[OnboardingStateForSequencer] =
    for {
      topologySnapshot <- ProtoConverter.parseRequired(
        StoredTopologyTransactions.fromProtoV30,
        "topology_snapshot",
        value.topologySnapshot,
      )
      staticSynchronizerParams <- ProtoConverter.parseRequired(
        StaticSynchronizerParameters.fromProtoV30,
        "static_synchronizer_parameters",
        value.staticSynchronizerParameters,
      )
      sequencerSnapshot <- ProtoConverter.parseRequired(
        SequencerSnapshot.fromProtoV30,
        "sequencer_snapshot",
        value.sequencerSnapshot,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield OnboardingStateForSequencer(
      topologySnapshot,
      staticSynchronizerParams,
      sequencerSnapshot,
    )(
      rpv
    )

}
