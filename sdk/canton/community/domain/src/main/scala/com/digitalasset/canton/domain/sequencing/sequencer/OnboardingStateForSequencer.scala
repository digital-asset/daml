// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.version.*

final case class OnboardingStateForSequencer(
    topologySnapshot: GenericStoredTopologyTransactionsX,
    staticDomainParameters: StaticDomainParameters,
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
    Some(staticDomainParameters.toProtoV30),
    Some(sequencerSnapshot.toProtoV30),
  )
}

object OnboardingStateForSequencer
    extends HasProtocolVersionedCompanion[OnboardingStateForSequencer] {
  override def name: String = "onboarding state for sequencer"

  def apply(
      topologySnapshot: GenericStoredTopologyTransactionsX,
      staticDomainParameters: StaticDomainParameters,
      sequencerSnapshot: SequencerSnapshot,
      protocolVersion: ProtocolVersion,
  ): OnboardingStateForSequencer =
    OnboardingStateForSequencer(topologySnapshot, staticDomainParameters, sequencerSnapshot)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(
      v30.OnboardingStateForSequencer
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      value: v30.OnboardingStateForSequencer
  ): ParsingResult[OnboardingStateForSequencer] = {
    for {
      topologySnapshot <- ProtoConverter.parseRequired(
        StoredTopologyTransactionsX.fromProtoV30,
        "topology_snapshot",
        value.topologySnapshot,
      )
      staticDomainParams <- ProtoConverter.parseRequired(
        StaticDomainParameters.fromProtoV30,
        "static_domain_parameters",
        value.staticDomainParameters,
      )
      sequencerSnapshot <- ProtoConverter.parseRequired(
        SequencerSnapshot.fromProtoV30,
        "sequencer_snapshot",
        value.sequencerSnapshot,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield OnboardingStateForSequencer(topologySnapshot, staticDomainParams, sequencerSnapshot)(
      rpv
    )
  }

}
