// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.admin.api.client.data.{
  MediatorStatus,
  ParticipantStatus,
  SequencerStatus,
}
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

private[health] trait StatusIntegrationTestUtil extends Matchers {
  protected def assertP1UnconnectedStatus(
      status: ParticipantStatus,
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    status.connectedSynchronizers shouldBe Map()
    status.uptime.toNanos should be > 0L
    status.id.identifier.str shouldBe "participant1"

    assertParticipantVersioningInfo(status, testedProtocolVersion)
  }

  protected def assertSequencerUnconnectedStatus(
      status: SequencerStatus,
      connectedMediators: List[MediatorId],
      synchronizerAlias: SynchronizerAlias,
      testedProtocolVersion: ProtocolVersion,
  ): Unit = {
    status.connectedParticipants shouldBe List()
    status.connectedMediators shouldBe connectedMediators

    status.uptime.toNanos should be > 0L
    status.uid.identifier.str shouldBe synchronizerAlias.unwrap

    assertSequencerVersioningInfo(status, testedProtocolVersion)
  }

  protected def assertNodesAreConnected(
      sequencerNodeStatus: SequencerStatus,
      participantStatus: ParticipantStatus,
      connectedMediators: List[MediatorId],
      testedProtocolVersion: ProtocolVersion,
  ): Unit = {

    sequencerNodeStatus.connectedParticipants should contain(participantStatus.id)
    sequencerNodeStatus.connectedMediators shouldBe connectedMediators

    participantStatus.connectedSynchronizers should contain(
      sequencerNodeStatus.synchronizerId -> SubmissionReady(true)
    )

    assertParticipantVersioningInfo(participantStatus, testedProtocolVersion)
    assertSequencerVersioningInfo(sequencerNodeStatus, testedProtocolVersion)
  }

  protected def assertParticipantVersioningInfo(
      status: ParticipantStatus,
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    status.version shouldBe ReleaseVersion.current

    status.supportedProtocolVersions shouldBe ProtocolVersionCompatibility
      .supportedProtocols(
        testedProtocolVersion.isAlpha,
        testedProtocolVersion.isBeta,
        ReleaseVersion.current,
      )
  }

  protected def assertSequencerVersioningInfo(
      sts: SequencerStatus,
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    sts.version shouldBe ReleaseVersion.current
    sts.protocolVersion shouldBe testedProtocolVersion
  }

  protected def assertMediatorVersioningInfo(
      status: MediatorStatus,
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    status.version shouldBe ReleaseVersion.current
    status.protocolVersion shouldBe testedProtocolVersion
  }
}
