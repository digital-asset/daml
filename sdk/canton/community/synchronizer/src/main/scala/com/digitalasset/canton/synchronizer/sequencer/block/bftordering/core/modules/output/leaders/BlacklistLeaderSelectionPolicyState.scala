// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistLeaderSelectionPolicy.Blacklist
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.LeaderSelectionPolicy.rotateLeaders
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

import scala.collection.immutable.SortedSet

final case class BlacklistLeaderSelectionPolicyState(
    epochNumber: EpochNumber,
    startBlock: BlockNumber,
    blacklist: Blacklist,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      BlacklistLeaderSelectionPolicyState.type
    ]
) extends HasProtocolVersionedWrapper[BlacklistLeaderSelectionPolicyState] {

  def toProto30: v30.BlacklistLeaderSelectionPolicyState =
    v30.BlacklistLeaderSelectionPolicyState.of(
      epochNumber,
      startBlock,
      blacklist.view.mapValues(_.toProto30).toMap,
    )

  def selectLeaders(
      topology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
  ): SortedSet[BftNodeId] = {
    val allBlacklistedNodes: Seq[BftNodeId] = blacklist
      .collect { case (node, BlacklistStatus.Blacklisted(_, epochLeftUntilNewTrial)) =>
        node -> epochLeftUntilNewTrial
      }
      .toSeq
      .sortBy(x => x._2 -> x._1)
      .reverse
      .map(_._1)

    val blacklistedNodes: Set[BftNodeId] = allBlacklistedNodes
      .take(config.howManyCanWeBlacklist.howManyCanWeBlacklist(topology))
      .toSet

    SortedSet.from(topology.nodes.removedAll(blacklistedNodes))
  }

  def computeLeaders(
      orderingTopology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
  ): Seq[BftNodeId] =
    rotateLeaders(selectLeaders(orderingTopology, config), epochNumber)

  def computeBlockToLeader(
      orderingTopology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
      epochLength: EpochLength,
  ): Map[BlockNumber, BftNodeId] =
    Epoch.blockToLeadersFromEpochInfo(
      computeLeaders(orderingTopology, config),
      startBlock,
      epochLength,
    )

  def update(
      topology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
      epochLength: EpochLength,
      blockToLeader: Map[BlockNumber, BftNodeId],
      nodesToPunish: Set[BftNodeId],
  ): BlacklistLeaderSelectionPolicyState = {
    val newBlacklist = updateBlacklist(topology, config, blockToLeader, nodesToPunish)
    BlacklistLeaderSelectionPolicyState(
      EpochNumber(epochNumber + 1),
      BlockNumber(startBlock + epochLength),
      newBlacklist,
    )(representativeProtocolVersion)
  }

  private def updateBlacklist(
      topology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
      blockToLeader: Map[BlockNumber, BftNodeId],
      nodesToPunish: Set[BftNodeId],
  ): Blacklist = {
    val nodesThatParticipated = blockToLeader.values.toSet
    val currentBlacklistWithoutRemovedNodes = blacklist.filter { case (node, _) =>
      topology.contains(node)
    }

    topology.nodes.foldLeft(currentBlacklistWithoutRemovedNodes) { case (blacklist, node) =>
      blacklist.updatedWith(node)(
        BlacklistStatus.transformToWorkOnBlacklistStatus { status =>
          val howEpochWent: BlacklistStatus.HowEpochWent = if (nodesToPunish.contains(node)) {
            BlacklistStatus.HowEpochWent.ShouldBePunished
          } else {
            if (nodesThatParticipated.contains(node)) {
              BlacklistStatus.HowEpochWent.Succeeded
            } else {
              BlacklistStatus.HowEpochWent.DidNotParticipate
            }
          }
          status.update(howEpochWent, config.howLongToBlackList)
        }
      )
    }
  }

  override protected val companionObj: BlacklistLeaderSelectionPolicyState.type =
    BlacklistLeaderSelectionPolicyState
}

object BlacklistLeaderSelectionPolicyState
    extends VersioningCompanion[BlacklistLeaderSelectionPolicyState] {
  def create(
      epochNumber: EpochNumber,
      startBlockNumber: BlockNumber,
      blacklist: Blacklist,
  )(protocolVersion: ProtocolVersion): BlacklistLeaderSelectionPolicyState =
    BlacklistLeaderSelectionPolicyState(epochNumber, startBlockNumber, blacklist)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "BlacklistLeaderSelectionPolicyState"

  private def fromProto30(
      proto: v30.BlacklistLeaderSelectionPolicyState
  ): ParsingResult[BlacklistLeaderSelectionPolicyState] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    epochNumber = EpochNumber(proto.epochNumber)
    startBlockNumber = BlockNumber(proto.startBlockNumber)
  } yield BlacklistLeaderSelectionPolicyState(
    epochNumber,
    startBlockNumber,
    proto.blacklist.flatMap { case (node, protoStatus) =>
      val status: Option[BlacklistStatus.BlacklistStatusMark] = protoStatus.status match {
        case Status.Empty => None
        case v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.OnTrial(value) =>
          Some(BlacklistStatus.OnTrial(value.failedAttemptsBeforeTrial))
        case v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.Blacklisted(value) =>
          Some(
            BlacklistStatus.Blacklisted(
              failedAttemptsBefore = value.failedAttemptsBefore,
              epochsLeftUntilNewTrial = value.epochsLeftUntilNewTrial,
            )
          )
      }
      status.map(BftNodeId(node) -> _)
    },
  )(rpv)

  override val versioningTable: VersioningTable =
    VersioningTable(
      ProtoVersion(30) ->
        VersionedProtoCodec(
          SupportedVersions.CantonProtocol
        )(v30.BlacklistLeaderSelectionPolicyState)(
          supportedProtoVersion(_)(fromProto30),
          _.toProto30,
        )
    )

  def FirstBlacklistLeaderSelectionPolicyState(
      protocolVersion: ProtocolVersion
  ): BlacklistLeaderSelectionPolicyState =
    create(
      EpochNumber(0L),
      BlockNumber.First,
      Map.empty,
    )(
      protocolVersion
    )
}
