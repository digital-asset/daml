// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.LeaderSelectionPolicyConfig.HowLongToBlacklist
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistStatus.HowEpochWent

sealed trait BlacklistStatus {
  def update(howEpochWent: HowEpochWent, howLongToBlacklist: HowLongToBlacklist): BlacklistStatus
}

object BlacklistStatus {

  sealed trait HowEpochWent

  object HowEpochWent {
    case object Succeeded extends HowEpochWent
    case object ShouldBePunished extends HowEpochWent
    case object DidNotParticipate extends HowEpochWent
  }

  case object Clean extends BlacklistStatus {
    override def update(
        howEpochWent: HowEpochWent,
        howLongToBlacklist: HowLongToBlacklist,
    ): BlacklistStatus = howEpochWent match {
      case HowEpochWent.Succeeded => Clean
      case HowEpochWent.ShouldBePunished => howLongToBlacklist.compute(1)
      case HowEpochWent.DidNotParticipate => Clean
    }
  }

  sealed trait BlacklistStatusMark extends BlacklistStatus {
    def toProto30: v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus
  }

  final case class OnTrial(numberOfConsecutiveFailedAttempts: Long) extends BlacklistStatusMark {
    override def update(
        howEpochWent: HowEpochWent,
        howLongToBlacklist: HowLongToBlacklist,
    ): BlacklistStatus = howEpochWent match {
      case HowEpochWent.Succeeded => Clean
      case HowEpochWent.ShouldBePunished =>
        howLongToBlacklist.compute(numberOfConsecutiveFailedAttempts + 1)
      case HowEpochWent.DidNotParticipate => this
    }

    override def toProto30: v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus =
      v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.of(
        v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.OnTrial(
          v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.OnTrial
            .of(numberOfConsecutiveFailedAttempts)
        )
      )
  }

  final case class Blacklisted(failedAttemptsBefore: Long, epochsLeftUntilNewTrial: Long)
      extends BlacklistStatusMark {
    override def update(
        howEpochWent: HowEpochWent,
        howLongToBlacklist: HowLongToBlacklist,
    ): BlacklistStatus = howEpochWent match {
      // Even if the node is blacklisted it might have participated in the last epoch
      case HowEpochWent.Succeeded => Clean
      case HowEpochWent.ShouldBePunished =>
        howLongToBlacklist.compute(failedAttemptsBefore + 1)
      case HowEpochWent.DidNotParticipate =>
        if (epochsLeftUntilNewTrial <= 0) {
          OnTrial(failedAttemptsBefore)
        } else {
          Blacklisted(failedAttemptsBefore, epochsLeftUntilNewTrial - 1)
        }
    }

    override def toProto30: v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus =
      v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.of(
        v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.Blacklisted(
          v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus
            .Blacklisted(failedAttemptsBefore, epochsLeftUntilNewTrial)
        )
      )
  }

  private def toMark(status: BlacklistStatus): Option[BlacklistStatusMark] = status match {
    case BlacklistStatus.Clean => None
    case mark: BlacklistStatusMark => Some(mark)
  }

  private def fromMark(status: Option[BlacklistStatusMark]): BlacklistStatus = status match {
    case Some(value) => value
    case None => Clean
  }

  def transformToWorkOnBlacklistStatus(fun: BlacklistStatus => BlacklistStatus)(
      status: Option[BlacklistStatusMark]
  ): Option[BlacklistStatusMark] =
    toMark(fun(fromMark(status)))
}
