// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{CantonConfigValidator, UniformCantonConfigValidation}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigWriter}

/** Defines which counter-participants cannot block pruning because of the states of their
  * commitments. The modes increasingly relax the pruning conditions. These modes override the
  * default, which is that all counter-participants block pruning when there isn't a timestamp where
  * all counter-participants sent matching commitments, and non-matching participants are not in a
  * no-wait configuration.
  */
sealed trait SafeToPruneCommitmentState
    extends Product
    with Serializable
    with UniformCantonConfigValidation {
  def toInt: Int

  def toProtoV30: v30.SafeToPruneCommitmentState =
    this match {
      case SafeToPruneCommitmentState.Match =>
        v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_MATCH
      case SafeToPruneCommitmentState.MatchOrMismatch =>
        v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_MATCH_MISMATCH
      case SafeToPruneCommitmentState.All =>
        v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_ALL
    }
}

object SafeToPruneCommitmentState {

  sealed trait SafeToPruneCommitmentStateRequiresChecks extends SafeToPruneCommitmentState

  case object Match extends SafeToPruneCommitmentStateRequiresChecks {
    override val toInt = 1
  }

  case object MatchOrMismatch extends SafeToPruneCommitmentStateRequiresChecks {
    override val toInt = 2
  }

  case object All extends SafeToPruneCommitmentState {
    override val toInt = 3
  }

  def fromProtoV30(
      state: v30.SafeToPruneCommitmentState
  ): ParsingResult[SafeToPruneCommitmentState] =
    state match {
      case v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_MATCH =>
        Right(SafeToPruneCommitmentState.Match)
      case v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_MATCH_MISMATCH =>
        Right(SafeToPruneCommitmentState.MatchOrMismatch)
      case v30.SafeToPruneCommitmentState.SAFE_TO_PRUNE_COMMITMENT_STATE_ALL =>
        Right(SafeToPruneCommitmentState.All)
      case _ =>
        Left(UnrecognizedEnum.apply("safe_to_prune_commitment_state", state.value))
    }

  implicit val safeToPruneCommitmentStateCantonConfigValidator
      : CantonConfigValidator[SafeToPruneCommitmentState] =
    CantonConfigValidatorDerivation[SafeToPruneCommitmentState]

  // PureConfig reader/writer (manual, robust)
  implicit val reader: ConfigReader[SafeToPruneCommitmentState] =
    ConfigReader[String].emap {
      case "match" => Right(Match)
      case "match-or-mismatch" => Right(MatchOrMismatch)
      case "all" => Right(All)
      case other =>
        Left(
          CannotConvert(
            other,
            "SafeToPruneCommitmentState",
            "expected one of: match, match-or-mismatch, all",
          )
        )
    }

  implicit val writer: ConfigWriter[SafeToPruneCommitmentState] =
    ConfigWriter[String].contramap {
      case Match => "match"
      case MatchOrMismatch => "match-or-mismatch"
      case All => "all"
    }
}
