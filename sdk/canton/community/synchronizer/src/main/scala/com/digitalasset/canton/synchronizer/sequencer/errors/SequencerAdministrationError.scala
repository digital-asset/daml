// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.errors

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.topology.Member

sealed trait SequencerAdministrationError extends BaseCantonError

object SequencerAdministrationError extends SequencerErrorGroup {

  @Explanation(
    """Sequencers cannot disable their local subscription as that would disable their ability to
      |adapt to topology changes."""
  )
  @Resolution(
    """Disabling sequencer subscriptions is typically done to facilitate sequencer pruning. If the
      |sequencer's local subscription prevents sequencer pruning, consider lowering the sequencer time tracker
      |`min_observation_duration`."""
  )
  object CannotDisableLocalSequencerMember
      extends ErrorCode(
        "CANNOT_DISABLE_LOCAL_SEQUENCER_MEMBER",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(sequencerMember: Member)
        extends BaseCantonError.Impl(
          cause = s"Sequencer $sequencerMember cannot disable its local sequencer subscription"
        )
        with SequencerAdministrationError
  }
}
