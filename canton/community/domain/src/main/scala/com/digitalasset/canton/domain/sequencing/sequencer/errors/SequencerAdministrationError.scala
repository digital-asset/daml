// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.topology.DomainMember

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
    final case class Error(sequencerMember: DomainMember)
        extends BaseCantonError.Impl(
          cause = s"Sequencer ${sequencerMember} cannot disable its local sequencer subscription"
        )
        with SequencerAdministrationError
  }
}
