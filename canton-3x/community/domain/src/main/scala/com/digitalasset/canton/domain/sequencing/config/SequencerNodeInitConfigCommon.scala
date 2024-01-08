// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.InitConfigBase

/** Common init config with db-locked-connection shared among sequencers supporting auto-init or not
  */
abstract class SequencerNodeInitConfigCommon() extends config.InitConfigBase

final case class CommunitySequencerNodeInitConfig() extends SequencerNodeInitConfigCommon {
  override def identity: Option[InitConfigBase.Identity] = None
}
