// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.traffic.MemberTrafficStatus

final case class SequencerTrafficStatus(members: Seq[MemberTrafficStatus])
