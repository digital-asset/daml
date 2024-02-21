// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.order.*
import com.digitalasset.canton.data.CantonTimestamp

final case class ParticipantAttributes(
    permission: ParticipantPermission,
    loginAfter: Option[CantonTimestamp] = None,
) {

  def merge(elem: ParticipantAttributes): ParticipantAttributes =
    ParticipantAttributes(
      permission = ParticipantPermission.lowerOf(permission, elem.permission),
      loginAfter = loginAfter.max(elem.loginAfter),
    )

}
