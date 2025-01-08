// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.health.admin.data.NodeStatus

/** A running instance of a canton node */
trait CantonNode extends AutoCloseable {
  type Status <: NodeStatus.Status

  def status: Status
  def isActive: Boolean

  def adminToken: CantonAdminToken

}
