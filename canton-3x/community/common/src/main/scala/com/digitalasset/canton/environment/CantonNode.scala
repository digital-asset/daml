// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.health.admin.data.NodeStatus

import scala.concurrent.Future

/** A running instance of a canton node */
trait CantonNode extends AutoCloseable {
  def status: Future[NodeStatus.Status]
  def isActive: Boolean
}
