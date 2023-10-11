// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

trait DomainOutboxStatus {

  def queueSize: Int

}

object DomainOutboxStatus {
  def combined(outboxes: DomainOutboxCommon*) = new DomainOutboxStatus {
    override def queueSize: Int = outboxes.map(_.queueSize).sum
  }
}
