// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.util.UUID

sealed trait LedgerIdMode extends Product with Serializable {
  def ledgerId(): String // with empty arguments since this might not be fixed (see [[Random]])
}

object LedgerIdMode {

  final case class Predefined(_ledgerId: String) extends LedgerIdMode {
    def ledgerId(): String = _ledgerId
  }

  final case object Random extends LedgerIdMode {
    def ledgerId(): String = LedgerIdGenerator.generateRandomId()
  }

}

object LedgerIdGenerator {
  def generateRandomId(): String = s"sandbox-${UUID.randomUUID().toString}"
}
