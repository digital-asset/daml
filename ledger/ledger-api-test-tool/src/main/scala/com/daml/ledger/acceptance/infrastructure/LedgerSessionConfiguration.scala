// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import com.digitalasset.platform.sandbox.config.SandboxConfig

private[acceptance] sealed trait LedgerSessionConfiguration

private[acceptance] object LedgerSessionConfiguration {

  final case class Managed(config: SandboxConfig) extends LedgerSessionConfiguration

}
