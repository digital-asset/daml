// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.File

sealed abstract class LedgerMode extends Product with Serializable
object LedgerMode {
  final case class LedgerAddress(host: String, port: Int) extends LedgerMode
  final case class ParticipantConfig(file: File) extends LedgerMode
  final case class IdeLedger() extends LedgerMode
}
