// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.File

sealed abstract class ParticipantMode extends Product with Serializable
object ParticipantMode {
  final case class RemoteParticipantHost(host: String, port: Int, adminPort: Option[Int] = None)
      extends ParticipantMode
  final case class RemoteParticipantConfig(file: File) extends ParticipantMode
  final case class IdeLedgerParticipant() extends ParticipantMode
}
