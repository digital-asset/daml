// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

sealed abstract class Mode

object Mode {

  /** Run the participant */
  case object Run extends Mode

  /** Dump index metadata and exit */
  final case class DumpIndexMetadata(jdbcUrls: Vector[String]) extends Mode

}
