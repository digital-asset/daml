// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

sealed abstract class Mode

object Mode {

  /** Run the participant */
  case object Run extends Mode

  /** Dump index metadata and exit */
  final case class DumpIndexMetadata(jdbcUrls: Vector[String]) extends Mode

}
