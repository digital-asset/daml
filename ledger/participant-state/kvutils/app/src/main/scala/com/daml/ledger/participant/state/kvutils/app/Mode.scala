// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

sealed abstract class Mode

object Mode {

  case object Run extends Mode

  final case class DumpIndexMetadata(jdbcUrls: Vector[String]) extends Mode

}
