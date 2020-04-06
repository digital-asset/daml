// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.caching

case class Configuration(maximumWeight: Size)

object Configuration {
  def none: Configuration = Configuration(maximumWeight = 0)
}
