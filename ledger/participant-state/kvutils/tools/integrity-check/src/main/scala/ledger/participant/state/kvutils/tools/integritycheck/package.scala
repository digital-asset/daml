// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import scala.language.implicitConversions

package object integritycheck {
  implicit def color(text: String): Color = new Color(text)
}
