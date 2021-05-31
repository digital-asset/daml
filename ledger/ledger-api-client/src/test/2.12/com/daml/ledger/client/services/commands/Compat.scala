// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

object Compat {
  // XXX SC remove in Scala 2.13; see notes in ConfSpec
  import scala.collection.GenTraversable, org.scalatest.enablers.Containing
  private[commands] implicit def `fixed sig containingNatureOfGenTraversable`[
      E: org.scalactic.Equality,
      TRAV,
  ]: Containing[TRAV with GenTraversable[E]] =
    Containing.containingNatureOfGenTraversable[E, GenTraversable]
}
