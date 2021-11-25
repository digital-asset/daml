// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.lf.data.Ref

object UserManagement {

  case class User(
      id: String,
      primaryParty: Ref.Party,
  )

  trait Right
  object Right {
    case object ParticipantAdmin extends Right
    case class CanActAs(party: Ref.Party) extends Right
    case class CanReadAs(party: Ref.Party) extends Right
  }
}
