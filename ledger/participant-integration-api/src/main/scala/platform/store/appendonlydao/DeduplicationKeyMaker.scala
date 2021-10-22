// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import com.daml.ledger.api.domain
import com.daml.lf.data.Ref

import java.security.MessageDigest
import scalaz.syntax.tag._

// TODO append-only: move to store
object DeduplicationKeyMaker {
  def make(commandId: domain.CommandId, submitters: List[Ref.Party]): String =
    commandId.unwrap + "%" + hashSubmitters(submitters.sorted(Ordering.String).distinct)

  private def hashSubmitters(submitters: List[Ref.Party]): String = {
    MessageDigest
      .getInstance("SHA-256")
      .digest(submitters.mkString.getBytes)
      .mkString
  }
}
