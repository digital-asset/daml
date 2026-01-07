// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.resource.ToDbPrimitive
import com.digitalasset.canton.store.db.DbDeserializationException
import slick.jdbc.GetResult

final case class ChangeIdHash(hash: LfHash) extends PrettyPrinting {
  override protected def pretty: Pretty[ChangeIdHash] = prettyOfClass(
    unnamedParam(_.hash)
  )
}

object ChangeIdHash {
  def apply(changeId: ChangeId): ChangeIdHash = ChangeIdHash(changeId.hash)

  implicit val getResultChangeIdHash: GetResult[ChangeIdHash] = { r =>
    val hex = r.nextString()

    ChangeIdHash(
      LfHash
        .fromString(hex)
        .getOrElse(throw new DbDeserializationException(s"Cannot parse change ID hash $hex"))
    )
  }

  implicit val changeIdToDbPrimitive: ToDbPrimitive[ChangeIdHash, String] =
    ToDbPrimitive(_.hash.toHexString)

}
