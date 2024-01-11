// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

final case class AggregationId(id: Hash) extends PrettyPrinting {
  override def pretty: Pretty[AggregationId] = prettyOfParam(_.id)

  def toProtoPrimitive: ByteString = id.getCryptographicEvidence
}

object AggregationId {
  // We serialize/deserialize aggregation IDs as hex strings rather than bytestrings so that they can be used as primary keys in all DBs
  implicit val setParameterAggregationId: SetParameter[AggregationId] = (v, pp) =>
    pp.>>(v.id.toLengthLimitedHexString)

  implicit val getResultAggregationId: GetResult[AggregationId] = GetResult { r =>
    val hex = r.nextString()
    val hash = Hash
      .fromHexString(hex)
      .valueOr(err =>
        throw new DbDeserializationException(s"Could not deserialize aggregation id: $err")
      )
    AggregationId(hash)
  }

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[AggregationId] =
    Hash.fromProtoPrimitive(bytes).map(id => AggregationId(id))
}
