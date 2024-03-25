// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.http.metrics.HttpJsonApiMetrics
import scalaz.syntax.std.option._
import scalaz.syntax.std.string._

import QueryBackend.RawConf
import OracleQueries.DisableContractPayloadIndexing

private[http] sealed abstract class QueryBackend {
  type SqlInterpol
  // Why does the difference between QueryBackend and Queries matter at all?
  // Because we cannot talk about the `Conf` of a (String => Queries) without
  // introducing another existential scope, which is not only a bit mind-twisting,
  // but won't work at all in Scala 3.  So we introduce the scope *here*, with
  // the added benefit that we can leave only the purely existential interactions
  // with `Queries` as that type's responsibility.
  type Conf

  def queries(tablePrefix: String, conf: Conf, tpIdCacheMaxEntries: Long)(implicit
      sqli: SqlInterpol,
      metrics: HttpJsonApiMetrics,
  ): Queries

  def parseConf(kvs: RawConf): Either[String, Conf]
}

private[http] object QueryBackend {
  type RawConf = Map[String, String]
  type Aux[SqlI] = QueryBackend { type SqlInterpol = SqlI }
  type Aux2[SqlI, Conf0] = QueryBackend { type SqlInterpol = SqlI; type Conf = Conf0 }
}

private[dbbackend] object PostgresQueryBackend extends QueryBackend {
  type SqlInterpol = Queries.SqlInterpolation.StringArray
  type Conf = Unit

  override def queries(tablePrefix: String, conf: Conf, tpIdCacheMaxEntries: Long)(implicit
      sqli: SqlInterpol,
      metrics: HttpJsonApiMetrics,
  ) =
    new PostgresQueries(tablePrefix, tpIdCacheMaxEntries)

  override def parseConf(kvs: RawConf) = Right(())
}

private[dbbackend] object OracleQueryBackend extends QueryBackend {
  type SqlInterpol = Queries.SqlInterpolation.Unused
  type Conf = DisableContractPayloadIndexing

  override def queries(tablePrefix: String, conf: Conf, tpIdCacheMaxEntries: Long)(implicit
      sqli: SqlInterpol,
      metrics: HttpJsonApiMetrics,
  ) =
    new OracleQueries(tablePrefix, conf, tpIdCacheMaxEntries)

  override def parseConf(kvs: RawConf): Either[String, Conf] =
    kvs
      .get(DisableContractPayloadIndexing)
      .cata(
        v =>
          v.parseBoolean.toEither.left
            .map(_ => s"$DisableContractPayloadIndexing must be a boolean, not $v"),
        Right(false),
      )
}
