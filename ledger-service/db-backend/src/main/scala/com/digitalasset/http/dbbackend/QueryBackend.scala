// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import scalaz.syntax.std.option._
import scalaz.syntax.std.string._

import QueryBackend.RawConf
import OracleQueries.DisableContractPayloadIndexing

private[http] sealed abstract class QueryBackend {
  type SqlInterpol
  type Conf

  def queries(tablePrefix: String, conf: Conf)(implicit sqli: SqlInterpol): Queries

  def parseConf(kvs: RawConf): Either[String, Conf]
}

private[http] object QueryBackend {
  type RawConf = Map[String, String]
  type Aux[SqlI] = QueryBackend { type SqlInterpol = SqlI }
  type Ap[SqlInterpol0, Conf0] = {
    type SqlInterpol = SqlInterpol0
    type Conf = Conf0
  }
}

private[dbbackend] object PostgresQueryBackend extends QueryBackend {
  type SqlInterpol = Queries.SqlInterpolation.StringArray
  type Conf = Unit

  override def queries(tablePrefix: String, conf: Conf)(implicit sqli: SqlInterpol) =
    new PostgresQueries(tablePrefix)

  override def parseConf(kvs: RawConf) = Right(())
}

private[dbbackend] object OracleQueryBackend extends QueryBackend {
  type SqlInterpol = Queries.SqlInterpolation.Unused
  type Conf = DisableContractPayloadIndexing

  override def queries(tablePrefix: String, conf: Conf)(implicit sqli: SqlInterpol) =
    new OracleQueries(tablePrefix, conf)

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
