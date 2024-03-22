// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.metrics.Metrics
import scalaz.Liskov, Liskov.<~<

/** Incompatible JDBC operations and settings, selected by
  * the options passed to json-api at startup.
  */
final class SupportedJdbcDriver[+Q] private (
    label: String,
    private[http] val retrySqlStates: Set[String],
    private[http] val q: Q,
) {
  import SupportedJdbcDriver.{Staged, Configured}
  def configure(tablePrefix: String, extraConf: Map[String, String], tpIdCacheMaxEntries: Long)(
      implicit
      Q: Q <~< Staged,
      metrics: Metrics,
  ): Either[String, SupportedJdbcDriver.TC] = {
    val staged: Staged = Q(q)
    import staged.{backend, ipol}
    backend.parseConf(extraConf) map { conf =>
      new SupportedJdbcDriver(
        label,
        retrySqlStates,
        new Configured(backend.queries(tablePrefix, conf, tpIdCacheMaxEntries)),
      )
    }
  }

  override def toString = s"SupportedJdbcDriver($label, $q)"
}

object SupportedJdbcDriver {
  type Available = SupportedJdbcDriver[Staged]

  sealed abstract class Staged {
    type SqlInterpol
    type Conf
    val backend: QueryBackend.Aux2[SqlInterpol, Conf]
    implicit val ipol: SqlInterpol
    override final def toString = "Staged"
  }

  private final class StagedInstance[SI, C](
      override val backend: QueryBackend.Aux2[SI, C]
  )(implicit override val ipol: SI)
      extends Staged {
    type SqlInterpol = SI
    type Conf = C
  }

  /** A nullary typeclass, with extra configuration from startup.
    *
    * We currently use the fact that it is nonsensical to run json-api against more
    * than one JDBC driver ''in the image'' as justification to make this typeclass
    * nullary.  If that changes in the future, a phantom type parameter should be
    * introduced so as to distinguish instances by type.
    */
  type TC = SupportedJdbcDriver[Configured]

  final class Configured private[SupportedJdbcDriver] (private[http] val queries: Queries) {
    override final def toString = "Configured"
  }

  val Postgres: Available = {
    import doobie.postgres.implicits._
    import doobie.postgres.sqlstate.{class23 => postgres_class23}
    val queries = Queries.Postgres
    implicit val ipol: queries.SqlInterpol = new Queries.SqlInterpolation.StringArray()
    new SupportedJdbcDriver(
      label = "PostgreSQL",
      q = new StagedInstance(queries),
      retrySqlStates =
        Set(postgres_class23.UNIQUE_VIOLATION.value, ContractDao.StaleOffsetException.SqlState),
    )
  }

  val Oracle: Available = {
    val queries = Queries.Oracle
    implicit val ipol: queries.SqlInterpol = new Queries.SqlInterpolation.Unused()
    new SupportedJdbcDriver(
      label = "Oracle",
      q = new StagedInstance(queries),
      // all oracle class 23 errors yield 23000; if we want to check for *unique*
      // violation specifically we'll have to look at something other than the SQLState.
      // All other class 23 errors indicate a bug, which should exhaust the retry loop anyway
      // likewise, class 61 covers a swath of errors for which one transaction was aborted
      // in favor of continuing another [conflicting] transaction, for which retrying
      // seems appropriate as well
      retrySqlStates = Set("23000", "61000", ContractDao.StaleOffsetException.SqlState),
    )
  }
}
