// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.OptionT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.{IndexedStringStore, IndexedStringType}

import scala.concurrent.{ExecutionContext, Future}

class DbIndexedStringStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends IndexedStringStore
    with DbStore {

  import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
  import storage.api.*

  override def getOrCreateIndex(dbTyp: IndexedStringType, str: String300): Future[Int] =
    getIndexForStr(dbTyp.source, str).getOrElseF {
      insertIgnore(dbTyp.source, str).flatMap { _ =>
        getIndexForStr(dbTyp.source, str).getOrElse {
          noTracingLogger.error(s"static string $str is still missing in db after i just stored it")
          throw new IllegalStateException(
            s"static string $str is still missing in db after i just stored it"
          )
        }
      }
    }

  private def getIndexForStr(dbType: Int, str: String300): OptionT[Future, Int] =
    OptionT(
      storage
        .query(
          sql"select id from common_static_strings where string = $str and source = $dbType"
            .as[Int]
            .headOption,
          functionFullName,
        )
    )

  private def insertIgnore(dbType: Int, str: String300): Future[Unit] = {
    // not sure how to get "last insert id" here in case the row was inserted
    // therefore, we're just querying the db again. this is a bit dorky,
    // but we'll hardly ever do this, so should be good
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        sqlu"insert into common_static_strings (string, source) values ($str, $dbType) ON CONFLICT DO NOTHING"
      case _: DbStorage.Profile.Oracle =>
        sqlu"""INSERT
              /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( common_static_strings (string, source) ) */
              INTO common_static_strings (string, source) VALUES ($str,$dbType)"""
    }
    // and now query it
    storage.update_(query, functionFullName)
  }

  override def getForIndex(dbTyp: IndexedStringType, idx: Int): Future[Option[String300]] = {
    storage
      .query(
        sql"select string from common_static_strings where id = $idx and source = ${dbTyp.source}"
          .as[String300],
        functionFullName,
      )
      .map(_.headOption)
  }
}
