// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.DataSourceStorageBackend
import com.digitalasset.canton.platform.store.backend.common.{
  DataSourceStorageBackendImpl,
  InitHookDataSourceProxy,
}

import java.sql.Connection
import javax.sql.DataSource

object H2DataSourceStorageBackend extends DataSourceStorageBackend {
  override def createDataSource(
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
      connectionInitHook: Option[Connection => Unit],
  ): DataSource = {
    val h2DataSource = new org.h2.jdbcx.JdbcDataSource()

    // H2 (org.h2.jdbcx.JdbcDataSource) does not support setting the user/password within the jdbcUrl, so remove
    // those properties from the url if present and set them separately. Note that Postgres and Oracle support
    // user/password in the URLs, so we don't bother exposing user/password configs separately from the url just for h2
    // which is anyway not supported for production. (This also helps run canton h2 participants that set user and
    // password.)
    val (urlNoUserNoPassword, user, password) = extractUserPasswordAndRemoveFromUrl(
      dataSourceConfig.jdbcUrl
    )
    user.foreach(h2DataSource.setUser)
    password.foreach(h2DataSource.setPassword)
    h2DataSource.setUrl(urlNoUserNoPassword)

    InitHookDataSourceProxy(h2DataSource, connectionInitHook.toList, loggerFactory)
  }

  def extractUserPasswordAndRemoveFromUrl(
      jdbcUrl: String
  ): (String, Option[String], Option[String]) = {
    def setKeyValueAndRemoveFromUrl(url: String, key: String): (String, Option[String]) = {
      val regex = s".*(;(?i)${key}=([^;]*)).*".r
      url match {
        case regex(keyAndValue, value) =>
          (url.replace(keyAndValue, ""), Some(value))
        case _ => (url, None)
      }
    }

    val (urlNoUser, user) = setKeyValueAndRemoveFromUrl(jdbcUrl, "user")
    val (urlNoUserNoPassword, password) = setKeyValueAndRemoveFromUrl(urlNoUser, "password")
    (urlNoUserNoPassword, user, password)
  }

  override def checkDatabaseAvailable(connection: Connection): Unit =
    DataSourceStorageBackendImpl.checkDatabaseAvailable(connection)
}
