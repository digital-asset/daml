// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.resources

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import HikariTransactorResourceOwner.NamedThreadFactory
import cats.effect.{Blocker, ContextShift, IO}
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ResourceOwnerFactories}
import com.zaxxer.hikari.HikariDataSource
import doobie.hikari.HikariTransactor

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

object HikariTransactorResourceOwner {

  object NamedThreadFactory {

    def cachedThreadPool(threadNamePrefix: String): ExecutorService =
      Executors.newCachedThreadPool(new NamedThreadFactory(threadNamePrefix))

    def fixedThreadPool(size: Int, threadNamePrefix: String): ExecutorService =
      Executors.newFixedThreadPool(size, new NamedThreadFactory(threadNamePrefix))

  }

  private final class NamedThreadFactory(threadNamePrefix: String) extends ThreadFactory {

    require(threadNamePrefix.nonEmpty, "The thread name prefix cannot be empty")

    private val threadCounter = new AtomicInteger(0)

    def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"$threadNamePrefix-${threadCounter.getAndIncrement()}")
      t.setDaemon(true)
      t
    }

  }

  def apply[Context: HasExecutionContext](
      factory: ResourceOwnerFactories[Context]
  )(jdbcUrl: String, username: String, password: String, maxPoolSize: Int)(implicit
      cs: ContextShift[IO]
  ): AbstractResourceOwner[Context, HikariTransactor[IO]] =
    new HikariTransactorResourceOwner[Context](factory).apply(
      jdbcUrl,
      username,
      password,
      maxPoolSize,
    )

}

@nowarn("msg=parameter value evidence.* is never used")
final class HikariTransactorResourceOwner[Context: HasExecutionContext] private (
    resourceOwner: ResourceOwnerFactories[Context]
) {

  private val BlockerPoolName = "transactor-blocker-pool"
  private val ConnectorPoolName = "transactor-connector-pool"

  private def managedBlocker: AbstractResourceOwner[Context, Blocker] =
    resourceOwner
      .forExecutorService(() => NamedThreadFactory.cachedThreadPool(BlockerPoolName))
      .map(Blocker.liftExecutorService)

  private def managedConnector(
      size: Int
  ): AbstractResourceOwner[Context, ExecutionContext] =
    resourceOwner
      .forExecutorService(() => NamedThreadFactory.fixedThreadPool(size, ConnectorPoolName))
      .map(ExecutionContext.fromExecutorService)

  private def managedHikariDataSource(
      jdbcUrl: String,
      username: String,
      password: String,
      maxPoolSize: Int,
  ): AbstractResourceOwner[Context, HikariDataSource] =
    resourceOwner.forCloseable { () =>
      val pool = new HikariDataSource()
      pool.setAutoCommit(false)
      pool.setJdbcUrl(jdbcUrl)
      pool.setUsername(username)
      pool.setPassword(password)
      pool.setMaximumPoolSize(maxPoolSize)
      pool
    }

  def apply(jdbcUrl: String, username: String, password: String, maxPoolSize: Int)(implicit
      cs: ContextShift[IO]
  ): AbstractResourceOwner[Context, HikariTransactor[IO]] =
    for {
      blocker <- managedBlocker
      connector <- managedConnector(size = maxPoolSize)
      dataSource <- managedHikariDataSource(jdbcUrl, username, password, maxPoolSize)
    } yield HikariTransactor[IO](dataSource, connector, blocker)

}
