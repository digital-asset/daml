// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.oracle

import com.digitalasset.canton.concurrent.Threading
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.sql.*
import scala.annotation.tailrec
import scala.util.chaining.scalaUtilChainingOps
import scala.util.{Failure, Random, Success, Try, Using}

object OracleAround {

  final case class OracleServer(
      systemUser: String,
      systemPassword: String,
      host: String,
      port: Int,
      dbName: String,
      disconnect: () => Unit,
  ) {
    def jdbcUrlWithoutCredentials: String =
      s"jdbc:oracle:thin:@$host:$port/$dbName"

    def jdbcUrlWithCredentials(userName: String, password: String): String =
      s"jdbc:oracle:thin:$userName/$password@$host:$port/$dbName"
  }

  final case class RichOracleUser(
      oracleUser: OracleUser,
      jdbcUrlWithoutCredentials: String,
      jdbcUrl: String,
      drop: () => Unit,
  ) {

    /** In CI we re-use the same Oracle instance for testing, so non-colliding DB-lock-ids need to be assigned
      *
      * @return A positive integer, which defines a unique / mutually exclusive range of usable lock ids: [seed, seed + 10)
      */
    def lockIdSeed: Int = {
      assert(oracleUser.id > 0, "Lock ID seeding is not supported, cannot ensure unique lock-ids")
      assert(oracleUser.id < 10 * 1000 * 1000)
      val seed = oracleUser.id * 10
      val maxLockId = seed + 10
      assert(
        maxLockId < (1 << 29),
        "Lock IDs here have the 30th bit unset",
      )
      seed
    }
  }

  final case class OracleUser(name: String, id: Int) {
    val pwd: String = "hunter2"
  }

  private val logger = LoggerFactory.getLogger(getClass)

  def createNewUniqueRandomUser(oracleServer: OracleServer): RichOracleUser =
    createRichOracleUser(oracleServer) { stmt =>
      val id = Random.nextInt(1000 * 1000) + 1
      val user = OracleUser(s"U$id", id)
      createUser(stmt, user.name, user.pwd)
      logger.info(s"New unique random Oracle user created $user")
      user
    }

  private def createUser(stmt: Statement, name: String, pwd: String): Unit = {
    stmt.execute(s"""create user $name identified by $pwd""")
    stmt.execute(s"""grant connect, resource to $name""")
    stmt.execute(
      s"""grant create table, create materialized view, create view, create procedure, create sequence, create type to $name"""
    )
    stmt.execute(s"""alter user $name quota unlimited on users""")

    // for DBMS_LOCK access
    stmt.execute(s"""GRANT EXECUTE ON SYS.DBMS_LOCK TO $name""")
    stmt.execute(s"""GRANT SELECT ON V_$$MYSTAT TO $name""")
    stmt.execute(s"""GRANT SELECT ON V_$$LOCK TO $name""")
    ()
  }

  private def createRichOracleUser(
      oracleServer: OracleServer
  )(createBody: Statement => OracleUser): RichOracleUser = {
    def withStmt[T](body: Statement => T): T =
      Using.resource(
        DriverManager.getConnection(
          oracleServer.jdbcUrlWithoutCredentials,
          oracleServer.systemUser,
          oracleServer.systemPassword,
        )
      ) { connection =>
        connection.setAutoCommit(false)
        val result = Using.resource(connection.createStatement())(body)
        connection.commit()
        result
      }

    @tailrec
    def retry[T](times: Int, sleepMillisBeforeReTry: Long)(body: => T): T = Try(body) match {
      case Success(t) => t
      case Failure(_) if times > 0 =>
        if (sleepMillisBeforeReTry > 0) Threading.sleep(sleepMillisBeforeReTry)
        retry(times - 1, sleepMillisBeforeReTry)(body)
      case Failure(t) => throw t
    }

    retry(20, 100) {
      withStmt { stmt =>
        logger.info("Trying to create Oracle user")
        val oracleUser = createBody(stmt)
        logger.info(s"Oracle user ready $oracleUser")
        RichOracleUser(
          oracleUser = oracleUser,
          jdbcUrlWithoutCredentials = oracleServer.jdbcUrlWithoutCredentials,
          jdbcUrl = oracleServer.jdbcUrlWithCredentials(oracleUser.name, oracleUser.pwd),
          drop = () => {
            retry(10, 1000) {
              logger.info(s"Trying to remove Oracle user ${oracleUser.name}")
              withStmt(_.execute(s"""drop user ${oracleUser.name} cascade"""))
            }
            logger.info(s"Oracle user removed successfully ${oracleUser.name}")
            ()
          },
        )
      }
    }
  }

  def connectToOracleServer(): OracleServer = {
    val isCI = sys.env.contains("CI")
    val isMachine = sys.env.contains("MACHINE")
    val forceTestContainer = sys.env.contains("DB_FORCE_TEST_CONTAINER")
    val useEnterprise = sys.env.contains("ORACLE_USE_ENTERPRISE")

    if (!forceTestContainer && (isCI && !isMachine)) {
      logger.info(s"Using Oracle Server instance at localhost:${Config.defaultPort}")
      OracleServer(
        systemUser = Config.sysDbaUserName,
        systemPassword = env("ORACLE_PWD"),
        host = "localhost",
        port = Config.defaultPort,
        dbName = env("ORACLE_DB"),
        disconnect = () => (),
      )
    } else {
      val (dockerImage, dbName, hostToNetwork) =
        if (useEnterprise)
          (Config.enterpriseDockerImage, Config.enterpriseDbName, true)
        else (Config.xeDockerImage, Config.xeDbName, false)

      class CustomOracleContainer
          extends GenericContainer[CustomOracleContainer](DockerImageName.parse(dockerImage))

      val oracleContainer: CustomOracleContainer = new CustomOracleContainer()
        .withExposedPorts(Config.defaultPort)
        .waitingFor(Wait.forLogMessage("DATABASE IS READY TO USE!\\n", 1))
        .withStartupTimeout(java.time.Duration.ofSeconds(160))
        .pipe(if (hostToNetwork) _.withNetworkMode("host") else identity)

      logger.info(s"Starting Oracle container $dockerImage / $dbName...")
      oracleContainer.start()
      logger.info(s"Started Oracle container $dockerImage / $dbName.")
      val host: String = if (hostToNetwork) "localhost" else oracleContainer.getHost
      val port: Int = if (hostToNetwork) Config.defaultPort else oracleContainer.getFirstMappedPort
      logger.info(s"Using Oracle Container instance at $host:$port")
      OracleServer(
        systemUser = Config.sysDbaUserName,
        systemPassword = "hunter2",
        host = host,
        port = port,
        dbName = dbName,
        disconnect = () => {
          logger.info("Stopping oracle container...")
          oracleContainer.close()
          logger.info("Oracle container stopped.")
        },
      )
    }
  }

  private def env(name: String): String =
    sys.env.getOrElse(name, sys.error(s"Environment variable not set [$name]"))

  private object Config {
    val xeDockerImage: String = "digitalasset/oracle:xe-18.4.0-preloaded-20210325-22-be14fb7"
    val enterpriseDockerImage: String =
      "digitalasset/oracle:enterprise-19.20.0-preloaded-20230908-42-a5f5feb"
    val xeDbName: String = "XEPDB1"
    val enterpriseDbName: String = "ORCLPDB1"
    val defaultPort: Int = 1521
    val sysDbaUserName: String = "sys as sysdba"
  }
}
