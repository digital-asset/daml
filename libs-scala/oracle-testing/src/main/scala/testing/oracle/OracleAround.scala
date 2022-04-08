// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import com.daml.ports._

import java.sql._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try, Using}

object OracleAround {

  case class RichOracleUser(
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
      oracleUser.id * 10
    }
  }

  case class OracleUser(name: String, id: Int) {
    val pwd: String = "hunter2"
  }

  private val logger = LoggerFactory.getLogger(getClass)

  def createNewUniqueRandomUser(): RichOracleUser = createRichOracleUser { stmt =>
    val id = Random.nextInt(1000 * 1000) + 1
    val user = OracleUser(s"U$id", id)
    createUser(stmt, user.name, user.pwd)
    logger.info(s"New unique random Oracle user created $user")
    user
  }

  def createOrReuseUser(name: String): RichOracleUser = createRichOracleUser { stmt =>
    val user = OracleUser(name.toUpperCase, -1)
    if (!userExists(stmt, user.name)) {
      createUser(stmt, user.name, user.pwd)
      logger.info(s"User $name not found: new Oracle user created $user")
    } else {
      logger.info(s"User $name already created: re-using existing Oracle user $user")
    }
    user
  }

  private def userExists(stmt: Statement, name: String): Boolean = {
    val res = stmt.executeQuery(
      s"""SELECT count(*) AS user_count FROM all_users WHERE username='$name'"""
    )
    res.next()
    res.getInt("user_count") > 0
  }

  private def tableExists(stmt: Statement, name: String): Boolean = {
    // Note: Oracle always stores table names in upper case
    val res = stmt.executeQuery(
      s"""SELECT count(*) AS table_count FROM user_tables WHERE table_name='${name.toUpperCase}'"""
    )
    res.next()
    res.getInt("table_count") > 0
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

  private val serverErrorsTable = "servererrors_table"

  // Debugging Oracle errors is very annoying:
  // - Oracle does not provide any details for errors (it will just print "ORA-XYZ: something went wrong")
  // - Oracle doesn't have any built in error log
  // - Our use of anorm and CompositeSql makes it hard to see what statements we are actually sending to the server
  //
  // To simplify debugging, we create a trigger that inserts all failing SQL statements into a new table
  // Notes:
  // - The table must not exist in the schema of the new user, as otherwise Flyway complains about a non-empty schema
  // - In Oracle, a schema is the same as a user. We'll use the sys user to own the table and the trigger.
  // - To debug a failing trigger, execute "select * from user_errors where type = 'TRIGGER'"
  private def createErrorTrigger(stmt: Statement): Unit = {
    if (!tableExists(stmt, serverErrorsTable)) {
      logger.info(s"Creating table $serverErrorsTable and trigger")
      stmt.execute(
        s"create table $serverErrorsTable( usr varchar2(1000), err varchar2(1000), msg clob )"
      )
      stmt.execute(s"""
        create or replace trigger servererror_trigger
        after servererror on database
        declare
            l_sql_text ora_name_list_t;
            l_n        number;
            v_stmt     clob;
        begin
            l_n := ora_sql_txt( l_sql_text );
            for i in 1 .. l_n loop
                v_stmt := v_stmt || l_sql_text(i);
            end loop;

            insert into $serverErrorsTable values ( ora_login_user, ora_server_error(1), v_stmt );
        end;
      """)
    } else {
      logger.info(s"Table $serverErrorsTable already exists, not creating any trigger")
    }

    ()
  }

  private def getServerErrors(stmt: Statement): List[String] = {
    Using.resource(stmt.executeQuery(s"select * from $serverErrorsTable")) { result =>
      val lb = ListBuffer[String]()
      while (result.next()) {
        val msg = result.getString("msg")
        val err = result.getString("err")
        lb.addOne(s"The following statement produced error $err: $msg")
      }
      lb.toList
    }
  }

  private def createRichOracleUser(createBody: Statement => OracleUser): RichOracleUser = {
    val systemUser = sys.env("ORACLE_USERNAME")
    val systemPwd = sys.env("ORACLE_PWD")
    val host = sys.env.getOrElse("ORACLE_HOST", "localhost")
    val port = Port(sys.env("ORACLE_PORT").toInt)
    val jdbcUrlWithoutCredentials = s"jdbc:oracle:thin:@$host:$port/ORCLPDB1"

    def withStmt[T](connectingUserName: String, password: String)(body: Statement => T): T =
      Using.resource(
        DriverManager.getConnection(
          jdbcUrlWithoutCredentials,
          connectingUserName,
          password,
        )
      ) { connection =>
        connection.setAutoCommit(false)
        val result = Using.resource(connection.createStatement())(body)
        connection.commit()
        result
      }

    @tailrec
    def retry[T](times: Int, sleepMillisBeforeReTry: Long = 0)(body: => T): T = Try(body) match {
      case Success(t) => t
      case Failure(_) if times > 0 =>
        if (sleepMillisBeforeReTry > 0) Thread.sleep(sleepMillisBeforeReTry)
        retry(times - 1)(body)
      case Failure(t) => throw t
    }

    retry(20, 100) {
      withStmt(
        connectingUserName =
          "sys as sysdba", // TODO this is needed for being able to grant the execute access for the sys.dbms_lock below. Consider making this configurable
        password = systemPwd,
      ) { stmt =>
        logger.info("Trying to create Oracle user")
        val oracleUser = createBody(stmt)
        withStmt("sys as sysdba", systemPwd)(createErrorTrigger)
        logger.info(s"Oracle user ready $oracleUser")
        RichOracleUser(
          oracleUser = oracleUser,
          jdbcUrlWithoutCredentials = jdbcUrlWithoutCredentials,
          jdbcUrl = s"jdbc:oracle:thin:${oracleUser.name}/${oracleUser.pwd}@$host:$port/ORCLPDB1",
          drop = () => {
            withStmt("sys as sysdba", systemPwd)(getServerErrors(_).map(logger.warn))
            retry(10, 1000) {
              logger.info(s"Trying to remove Oracle user ${oracleUser.name}")
              withStmt(systemUser, systemPwd)(
                _.execute(s"""drop user ${oracleUser.name} cascade""")
              )
            }
            logger.info(s"Oracle user removed successfully ${oracleUser.name}")
            ()
          },
        )
      }
    }
  }

}
