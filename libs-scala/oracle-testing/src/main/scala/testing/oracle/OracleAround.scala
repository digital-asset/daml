// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.oracle

import com.daml.ports._
import java.sql._
import scala.util.{Random, Using}

private[daml] final case class User(name: String, pwd: String)

trait OracleAround {
  @volatile
  private var systemUser: String = _
  @volatile
  private var systemPwd: String = _
  @volatile
  private var port: Port = _

  def oraclePort: Port = port

  def oracleJdbcUrl: String = s"jdbc:oracle:thin:@localhost:$oraclePort/ORCLPDB1"

  protected def connectToOracle(): Unit = {
    systemUser = sys.env("ORACLE_USERNAME")
    systemPwd = sys.env("ORACLE_PWD")
    port = Port(sys.env("ORACLE_PORT").toInt)
  }

  protected def createNewRandomUser(): User = {
    // See https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm#i27570
    // for name restrictions.
    val u = "u" + Random.alphanumeric.take(29).mkString("")
    createNewUser(u.toUpperCase)
  }

  protected def checkUserExists(con: Connection, name: String): Boolean = {
    Using(con.createStatement()) { stmt =>
      val res = stmt.executeQuery(
        s"""SELECT count(*) AS user_count FROM all_users WHERE username='${name.toUpperCase}'"""
      )
      val count = if (res.next()) {
        res.getInt("user_count")
      } else 0
      count > 0
    }.get
  }

  protected def createNewUser(name: String, pwd: String = "hunter2"): User = {
    Using.Manager { use =>
      val con = use(
        DriverManager.getConnection(
          s"jdbc:oracle:thin:@localhost:$port/ORCLPDB1",
          "sys as sysdba", // TODO this is needed for being able to grant the execute access for the sys.dbms_lock below. Consider making this configurable
          systemPwd,
        )
      )
      if (!checkUserExists(con, name)) {
        val stmt = con.createStatement()
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
      } else false
    }.get
    User(name, pwd)
  }

  protected def dropUser(name: String): Unit = {
    Using.Manager { use =>
      val con = use(
        DriverManager.getConnection(
          s"jdbc:oracle:thin:@localhost:$port/ORCLPDB1",
          systemUser,
          systemPwd,
        )
      )
      val stmt = use(con.createStatement())
      stmt.execute(s"""drop user $name cascade""")
    }.get
    ()
  }
}
