package com.daml.runtime

object JdbcDrivers {
  val supportedJdbcDriverNames: Set[String] = Set("org.postgresql.Driver", "oracle.jdbc.OracleDriver")
}
