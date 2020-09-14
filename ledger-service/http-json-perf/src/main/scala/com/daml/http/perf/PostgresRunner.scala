package com.daml.http.perf

import com.daml.testing.postgresql.PostgresAroundSuite
import org.scalatest.Suite

class PostgresRunner {
  private val suite = new PostgresAroundSuite with Suite {}

  def createNewRandomDatabase(): Unit = {
//    suite.c
  }

}
