// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import com.daml.http.perf.scenario.MultiUserQueryScenario._
import io.gatling.core.Predef._
import io.gatling.core.structure.PopulationBuilder
import io.gatling.http.Predef._

import scala.concurrent.duration._

private[scenario] trait HasRandomCurrency {
  protected val rng = new scala.util.Random(123456789)
  final val currencies = List("USD", "GBP", "EUR", "CHF", "AUD")

  def randomCurrency(): String = {
    this.synchronized { rng.shuffle(currencies).head }
  }
}

object MultiUserQueryScenario {
  sealed trait RunMode { def name: String }
  case object PopulateCache extends RunMode { val name = "populateCache" }
  case object FetchByKey extends RunMode { val name = "fetchByKey" }
  case object FetchByQuery extends RunMode { val name = "fetchByQuery" }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class MultiUserQueryScenario
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasRandomCurrency {

  private def getEnvValueAsInt(key: String, default: Int) = {
    sys.env.get(key).map(_.toInt).getOrElse(default)
  }

  //hardcoded for now , needs to be made configurable on cli
  private val numRecords = getEnvValueAsInt("NUM_RECORDS", 100000)
  private val numQueries = getEnvValueAsInt("NUM_QUERIES", 1000)
  private val numWriters = getEnvValueAsInt("NUM_WRITERS", 10)
  private val numReaders = getEnvValueAsInt("NUM_READERS", 100)
  private val runModeString = sys.env.getOrElse("RUN_MODE", "populateCache")

  def runMode(mode: String): RunMode = {
    mode match {
      case PopulateCache.name => PopulateCache
      case FetchByKey.name => FetchByKey
      case FetchByQuery.name => FetchByQuery
    }
  }

  private val msgIds = (1 to numRecords).toList

  private def queryId: Int = rng.shuffle(msgIds).head

  private val createRequest =
    http("CreateCommand")
      .post("/v1/create")
      .body(StringBody("""{
  "templateId": "LargeAcs:KeyedIou",
  "payload": {
    "id": "${id}",
    "issuer": "Alice",
    "owner": "Alice",
    "currency": "${currency}",
    "amount": "${amount}",
    "observers": ["Bob", "Trent"]
  }
}"""))

  private val fetchByQueryRequest =
    http("SyncIdQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
          "templateIds": ["LargeAcs:KeyedIou"],
          "query": {"id": "${id}"}
      }"""))

  private val writeScn = {
    val iter = msgIds.iterator
    scenario("Write100kContracts")
      .repeat(numRecords / numWriters) {
        feed(
          Iterator.continually(
            Map(
              "id" -> String.valueOf(iter.next()),
              "currency" -> randomCurrency(),
              "amount" -> randomAmount(),
            )
          )
        )
          .exec(createRequest)
      }
  }

  private val fetchByQueryScn = {
    val iter = msgIds.iterator
    scenario("MultipleReadersByQueryScenario")
      .repeat(numQueries / numReaders) {
        feed(Iterator.continually(Map("id" -> String.valueOf(iter.next()))))
          .exec(fetchByQueryRequest)
      }
  }

  private val fetchByKeyRequest =
    http("SyncFetchRequest")
      .post("/v1/fetch")
      .body(StringBody("""{
          "templateId": "LargeAcs:KeyedIou",
          "key": {
            "_1": "Alice",
            "_2": "${id}"
          }
      }"""))

  private val currencyQueryRequest =
    http("SyncCurrQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
          "templateIds": ["LargeAcs:KeyedIou"],
          "query": {"currency": "${currency}"}
      }"""))

  // Scenario to fetch a subset of the ACS population
  private def currQueryScn(numIterations: Int, curr: () => String) =
    scenario("MultipleReadersCurrQueryScenario")
      .repeat(numIterations) {
        feed(Iterator.continually(Map("currency" -> curr())))
          .exec(currencyQueryRequest)
      }

  //fetch by key scenario
  private val fetchByKeyScn = scenario("MultipleReadersByKeyScenario")
    .repeat(numQueries / numReaders) {
      feed(Iterator.continually(Map("id" -> String.valueOf(queryId))))
        .exec(fetchByKeyRequest)
    }

  def getPopulationBuilder(runMode: RunMode): PopulationBuilder = {
    runMode match {
      case PopulateCache =>
        val currIter = currencies.iterator
        writeScn
          .inject(atOnceUsers(numWriters))
          .andThen(
            currQueryScn(numIterations = currencies.size, () => currIter.next())
              .inject(
                nothingFor(2.seconds),
                atOnceUsers(1),
              )
          )
      case FetchByKey =>
        fetchByKeyScn.inject(
          nothingFor(5.seconds),
          atOnceUsers(numReaders / 2),
          rampUsers(numReaders / 2).during(10.seconds),
        )
      case FetchByQuery =>
        fetchByQueryScn.inject(
          nothingFor(2.seconds),
          atOnceUsers(numReaders),
        )
    }
  }

  setUp(
    getPopulationBuilder(runMode(runModeString))
  ).protocols(httpProtocol)
}
