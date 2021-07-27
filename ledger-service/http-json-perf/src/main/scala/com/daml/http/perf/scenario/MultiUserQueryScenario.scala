// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random

private[scenario] trait HasRandomCurrency {
  private val rng = new scala.util.Random(123456789)
  final val currencies = List("USD", "GBP", "EUR", "CHF", "AUD")

  def randomCurrency(): String = {
    this.synchronized { rng.shuffle(currencies).head }
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class MultiUserQueryScenario
    extends Simulation
    with SimulationConfig
    with HasRandomAmount
    with HasRandomCurrency {

  private val logger = LoggerFactory.getLogger(getClass)

  //hardcoded for now , needs to be made configurable on cli
  private val numRecords = 100000
  private val numQueries = 1000
  private val numWriters = 10
  private val numReaders = 100

  private val msgIds = Range(0, numRecords).toList
  private val msgIdIter = msgIds.iterator

  def incrementalId = msgIdIter.next()
  def queryId: Int = Random.shuffle(msgIds).head

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
    "observers": []
  }
}"""))

  private val queryRequest =
    http("SyncIdQueryRequest")
      .post("/v1/query")
      .body(StringBody("""{
          "templateIds": ["LargeAcs:KeyedIou"],
          "query": {"id": "${id}"}
      }"""))

  private val writeScn = scenario("Write100kContracts")
    .repeat(numRecords / numWriters) {
      feed(
        Iterator.continually(
          Map(
            "id" -> String.valueOf(incrementalId),
            "currency" -> randomCurrency(),
            "amount" -> randomAmount(),
          )
        )
      )
        .exec(createRequest)
    }

  private val idReadScn = scenario("MultipleReadersQueryScenario")
    .repeat(numQueries / numReaders) {
      feed(Iterator.continually(Map("id" -> String.valueOf(queryId))))
        .exec(queryRequest)
    }

  private val fetchRequest =
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
  private val currQueryScn = scenario("MultipleReadersCurrQueryScenario")
    .repeat(numQueries / numReaders) {
      feed(Iterator.continually(Map("currency" -> randomCurrency())))
        .exec(currencyQueryRequest)
    }

  //fetch by key scenario
  private val fetchScn = scenario("MultipleReadersFetchScenario")
    .repeat(numQueries / numReaders) {
      feed(Iterator.continually(Map("id" -> String.valueOf(queryId))))
        .exec(fetchRequest)
    }

  logger.debug(s"Scenarios defined $fetchScn $currQueryScn $idReadScn $writeScn")
  setUp(
    writeScn
      .inject(atOnceUsers(numWriters))
      .andThen(
//      fetchScn.inject(
//        nothingFor(5.seconds),
//        atOnceUsers(numReaders/2),
//        rampUsers(numReaders/2).during(10.seconds)).andThen(
//        idReadScn.inject(
//          nothingFor(2.seconds),
//          atOnceUsers(numReaders)).andThen(
//          currQueryScn.inject(
//            nothingFor(2.seconds),
//            atOnceUsers(numReaders))
//        )
//      )
        idReadScn.inject(nothingFor(5.seconds), atOnceUsers(numReaders))
//      fetchScn.inject(
//        nothingFor(5.seconds),
//        atOnceUsers(numReaders))
      )
  ).protocols(httpProtocol)
}
