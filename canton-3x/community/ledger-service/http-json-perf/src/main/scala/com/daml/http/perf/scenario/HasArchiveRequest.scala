// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

import io.gatling.core.Predef._
import io.gatling.http.Predef._

private[scenario] trait HasArchiveRequest {

  lazy val archiveRequest =
    http("ExerciseCommand")
      .post("/v1/exercise")
      .body(StringBody("""{
    "templateId": "Iou:Iou",
    "contractId": "${archiveContractId}",
    "choice": "Archive",
    "argument": {}
}"""))

}
