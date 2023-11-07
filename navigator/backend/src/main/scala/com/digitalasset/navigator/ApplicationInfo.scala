// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class ApplicationInfo(
    id: String,
    name: String,
    version: String,
)

trait ApplicationInfoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val applicationInfoFormat: RootJsonFormat[ApplicationInfo] = jsonFormat3(ApplicationInfo)
}
