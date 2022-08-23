// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.platform.apiserver.meteringreport.HmacSha256.Key
import spray.json._

import java.net.URL
import java.nio.charset.StandardCharsets

sealed trait MeteringReportKey {
  def key: Key
}

object MeteringReportKey {

  case object CommunityKey extends MeteringReportKey {
    val key: Key = communityKey()
  }
  final case class EnterpriseKey(key: Key) extends MeteringReportKey

  def communityKey(): Key = readSystemResourceAsKey(
    getClass.getClassLoader.getResource("metering-keys/community.json")
  )

  /** It may help when loading from the class path:
    *  - To start with a `Class` close to the key resource location
    *  - Get the `ClassLoader` associated with that class
    *  - Use the `getResource` classloader method.
    */
  def readSystemResourceAsKey(keyUrl: URL): Key = {
    val json = new String(keyUrl.openStream().readAllBytes(), StandardCharsets.UTF_8)
    json.parseJson.convertTo[Key]
  }

}
