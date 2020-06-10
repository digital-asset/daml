// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.HttpRequest

import scalaz.syntax.std.option._
import scalaz.{\/}

import java.nio.charset.StandardCharsets

case class Unauthorized(message: String) extends Error(message)

object TokenManagement {

  // Utility to get the username and password out of a basic auth
  // token. By construction we ensure that there will always be two
  // components (see 'findCredentials'). We use the first component to
  // identify parties.
  def decodeCredentials(credentials: UserCredentials): (String, String) = {
    val token = credentials.token
    val bytes = java.util.Base64.getDecoder.decode(token.getBytes())
    val components = new String(bytes, StandardCharsets.UTF_8).split(":")
    (components(0), components(1))
  }

  /*
   User : alice
   Password : &alC2l3SDS*V
   curl -X GET localhost:8080/hello -H "Authorization: Basic YWxpY2U6JmFsQzJsM1NEUypW"
   */
  def findCredentials(req: HttpRequest): Unauthorized \/ UserCredentials = {
    req.headers
      .collectFirst {
        case Authorization(c @ BasicHttpCredentials(username, password)) => {
          UserCredentials(c.token())
        }
      }
      .toRightDisjunction(Unauthorized("missing Authorization header with Basic Token"))
  }

}
