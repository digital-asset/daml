// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.grpcHeaders

import scala.io.Source
import com.google.auth.oauth2.AccessToken
import io.grpc.Metadata

import scala.util.control.NonFatal

class GrpcHeadersWithAccessToken(accessToken: AccessToken) extends GrpcHeadersDataOrigin {
  import GrpcHeadersWithAccessToken._

  def toHeaders(metadata: Map[String, List[String]]): Metadata = {
    val headers = new Metadata
    for (key <- metadata.keySet;
      values <- metadata.get(key)) {
      val headerKey: Metadata.Key[String] = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)
      for (value <- values) {
        headers.put(headerKey, value)
      }
    }
    headers
  }
  override def getAdditionalHeaderData: Metadata = {
    var metadata = new Metadata()
    metadata.put(
      Metadata.Key.of(AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER),
      BEARER_PREFIX + accessToken.getTokenValue)
    metadata
  }
}

object GrpcHeadersWithAccessToken {
  val AUTHORIZATION = "Authorization"
  val BEARER_PREFIX = "Bearer "

  def fromConfig(config: AuthorizationConfig): Option[GrpcHeadersWithAccessToken] = {
    config match {
      case AuthorizationConfig.FileAccessToken(path) => fromFile(path)
      case _ => None
    }
  }

  def fromFile(path: String): Option[GrpcHeadersWithAccessToken] = {
    try {
      val source = Source.fromFile(path)
      val token = source.mkString
      source.close()
      Some(new GrpcHeadersWithAccessToken(new AccessToken(token, null)))
    } catch {
      case NonFatal(e) => None
    }
  }
}
