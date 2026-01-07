// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.net.URI
import scala.util.Try

package jwt {

  final case class KeyPair[A](publicKey: A, privateKey: A)

  final case class Jwt(value: String)

  final case class DecodedJwt[A](header: A, payload: A) {
    def transform[B](f: A => Either[Error, B]): Either[Error, DecodedJwt[B]] =
      for {
        header <- f(header)
        payload <- f(payload)
      } yield DecodedJwt(header, payload)
  }

  final case class JwksUrl(value: String) extends AnyVal {
    def toURL = new URI(value).toURL
  }

  object JwksUrl {
    def fromString(value: String): Either[String, JwksUrl] =
      Try(new URI(value).toURL).toEither.left
        .map(_.getMessage)
        .map(_ => JwksUrl(value))

    def assertFromString(str: String): JwksUrl = fromString(str) match {
      case Right(value) => value
      case Left(err) => throw new IllegalArgumentException(err)
    }
  }
}
