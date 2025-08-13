// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import scalaz.syntax.applicative.*
import scalaz.{Applicative, Traverse}

import java.net.URI
import scala.util.Try

package jwt {

  final case class KeyPair[A](publicKey: A, privateKey: A)

  final case class Jwt(value: String)

  final case class DecodedJwt[A](header: A, payload: A)

  object KeyPair {
    implicit val traverseInstance: Traverse[KeyPair] = new Traverse[KeyPair] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: KeyPair[A]
      )(f: A => G[B]): G[KeyPair[B]] =
        ^(f(fa.publicKey), f(fa.privateKey))(KeyPair(_, _))
    }
  }

  object DecodedJwt {
    implicit val traverseInstance: Traverse[DecodedJwt] = new Traverse[DecodedJwt] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: DecodedJwt[A]
      )(f: A => G[B]): G[DecodedJwt[B]] =
        ^(f(fa.header), f(fa.payload))((h, p) => DecodedJwt(header = h, payload = p))
    }
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
