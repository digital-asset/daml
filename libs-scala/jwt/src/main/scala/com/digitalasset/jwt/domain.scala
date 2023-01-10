// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scalaz.syntax.applicative._
import scalaz.{Applicative, Traverse}

package domain {

  final case class KeyPair[A](publicKey: A, privateKey: A)

  final case class Jwt(value: String)

  final case class DecodedJwt[A](header: A, payload: A)

  object KeyPair {
    implicit val traverseInstance: Traverse[KeyPair] = new Traverse[KeyPair] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: KeyPair[A]
      )(f: A => G[B]): G[KeyPair[B]] = {
        ^(f(fa.publicKey), f(fa.privateKey))(KeyPair(_, _))
      }
    }
  }

  object DecodedJwt {
    implicit val traverseInstance: Traverse[DecodedJwt] = new Traverse[DecodedJwt] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: DecodedJwt[A]
      )(f: A => G[B]): G[DecodedJwt[B]] = {
        ^(f(fa.header), f(fa.payload))((h, p) => DecodedJwt(header = h, payload = p))
      }
    }
  }
}
