// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.io.{File, FileNotFoundException, FileOutputStream}

import com.daml.lf.data.TryOps.Bracket.bracket
import scalaz.std.option._
import scalaz.syntax.applicative._

import scala.util.{Failure, Success, Try}

object RsaKeysGenerator {

  private val keySize: Int = 2048

  def generate(destination: domain.KeyPair[File]): Try[domain.KeyPair[File]] =
    for {
      keyPair <- generate_(): Try[domain.KeyPair[Array[Byte]]]
      publicKeyFile <- writeKey(keyPair.publicKey, destination.publicKey)
      privateKeyFile <- writeKey(keyPair.privateKey, destination.privateKey)
    } yield domain.KeyPair(publicKey = publicKeyFile, privateKey = privateKeyFile)

  def generate(): Try[domain.KeyPair[Seq[Byte]]] =
    generate_().map(k => k.map(as => as.toSeq))

  private def generate_(): Try[domain.KeyPair[Array[Byte]]] =
    Try {
      val kpg = java.security.KeyPairGenerator.getInstance("RSA")
      kpg.initialize(keySize)
      Option(kpg.generateKeyPair()).flatMap(domainKeyPair)
    } flatMap {
      case Some(x) => Success(x)
      case None => Failure(new IllegalStateException("Cannot generate RSA key pair, null returned"))
    }

  private def domainKeyPair(k: java.security.KeyPair): Option[domain.KeyPair[Array[Byte]]] =
    ^(Option(k.getPublic), Option(k.getPrivate)) { (pub, pvt) =>
      domain.KeyPair(publicKey = pub.getEncoded, privateKey = pvt.getEncoded)
    }

  private def writeKey(key: Array[Byte], file: File): Try[File] =
    bracket(Try(new FileOutputStream(file)))(close).flatMap { ostream =>
      for {
        encoder <- Try(java.util.Base64.getEncoder)
        _ <- Try(ostream.write(encoder.encode(key)))
        _ <- exists(file)
      } yield file
    }

  private def close(a: FileOutputStream): Try[Unit] = Try(a.close())

  private def exists(f: File): Try[File] =
    for {
      b <- Try(f.exists())
      x <- if (b) Success(f) else Failure(new FileNotFoundException(f.getAbsolutePath))
    } yield x
}
