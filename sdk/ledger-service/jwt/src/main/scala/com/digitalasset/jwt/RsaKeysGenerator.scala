// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.io.{File, FileNotFoundException, FileOutputStream}

import scalaz.std.option._
import scalaz.syntax.applicative._

import scala.util.{Failure, Success, Try, Using}

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
    Using(new FileOutputStream(file))(_.write(java.util.Base64.getEncoder.encode(key)))
      .flatMap(_ => exists(file))

  private def exists(f: File): Try[File] =
    for {
      b <- Try(f.exists())
      x <- if (b) Success(f) else Failure(new FileNotFoundException(f.getAbsolutePath))
    } yield x
}
