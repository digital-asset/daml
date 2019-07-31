// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.io.{File, FileNotFoundException, FileOutputStream}
import java.security.{Key, KeyPair}

import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket

import scala.util.{Failure, Success, Try}

object RsaKeysGenerator {
  def generate(destination: domain.KeyPair): Try[domain.KeyPair] =
    for {
      keyPair <- generateKeyPair()
      publicKeyFile <- writeKey(keyPair.getPublic, destination.publicKey)
      privateKeyFile <- writeKey(keyPair.getPrivate, destination.privateKey)
    } yield domain.KeyPair(publicKey = publicKeyFile, privateKey = privateKeyFile)

  private def generateKeyPair(): Try[KeyPair] =
    Try {
      val kpg = java.security.KeyPairGenerator.getInstance("RSA")
      kpg.initialize(2048)
      Option(kpg.generateKeyPair())
    } flatMap {
      case Some(x) => Success(x)
      case None => Failure(new IllegalStateException("Cannot generate RSA key pair, null returned"))
    }

  private def writeKey(key: Key, file: File): Try[File] =
    bracket(Try(new FileOutputStream(file)))(close).flatMap { os =>
      Try(os.write(key.getEncoded)).flatMap(_ => exists(file))
    }

  private def close(a: FileOutputStream): Try[Unit] = Try(a.close())

  private def exists(f: File): Try[File] =
    for {
      b <- Try(f.exists())
      x <- if (b) Success(f) else Failure(new FileNotFoundException(f.getAbsolutePath))
    } yield x
}
