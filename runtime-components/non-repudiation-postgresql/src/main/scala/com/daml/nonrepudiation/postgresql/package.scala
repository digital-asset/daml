// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}

import cats.effect.{ContextShift, IO}
import com.daml.nonrepudiation.resources.HikariTransactorResourceOwner
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ResourceOwnerFactories}
import doobie.hikari.HikariTransactor
import doobie.util.{Get, Put, Read}

import scala.collection.immutable.ArraySeq
import scala.concurrent.ExecutionContext

package object postgresql {

  def createTransactor[Context: HasExecutionContext](
      jdbcUrl: String,
      username: String,
      password: String,
      maxPoolSize: Int,
      factory: ResourceOwnerFactories[Context],
  ): AbstractResourceOwner[Context, HikariTransactor[IO]] = {
    implicit val shift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
    HikariTransactorResourceOwner(factory)(jdbcUrl, username, password, maxPoolSize)
  }

  implicit def getBytes[Bytes <: ArraySeq.ofByte]: Get[Bytes] =
    Get[Array[Byte]].map(ArraySeq.unsafeWrapArray(_).asInstanceOf[Bytes])

  implicit def putBytes[Bytes <: ArraySeq.ofByte]: Put[Bytes] =
    Put[Array[Byte]].contramap(_.unsafeArray)

  implicit val getAlgorithmString: Get[AlgorithmString] =
    Get[String].map(AlgorithmString.wrap)

  implicit val putAlgorithmString: Put[AlgorithmString] =
    Put[String].contramap(identity)

  implicit val getCommandIdString: Get[CommandIdString] =
    Get[String].map(CommandIdString.wrap)

  implicit val putCommandIdString: Put[CommandIdString] =
    Put[String].contramap(identity)

  implicit val getCertificate: Get[X509Certificate] =
    Get[Array[Byte]].map { bytes =>
      val factory = CertificateFactory.getInstance("X.509")
      factory.generateCertificate(new ByteArrayInputStream(bytes)).asInstanceOf[X509Certificate]
    }

  implicit val readCertificate: Read[X509Certificate] =
    Read.fromGet[X509Certificate]

  implicit val putCertificate: Put[X509Certificate] =
    Put[Array[Byte]].contramap(_.getEncoded)

}
