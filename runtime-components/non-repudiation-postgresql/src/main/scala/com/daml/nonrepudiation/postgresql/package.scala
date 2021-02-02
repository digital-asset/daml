// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, PublicKey}

import doobie.util.log.{ExecFailure, LogHandler, ProcessingFailure, Success}
import doobie.util.{Get, Put}
import org.slf4j.Logger

import scala.collection.compat.immutable.ArraySeq

package object postgresql {

  implicit def getBytes[Bytes <: ArraySeq[Byte]]: Get[Bytes] =
    Get[Array[Byte]].map(ArraySeq.unsafeWrapArray(_).asInstanceOf[Bytes])

  implicit def putBytes[Bytes <: ArraySeq[Byte]]: Put[Bytes] =
    Put[Array[Byte]].contramap(_.unsafeArray)

  implicit val getAlgorithmString: Get[AlgorithmString] =
    Get[String].map(AlgorithmString.wrap)

  implicit val putAlgorithmString: Put[AlgorithmString] =
    Put[String].contramap(identity)

  implicit val getCommandIdString: Get[CommandIdString] =
    Get[String].map(CommandIdString.wrap)

  implicit val putCommandIdString: Put[CommandIdString] =
    Put[String].contramap(identity)

  implicit val getPublicKey: Get[PublicKey] =
    Get[Array[Byte]].map { bytes =>
      val keySpec = new X509EncodedKeySpec(bytes)
      val keyFactory = KeyFactory.getInstance("RSA")
      keyFactory.generatePublic(keySpec)
    }

  implicit val putPublicKey: Put[PublicKey] =
    Put[Array[Byte]].contramap(_.getEncoded)

  def slf4jLogHandler(logger: Logger): LogHandler =
    LogHandler {
      case Success(s, a, e1, e2) =>
        logger.debug(s"""Successful Statement Execution:
                          |
                          |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                          |
                          | arguments = [${a.mkString(", ")}]
                          |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (${(e1 + e2).toMillis.toString} ms total)
          """.stripMargin)
      case ProcessingFailure(s, a, e1, e2, t) =>
        logger.error(s"""Failed Resultset Processing:
                            |
                            |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                            |
                            | arguments = [${a.mkString(", ")}]
                            |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (failed) (${(e1 + e2).toMillis.toString} ms total)
                            |   failure = ${t.getMessage}
          """.stripMargin)
      case ExecFailure(s, a, e1, t) =>
        logger.error(s"""Failed Statement Execution:
                            |
                            |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                            |
                            | arguments = [${a.mkString(", ")}]
                            |   elapsed = ${e1.toMillis.toString} ms exec (failed)
                            |   failure = ${t.getMessage}
          """.stripMargin)
    }

}
