// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.io.File

import scala.util.{Failure, Success}

object Main {

  object ErrorCodes {
    val InvalidUsage = 100
    val GenerateKeysError = 101
    val GenerateJwtError = 102
  }

  final case class Config(
      generateKeys: Option[GenerateKeys] = None,
      generateJwt: Option[GenerateJwt] = None,
  )

  final case class GenerateKeys(name: Option[String] = None)
  final case class GenerateJwt(publicKey: Option[File] = None, privateKey: Option[File] = None)

  def main(args: Array[String]): Unit = {
    parseConfig(args) match {
      case Some(Config(Some(GenerateKeys(Some(name))), None)) =>
        RsaKeysGenerator.generate(keyPair(name)) match {
          case Success(a) =>
            print(s"Generated keys: ${a: domain.KeyPair[File]}")
          case Failure(e) =>
            e.printStackTrace()
            sys.exit(ErrorCodes.GenerateKeysError)
        }
      case Some(Config(None, Some(GenerateJwt(Some(publicKey @ _), Some(privateKey @ _))))) =>
        JwtGenerator.generate match {
          case Success(a) =>
            println(s"Generated JWT: $a")
          case Failure(e) =>
            e.printStackTrace()
            sys.exit(ErrorCodes.GenerateJwtError)
        }
      case Some(_) =>
        configParser.displayToErr(configParser.usage)
        sys.exit(ErrorCodes.InvalidUsage)
      case None =>
        // error is printed out by scopt... yeah I know... why?
        sys.exit(ErrorCodes.InvalidUsage)
    }
  }

  private def keyPair(name: String) =
    domain.KeyPair(
      publicKey = new File(s"./$name.pub").getAbsoluteFile,
      privateKey = new File(s"./$name.pvt").getAbsoluteFile,
    )

  private def parseConfig(args: collection.Seq[String]): Option[Config] = {
    configParser.parse(args, Config())
  }

  private val configParser = new scopt.OptionParser[Config]("ledger-service-jwt") {
    cmd("generate-keys")
      .text("generate public and private keys")
      .action((_, c) => c.copy(generateKeys = Some(GenerateKeys())))
      .children(
        opt[String]("name")
          .required()
          .valueName("<keys name>")
          .action((x, c) => c.copy(generateKeys = c.generateKeys.map(_.copy(name = Some(x)))))
      )

    cmd("generate-jwt")
      .text("generate JWT")
      .action((_, c) => c.copy(generateJwt = Some(GenerateJwt())))
      .children(
        opt[File]("public-key")
          .required()
          .valueName("<public key file path>")
          .action((x, c) => c.copy(generateJwt = c.generateJwt.map(_.copy(publicKey = Some(x))))),
        opt[File]("private-key")
          .required()
          .valueName("<private key file path>")
          .action((x, c) => c.copy(generateJwt = c.generateJwt.map(_.copy(privateKey = Some(x))))),
      )
  }
}
