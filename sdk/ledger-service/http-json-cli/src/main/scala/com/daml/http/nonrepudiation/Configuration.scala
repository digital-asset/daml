// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.nonrepudiation

import java.nio.file.Path

import scala.util.{Failure, Success, Try}

sealed abstract class Configuration[F[_]] {
  def certificateFile: F[Path]
  def privateKeyFile: F[Path]
  def privateKeyAlgorithm: F[String]
}

object Configuration {

  type Id[X] = X

  final case class Validated(
      certificateFile: Path,
      privateKeyFile: Path,
      privateKeyAlgorithm: String,
  ) extends Configuration[Id]

  final case class Cli(
      certificateFile: Option[Path],
      privateKeyFile: Option[Path],
      privateKeyAlgorithm: Option[String],
  ) extends Configuration[Option] {
    lazy val validated: Try[Option[Validated]] =
      (certificateFile, privateKeyFile, privateKeyAlgorithm) match {
        case (None, None, None) =>
          Success(None)
        case (Some(cf), Some(kf), Some(ka)) =>
          Success(Some(Validated(cf, kf, ka)))
        case _ =>
          Failure(validationError())
      }
  }

  object Cli {

    def apply(
        certificateFile: Path,
        privateKeyFile: Path,
        privateKeyAlgorithm: String,
    ): Cli = Cli(Some(certificateFile), Some(privateKeyFile), Some(privateKeyAlgorithm))

    val Empty: Cli = Cli(None, None, None)

  }

  private def validationError(): IllegalArgumentException =
    new IllegalArgumentException("Either all or none of the non-repudiation options must be passed")

}
