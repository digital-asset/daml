// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
import java.io.File
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.{Error => ArchiveError}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast

//import scala.collection.immutable.ListMap
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
//import scala.util.{Try, Success, Failure}
import scala.util.Try
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
//import cats.data.EitherT
import com.daml.error.DamlError

final case class CouldNotReadDar(path: String, err: ArchiveError)
    extends RuntimeException
    with Product
    with Serializable {
  val message: String = s"Error reading DAR from ${path}: ${err.msg}"
}

final case class ValidationFailed(err: DamlError)
    extends RuntimeException
    with Product
    with Serializable {
  val message: String = s"Validation failed: ${err.cause}"
}

case class UpgradeCheckMain () {}

object UpgradeCheckMain {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty
  val loggerFactory = NamedLoggerFactory.root
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  private def tryAll[A, B](t: Iterable[A], f: A => Try[B]): Try[Seq[B]] =
    Try(t.map(f(_).get).toSeq)

  private def decodeDar(path: String): Try[Dar[(Ref.PackageId, Ast.Package)]] = {
    logger.debug(s"Decoding DAR from ${path}")
    val result = DarDecoder.readArchiveFromFile(new File(path))
    result.left.map(CouldNotReadDar(path, _)).toTry
  }

  val validator = new PackageUpgradeValidator(
    getPackageMap = _ => Map.empty,
    getLfArchive = _ => _ => Future(None),
    loggerFactory = loggerFactory
  )

  def main(args: Array[String]): Unit = {
    logger.debug(s"Called UpgradeCheckMain with args: ${args.toSeq.mkString("\n")}")

    val res = for {
      dars <- tryAll(args, decodeDar(_))
      archives = for { dar <- dars; archive <- dar.all.toSeq } yield {
        logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.pkgNameVersion}")
        archive
      }
      validation = validator.validateUpgrade(archives.toList)
      _ <- Await.result(validation.value, Duration.Inf).left.map(ValidationFailed(_)).toTry
    } yield ()
    res.get
  }
}
