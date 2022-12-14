// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.metering

import com.daml.platform.apiserver.meteringreport.HmacSha256.Key
import com.daml.platform.apiserver.meteringreport.JcsSigner.VerificationStatus
import com.daml.platform.apiserver.meteringreport.{JcsSigner, MeteringReportKey}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.StreamConverters._
import scala.util.Try

/** The metering report validation app can be used to verify that a provided metering report is consistent with
  * a `check` section within the report. When the report is created the check section is populated with a digest
  * and a scheme that was used in the creation of that digest.  Different schemes may be used by different
  * schemes (e.g. community verses enterprise) and for different point in time.
  *
  * The app takes as parameters a directory that contains one file for each supported scheme and the report
  * file to be verified. The scheme files need to contain JSON with fields for the scheme name digest algorithm
  * and encoded key. For example:
  *
  *   {
  *    "scheme": "community-2021",
  *    "algorithm": "HmacSHA256",
  *    "encoded": "ifKEd83-fAvOBTXnGjIVfesNzmWFKpo_35zpUnXEsg="
  *   }
  *
  * Verification works by inspecting the scheme referenced in the report file and then checking that the
  * recalculated digest matches that provided in the file.
  *
  * Usage: metering-verification-app <directory> <report>
  */
object Main {

  trait ExitCode {
    def code: String
    def detail: String
    final def message = s"$code: $detail"
  }

  type ExitCodeOr[T] = Either[ExitCode, T]

  abstract class Code(val code: String) extends ExitCode
  case class CodeDetail(code: String, detail: String) extends ExitCode

  val OK: ExitCode = CodeDetail("OK", "The digest is as expected")

  case class ErrUsage(app: String) extends Code("ERR_USAGE") {
    def detail = s"Usage: $app <directory> <report>"
  }

  case class NotDirectory(dir: String) extends Code("ERR_NOT_DIRECTORY") {
    def detail = s"The passed directory is not a valid directory: $dir"
  }

  case class NotKeyFile(path: Path, t: Throwable) extends Code("ERR_NOT_KEY_FILE") {
    def detail = s"Unable to parse metering report key from key file: $path [${t.getMessage}]"
  }

  case class NoKeys(dir: String) extends Code("ERR_NO_KEYS") {
    def detail = s"No keys found in the key directory: $dir"
  }

  case class NotFile(report: String) extends Code("ERR_NO_REPORT") {
    def detail = s"The passed report is not a file: $report"
  }

  case class FailedToReadReport(report: String, t: Throwable) extends Code("ERR_READING_REPORT") {
    def detail = s"Failed to read the participant report: $report [${t.getMessage}]"
  }

  case class FailedVerification(status: VerificationStatus)
      extends Code("ERR_FAILED_VERIFICATION") {
    def detail = s"Report verification failed with status: $status"
  }

  def main(args: Array[String]): Unit = {
    val result = for {
      keyReport <- checkUsage(args)
      (keyDir, reportFile) = keyReport
      report <- readReport(reportFile)
      keys <- readKeys(keyDir)
      _ <- verifyReport(report, keys)
    } yield {
      ()
    }
    result match {
      case Right(_) =>
        System.out.println(OK.message)
        System.exit(0)
      case Left(exitCode) =>
        System.err.println(exitCode.message)
        System.exit(1)
    }
  }

  private def checkUsage(args: Array[String]): ExitCodeOr[(String, String)] = {
    args.toList match {
      case List(dir, report) => Right((dir, report))
      case _ => Left(ErrUsage("metering-verification-app"))
    }
  }

  private def readKey(keyPath: Path): ExitCodeOr[Key] = {
    Try(MeteringReportKey.assertParseKey(Files.readAllBytes(keyPath)))
      .fold(t => Left(NotKeyFile(keyPath, t)), Right(_))
  }

  private def readKeys(keyDir: String): ExitCodeOr[Map[String, Key]] = {

    import scalaz._
    import scalaz.syntax.traverse._
    import std.either._
    import std.list._

    val dir = Path.of(keyDir)
    if (Files.isDirectory(dir)) {
      for {
        keys <- Files.list(dir).toScala(List).traverse(readKey)
        _ <- if (keys.isEmpty) Left(NoKeys(keyDir)) else Right(())
      } yield {
        keys.map(k => k.scheme -> k).toMap
      }
    } else {
      Left(NotDirectory(keyDir))
    }
  }

  private def readReport(reportFile: String): ExitCodeOr[String] = {
    val path = Path.of(reportFile)
    if (Files.isRegularFile(path)) {
      Try(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
        .fold(t => Left(FailedToReadReport(reportFile, t)), Right(_))
    } else {
      Left(NotFile(reportFile))
    }
  }

  private def verifyReport(json: String, keys: Map[String, Key]): ExitCodeOr[Unit] = {
    JcsSigner.verify(json, keys.get) match {
      case VerificationStatus.Ok => Right(())
      case status => Left(FailedVerification(status))
    }
  }

}
