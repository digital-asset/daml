// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import buf.alpha.image.v1.image.Image
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.tests.manual.ProtobufCompatibilityTest.*
import com.digitalasset.canton.version.{AlphaProtoVersion, ReleaseVersion}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertions, Inside, LoneElement}
import scalapb.options.ScalapbProto

import java.nio.file.Files
import scala.sys.process.ProcessLogger

private object ProtobufCompatibilityTest {
  import Assertions.*
  val protobufImageFileName: String = "protobuf_image.bin.gz"
  val pvAnnotatedProtos = "protocol_version_annotated_protobufs.json"

  def runCommand(command: String, args: String*): Unit = {
    val processLogger = new BufferedProcessLogger()
    val exitCode = runCommandWithLogger(processLogger, command, args*)
    if (exitCode != 0) fail(processLogger.output(linePrefix = s"$command: "))
  }

  def runCommandWithLogger(logger: ProcessLogger, command: String, args: String*): Int =
    sys.process.Process(command, args.toSeq).!(logger)

  private val alphaProtoVersionName = classOf[AlphaProtoVersion].getName

  def removeAlphaProtoVersionMessagesFromImage(source: File, target: File): Unit = {
    // read the file
    val in = source.newGzipInputStream()
    val imageBytes = in.readAllBytes()
    in.close()

    // filter out messages tagged with AlphaProtoVersion
    val img = Image.parseFrom(imageBytes)
    val updatedImgFiles = img.file.map { imgFile =>
      // first identify messages that are tagged with AlphaProtoVersion
      val (nonAlphaMessages, alphaMessages) = imgFile.messageType.partition(
        _.options.forall(
          _.extension(ScalapbProto.message).forall(
            !_.companionExtends.contains(alphaProtoVersionName)
          )
        )
      )

      // If there are alpha messages, we also have to remove service methods that use the alpha messages,
      // otherwise the image is inconsisent with services pointing to messages that don't exist in the image.

      // The input and output type of methods convert the message names to the format .package.name.
      // The leading dot signals that they are used within the same proto file.
      // We currently don't detect a service method that uses a message tagged as alpha from a different proto file
      // as the request or the response type.
      // However, buf will fail regardless, because the post-processed image isn't consistent anymore.

      // the method's input/output type is in the format: .$package.$name
      val alphas = alphaMessages.map(desc => s".${imgFile.getPackage}.${desc.getName}").toSet

      val potentiallyUpdatedServices = if (alphas.nonEmpty) {
        imgFile.service.map(svc =>
          svc.copy(method = svc.method.filter { m =>
            // only retain methods that don't use messages as request/input or response/output that are tagged as alpha messages
            m.inputType.forall(!alphas.contains(_)) && m.outputType.forall(!alphas.contains(_))
          })
        )

      } else imgFile.service

      imgFile.copy(messageType = nonAlphaMessages, service = potentiallyUpdatedServices)
    }

    val updatedImg = img.copy(file = updatedImgFiles)

    // write the same file again
    val os = target.newGzipOutputStream()
    updatedImg.writeTo(os)
    os.finish()
  }
}

final class ProtobufCompatibilityWriterTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with LoneElement
    with S3Synchronization {

  "buf" should {

    // the protobuf image will be uploaded automatically during a release, via publish-data-continuity-dumps-to-s3.sh
    "generate the protobuf image for the data continuity dump" in {
      val dumpDir = DataContinuityTest.baseDbDumpPath / ReleaseVersion.current.fullVersion
      Files.createDirectories(dumpDir.path)

      // proto snapshot
      val protobufSnapshotForVersion = dumpDir.toTempFile(protobufImageFileName)

      if (protobufSnapshotForVersion.file.exists) {
        fail(s"Target file $protobufSnapshotForVersion already exists.")
      }

      runCommand("buf", "build", "-o", protobufSnapshotForVersion.path.toFile.getAbsolutePath)

      ProtobufCompatibilityTest.removeAlphaProtoVersionMessagesFromImage(
        source = protobufSnapshotForVersion.file,
        target = protobufSnapshotForVersion.file,
      )
    }
  }
}

final class ProtobufCompatibilityReaderTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with LoneElement
    with S3Synchronization {

  // when adding an exception, replace `:<LINE>:<COLUMN>:` with `:` in the error message. For example:
  //
  // original buf error messages:
  // my.proto:17:5:Field "3" with name "bar" on message "Foo" changed type from "bytes" to "string".
  //
  // modified message:
  // my.proto:Field "3" with name "bar" on message "Foo" changed type from "bytes" to "string".
  //         ^ notice the removed line and column numbers
  val acceptedBreakingChanges = Seq(
    // Contract id recomputation had to be removed
    """com/digitalasset/canton/admin/participant/v30/acs_import.proto:Previously present enum value "3" on enum "ContractImportMode" was deleted.""",
    """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present field "3" with name "allow_contract_id_suffix_recomputation" on message "ImportAcsOldRequest" was deleted.""",
    """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present field "1" with name "contract_id_mapping" on message "ImportAcsOldResponse" was deleted.""",
    """com/digitalasset/canton/admin/participant/v30/participant_repair_service.proto:Previously present field "1" with name "contract_id_mappings" on message "ImportAcsResponse" was deleted.""",
    // Internal classes that should have been marked as alpha/unsable
    """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "3" with name "message_id" on message "OrderingRequest" changed type from "bytes" to "string".""",
    """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "payload" on message "OrderingRequest" changed cardinality from "optional with explicit presence" to "optional with implicit presence".""",
    """com/digitalasset/canton/synchronizer/sequencing/sequencer/bftordering/v30/bft_ordering_service.proto:Field "4" with name "payload" on message "OrderingRequest" changed type from "message" to "bytes".""",
  )

  "protobuf" should {

    // Test against previous patch versions of the current and the previous minor version
    val versionsToTest =
      Option(ReleaseVersion.current)
        .filter(_.patch > 0)
        .map(_.majorMinor)
        .toList :+
        (ReleaseVersion.current.major, ReleaseVersion.current.minor - 1)

    versionsToTest.foreach { case majorMinor @ (major, minor) =>
      S3Dump.getDumpBaseDirectoriesForVersion(Some(majorMinor)).foreach { case (dumpRef, version) =>
        s"be compatible with $version" in {
          val protoImageFile = dumpRef.localDownloadPath / protobufImageFileName

          File.temporaryFile(suffix = protobufImageFileName) { protoImageFileWithoutAlphaMessages =>
            ProtobufCompatibilityTest.removeAlphaProtoVersionMessagesFromImage(
              source = protoImageFile,
              target = protoImageFileWithoutAlphaMessages,
            )
            // run the compatibility test against the buf image that doesn't contain alpha messages
            val processLogger = new BufferedProcessLogger()
            val _ = runCommandWithLogger(
              processLogger,
              "scripts/ci/buf-checks.sh",
              protoImageFileWithoutAlphaMessages.toJava.getAbsolutePath,
            )
            val errorOutput =
              processLogger.outputLines().map(_.replaceAll(raw".proto:\d+:\d+:", ".proto:"))

            val unacceptableBreakages = errorOutput.filter(!acceptedBreakingChanges.contains(_))
            if (unacceptableBreakages.nonEmpty) {
              fail(
                s"""Detected a backwards breaking change in a protobuf file.
                    |Check with release owners whether this breaking change is acceptable or not.
                    |If it IS acceptable, add the entire line (removing the line number + column) to `acceptedBreaking` in this test.
                    |
                    |Breaking changes:
                    |${unacceptableBreakages.mkString("\n")}
                    |""".stripMargin
              )
            }

            val superfluousExceptions = acceptedBreakingChanges.filter(!errorOutput.contains(_))
            if (superfluousExceptions.nonEmpty) {
              fail(
                s"""Some exceptions in `acceptedBreakingChanges` are not valid anymore. Remove them from the list.
                   |
                   |${superfluousExceptions.mkString("\n")}""".stripMargin
              )
            }
          }
          succeed
        }
      }
    }
  }
}
