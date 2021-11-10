// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import io.circe.{Json, JsonObject, ParsingFailure, yaml}
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.StandardCharsets

// TODO error codes: Delete it once final version of migration guide is ready.
/** How to update self-service error codes migration guide table:
  * 1. Make appropriate changes to self-service-error-codes-migration.yml.
  * 2. Run this app to generate reST table in stdout.
  * 3. Paste that table into docs/source/error-codes/self-service/index.rst.
  */
object SelfServiceErrorCodes_MigrationGuideGen_App {
  def main(args: Array[String]): Unit = {
    val changes: Seq[Change] = parseErrorsYml(
      "com/daml/platform/docs/self-service-error-codes-migration.yml"
    ).toList

    // Group changes by service
    val serviceToChangesMap: mutable.Map[Service, ArrayBuffer[Change0]] =
      mutable.Map.empty[Service, ArrayBuffer[Change0]]
    changes.foreach { change: Change =>
      if (!serviceToChangesMap.contains(change.service)) {
        serviceToChangesMap.put(change.service, new ArrayBuffer[Change0])
      }
      serviceToChangesMap.apply(change.service) ++= (change.changes)
    }
    val changesGroupedByService: Seq[(Service, ArrayBuffer[Change0])] = serviceToChangesMap.toList

    val tableLines: Array[Array[String]] = {
      val unsorted: Array[Array[String]] = for {
        (service, changes) <- changesGroupedByService.toArray
        serviceName = service.mkString
        change <- changes
      } yield {
        val comment = change.comments.getOrElse("").replace('\n', ' ')
        Array(
          serviceName,
          change.v1_line_string,
          change.v2_line_string,
          comment,
          change.self_service_error_code_id.getOrElse(
            throw new Exception(s"Missing self service code id for change: $change!")
          ),
        )
      }
      unsorted.sortBy(line => (line(0), line(1), line(2), line(3), line(4)))
    }

    printOutAsCsvText(tableLines)

    println()
    println()
    println()

    val tableLinesWithoutUnchangedErrorCodes = for {
      line <- tableLines if line(1) != line(2)
    } yield line

    val reStTable = generateReStTable(
      tableLinesWithoutUnchangedErrorCodes,
      header = Array(
        "Service method",
        "gRPC error code (legacy errors)",
        "gRPC error code (self-service errors)",
        "Remarks",
        "Sef-service error code id",
      ),
    )
    println(reStTable)
  }

  private def printOutAsCsvText(tableLines: Array[Array[String]]): Unit = {
    val csvLines = for {
      line <- tableLines
    } yield {
      line.mkString("|")
    }
    val csvLinesSorted = csvLines.distinct.sorted

    println("==============================")
    println("==============================")
    println("==============================")
    println(csvLinesSorted.mkString("\n"))
  }

  def generateReStTable(rows: Array[Array[String]], header: Array[String]): String = {
    val table: Array[Array[String]] = header +: rows

    val columnMaxLengths: Array[Int] =
      table.transpose.map(column => column.map(_.length).max).map(_ + 1)

    def padRight(s: String, length: Int): String = {
      require(s.length <= length, s"String |$s| is longer then target length: $length")
      s + (" " * (length - s.length))
    }

    val textTableRows: Array[String] = table.map { row =>
      val textTableRow: String = row.zipWithIndex
        .map { case (entry, index) =>
          padRight(entry, columnMaxLengths(index))
        }
        .mkString("|", "|", "|")
      textTableRow
    }

    val textTableRowSeparator = columnMaxLengths.map("-" * _).mkString("+", "+", "+")
    val textTableHeaderRowSeparator = columnMaxLengths.map("=" * _).mkString("+", "+", "+")

    val contentRows = textTableRows.tail
    val headerRow = textTableRows.head
    val textTable = {
      textTableRowSeparator + "\n" + headerRow + "\n" + {
        contentRows.mkString(
          textTableHeaderRowSeparator + "\n",
          "\n" + textTableRowSeparator + "\n",
          "\n" + textTableRowSeparator,
        )
      }
    }

    textTable
  }

  def parseErrorsYml(resourcePath: String): Seq[Change] = {
    val changesDocument: JsonObject = readYmlFileAsJsonObject(resourcePath)
    val changesToCommentsMap: mutable.Map[Change, mutable.Set[String]] =
      mutable.Map.empty[Change, mutable.Set[String]]

    changesDocument.toList.foreach {
      case (errorFactories_methodName: String, changeEntryJson: Json) =>
        val changeEntryJsonObject: JsonObject = changeEntryJson.asObject.get
        // "change"
        val grpcOldAndNewErrorCode: Seq[Json] =
          changeEntryJsonObject.apply("change").get.asArray.get.toList
        val oldGrpcCode = grpcOldAndNewErrorCode(0).asString.get
        val newGrpcCode = grpcOldAndNewErrorCode(1).asString.get
        // "services"
        val affectedServicesJson: Seq[(String, Json)] =
          changeEntryJsonObject.apply("services").get.asObject.get.toList
        val affectedServices: Seq[Service] = affectedServicesJson.flatMap {
          case (serviceName: String, methodsJson: Json) =>
            val methods: Seq[String] = methodsJson.asArray.get.map(_.asString.get).toList
            val services =
              for (m <- methods)
                yield Service(name = serviceName, method = m)
            services
        }
        // "changeExplanation"
        val changeExplanation: String =
          changeEntryJsonObject("changeExplanation").fold(errorFactories_methodName)(_.asString.get)
        // "selfServiceErrorCodeId"
        val selfServiceCodeId: String =
          changeEntryJsonObject("selfServiceErrorCodeId").fold("")(_.asString.get)

        for {
          service: Service <- affectedServices
        } {
          val change = Change(
            service = service,
            v1_grpc_code = oldGrpcCode,
            v2_grpc_code = newGrpcCode,
            comment = Some(changeExplanation),
            self_service_error_code_id = Some(selfServiceCodeId),
          )

          if (!changesToCommentsMap.contains(change)) {
            changesToCommentsMap.put(change, new mutable.HashSet[String]())
          }
          changesToCommentsMap.apply(change) += (changeExplanation)
        }
    }

    changesToCommentsMap.map { case (change: Change, comments: mutable.Set[String]) =>
      require(
        comments.size == 1,
        s"Actual length: ${comments.size}, expected: 1. Value: ${comments}",
      )
      change.withComment(comments.mkString(" "))
    }.toList
  }

  private def readYmlFileAsJsonObject(resourcePath: String): JsonObject = {
    val in = getClass.getClassLoader.getResourceAsStream(resourcePath);
    val content: String = IOUtils.toString(in, StandardCharsets.UTF_8)
    val jsonE: Either[ParsingFailure, Json] = for {
      json <- yaml.parser.parse(content)
    } yield json
    val json: Json = jsonE match {
      case Left(e) => throw e
      case Right(json) => json
    }
    json.asObject.get
  }
}

case class Service(name: String, method: String) {
  def mkString: String = s"$name.$method"
}

object Service {
  def apply(name: String, methods: Seq[String]): Seq[Service] = {
    for {
      m <- methods
    } yield Service(name, m)
  }

}

case class Change0(
    v1_grpc_codes: List[String],
    v2_grpc_codes: List[String],
    self_service_error_code_id: Option[String],
    comments: Option[String],
) {
  def v1_line_string: String = v1_grpc_codes.mkString(", ")

  def v2_line_string: String = v2_grpc_codes.mkString(", ")
}

case class Change(service: Service, changes: List[Change0]) {

  def withComment(s: String): Change = {
    val changes2 = changes.map(_.copy(comments = Some(s)))
    copy(changes = changes2)
  }

}

object Change {
  def apply(
      service: Service,
      v1_grpc_code: String,
      v2_grpc_code: String,
      self_service_error_code_id: Option[String],
      comment: Option[String],
  ): Change = {
    val c0 = Change0(List(v1_grpc_code), List(v2_grpc_code), self_service_error_code_id, comment)
    new Change(service, List(c0))
  }
}
