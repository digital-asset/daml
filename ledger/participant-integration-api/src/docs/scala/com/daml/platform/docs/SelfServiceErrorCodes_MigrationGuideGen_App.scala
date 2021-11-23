// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import java.nio.charset.StandardCharsets

import com.daml.error.{Description, ErrorCategory, Resolution, RetryStrategy}
import io.circe.{Json, JsonObject, ParsingFailure, yaml}
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO error codes: Delete it once final version of migration guide is ready.
/** How to update self-service error codes migration guide table:
  * 1. Make appropriate changes to self-service-error-codes-migration.yml.
  * 2. Run this app to generate reST table in stdout.
  * 3. Paste that table into docs/source/error-codes/self-service/index.rst.
  */
object SelfServiceErrorCodes_MigrationGuideGen_App {
  def main(args: Array[String]): Unit = {

    println("\n\n########################## LEDGER API")
    println(
      genStatusCodeMigrationTableText(
        "com/daml/platform/docs/self-service-error-codes-migration.yml"
      )
    )

    println("\n\n########################## SANDBOX classic")
    println(
      genStatusCodeMigrationTableText(
        "com/daml/platform/docs/self-service-error-codes-sandbox-classic-migration.yml"
      )
    )

    println("\n\n########################## KV (Daml Sandbox and VMBC)")
    println(
      genStatusCodeMigrationTableText(
        "com/daml/platform/docs/self-service-error-codes-kv-errors-migration.yml"
      )
    )

  }

  def genStatusCodeMigrationTableText(inputYamlResourcePath: String): String = {
    // Generation migration table
    val changes: Seq[Change] = parseErrorsYml(
      inputYamlResourcePath
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

    case class EndpointWithGrpcCode(serviceName: String, legacy_grpc_code: String)
    val affectedMap = mutable.Map[EndpointWithGrpcCode, mutable.ArrayBuffer[Array[String]]]()
    // Add all changed codes
    for {
      line <- tableLines if line(1) != line(2)
    } {
      val key = EndpointWithGrpcCode(line(0), line(1))
      if (!affectedMap.contains(key)) {
        affectedMap.put(key, new ArrayBuffer[Array[String]])
      }
      affectedMap.apply(key) += (line)
    }

    // Add unchanged codes if the there is at least one change for the corresponding (endpoint, old_code) pair
    for {
      line <- tableLines if line(1) == line(2)
    } {
      val key = EndpointWithGrpcCode(line(0), line(1))

      if (affectedMap.contains(key)) {
        affectedMap.apply(key) += (line)
      }
    }

    val linesForTable: Array[Array[String]] = affectedMap.values.flatten.toArray.sortBy(line =>
      (line(0), line(1), line(2), line(3), line(4))
    )

    linesForTable
      .groupBy(_(0))
      .toList
      .sortBy(_._1)
      .map { case (endpoint: String, rows: Array[Array[String]]) =>
        val rows2 = (rows.map(_.tail)).sortBy(line => (line(0), line(1), line(2), line(3)))
        val tableText = generateReStTable(
          rows2,
          header = Array(
            //        "Service endpoint",
            "gRPC status code\n" +
              "(before SDK 1.18)",
            "gRPC status code\n" +
              "(since SDK 1.18)",
            "Remarks",
            "Ledger API \n" +
              "error code id",
          ),
        )
        s"""|
            |$endpoint
            |^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            |""".stripMargin + "\n" + tableText

      }
      .mkString("\n\n")
  }

  def printOutAsCsvText(tableLines: Array[Array[String]]): Unit = {
    val csvLines: Array[String] = for {
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
    val table2d: Array[Array[String]] = header +: rows

    def padRightF(s: String, length: Int): String = {
      require(s.length <= length, s"String |$s| is longer then target length: $length")
      s + (" " * (length - s.length))
    }

    case class Cell(lines: mutable.ArrayBuffer[String]) {
      def maxLength(): Int = {
        lines.map(_.length).maxOption.getOrElse(0)
      }

      def height(): Int = {
        lines.length
      }

      def padRight(targetLength: Int, targetHeight: Int): Cell = {
        require(
          targetHeight >= this.height(),
          s"Cell ${this} has more lines than the max lines expected: ${targetHeight}!",
        )
        val newLines = (1 to (targetHeight - this.height())).map(_ => padRightF("", targetLength))
        lines.mapInPlace(line => padRightF(line, targetLength))
        lines.addAll(newLines)
        this
      }
    }

    def cellRowToTextRows(row: Array[Cell]): String = {
      val table = row.map(_.lines.toArray).transpose
      table.map(row => row.mkString("|", "|", "|")).mkString("\n")
    }

    val table: Array[Array[Cell]] = table2d
      .map(line =>
        line.map { cell: String =>
          val linesInCell: Array[String] = cell.split("\n")
          Cell(lines = mutable.ArrayBuffer.from(linesInCell))
        }
      )

    val columnMaxLengths: Array[Int] =
      table
        .filter(_.length != 1)
        .transpose
        .map(column => column.map(_.maxLength()).max)
        .map(_ + 1)

    val totalLineWidthExclusiveOfOuterBorders = columnMaxLengths.sum + (columnMaxLengths.length - 1)

    val textTableRows: Array[String] = table.map { row =>
      if (row.length == 1) {
        // If row has only one element it is the endpoint name that will span full table width.
        row.head
          .padRight(targetLength = totalLineWidthExclusiveOfOuterBorders, row.head.height())
          .lines
          .map(l => s"|${l}|")
          .mkString("\n")
      } else {
        val maxHeightInThisRow = row.map(_.height()).max
        val paddedCells: Array[Cell] = row.zipWithIndex
          .map { case (cell, index) =>
            cell.padRight(columnMaxLengths(index), maxHeightInThisRow)
          }
        cellRowToTextRows(paddedCells)
      }

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

  // Generate error categories table
  def genErrorCategoryTableText(): Unit = {
    case class ErrorCategoryDoc(
        description: Option[String],
        resolution: Option[String],
        retryStrategy: Option[String],
    )

    def handleErrorCategoryAnnotations(errorCategory: ErrorCategory): ErrorCategoryDoc = {
      import scala.reflect.runtime.{universe => ru}

      val descriptionTypeName = classOf[Description].getTypeName.replace("$", ".")
      val resolutionTypeName = classOf[Resolution].getTypeName.replace("$", ".")
      val retryStrategyTypeName = classOf[RetryStrategy].getTypeName.replace("$", ".")

      val runtimeMirror: ru.Mirror = ru.runtimeMirror(this.getClass.getClassLoader)

      def isAnnotation(annotation: ru.Annotation, typeName: String): Boolean =
        annotationTypeName(annotation) == typeName

      def annotationTypeName(annotation: ru.Annotation) =
        annotation.tree.tpe.toString

      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      def parseAnnotationValue(tree: ru.Tree): String = {
        try {
          Seq(1).map(
            tree.children(_).asInstanceOf[ru.Literal].value.value.asInstanceOf[String]
          ) match {
            case s :: Nil => s.stripMargin
            case _ => sys.exit(1)
          }
        } catch {
          case x: RuntimeException =>
            println(
              "Failed to process description (description needs to be a constant-string. i.e. don't apply stripmargin here ...): " + tree.toString
            )
            throw x
        }
      }

      val mirroredType = runtimeMirror.reflect(errorCategory)
      val annotations: Seq[ru.Annotation] = mirroredType.symbol.annotations

      var description: Option[String] = None
      var resolution: Option[String] = None
      var retryStrategy: Option[String] = None
      annotations.foreach { annotation =>
        if (isAnnotation(annotation, descriptionTypeName)) {
          description = Option(parseAnnotationValue(annotation.tree))
        } else if (isAnnotation(annotation, resolutionTypeName)) {
          resolution = Option(parseAnnotationValue(annotation.tree))
        } else if (isAnnotation(annotation, retryStrategyTypeName)) {
          retryStrategy = Option(parseAnnotationValue(annotation.tree))
        } else {
          ???
        }
      }

      ErrorCategoryDoc(
        description = description,
        resolution = resolution,
        retryStrategy = retryStrategy,
      )

    }

    val errorCategoriesTable: Array[Array[String]] = ErrorCategory.all.map { cat: ErrorCategory =>
      val doc = handleErrorCategoryAnnotations(cat)
      println(doc)
      val intValue: String = cat.asInt.toString
      val grpcCode: String = cat.grpcCode.fold("N/A")(_.toString)
      val name: String = cat.getClass.getSimpleName.replace("$", "")
      val logLevel: String = cat.logLevel.toString

      Array(
        name,
        intValue,
        grpcCode,
        doc.description.getOrElse("").replace("\n", " "),
        doc.resolution.getOrElse("").replace("\n", " "),
        doc.retryStrategy.getOrElse("").replace("\n", " "),
        logLevel,
      )

    }.toArray

    val errorCategoryTableText = generateReStTable(
      errorCategoriesTable,
      header = Array(
        "Error category",
        "Category id",
        "gRPC code",
        "Description",
        "Resolution",
        "Retry strategy",
        "Log level",
      ),
    )

    def genSubsectionsForErrorCategory(a: Array[String]) = {
      s"""${a(0)}
         |^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |    **Category id**: ${a(1)}
         |
         |    **gRPC status code**: ${a(2)}
         |
         |    **Default log level**: ${a(6)}
         |
         |    **Description**: ${a(3)}
         |
         |    **Resolution**: ${a(4)}
         |
         |    **Retry strategy**: ${a(5)}""".stripMargin
    }

    println(errorCategoriesTable.map(genSubsectionsForErrorCategory).mkString("\n\n\n"))

    println(errorCategoryTableText)
    println()
    println("Error category table done")
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
