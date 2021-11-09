package com.daml.platform

import io.circe.{Json, JsonObject, ParsingFailure, yaml}
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.StandardCharsets

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

case class Change0(v1_grpc_codes: List[String], v2_grpc_codes: List[String], self_service_error_code_id: Option[String], comments: Option[String]) {
  //  def mkString: String = {
  //    val remarks = ""
  //    val v1 = v1_line_string
  //    val v2 = v2_line_string
  //    s"""$v1 -> $v2$remarks""".stripMargin
  //  }

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
  def apply(service: Service, v1_grpc_code: String, v2_grpc_code: String, self_service_error_code_id: Option[String], comment: Option[String]): Change = {
    val c0 = Change0(List(v1_grpc_code), List(v2_grpc_code), self_service_error_code_id, comment)
    new Change(service, List(c0))
  }
}

object PbatkoErrorsYml {
  def main(args: Array[String]): Unit = {
    val cs1: Seq[Change] = bar_submit()
    val cs2: Seq[Change] = parseErrorsYml()
    val all = (cs1 ++ cs2).distinct

    val serviceToChanges: mutable.Map[Service, ArrayBuffer[Change0]] = mutable.Map.empty[Service, ArrayBuffer[Change0]]

    all.foreach { change
    =>
      if (!serviceToChanges.contains(change.service)) {
        serviceToChanges.put(change.service, new ArrayBuffer[Change0])
      }
      serviceToChanges.apply(change.service).addAll(change.changes)
    }

    val all2: Seq[(Service, ArrayBuffer[Change0])] = serviceToChanges.toList


    val tableLines: Array[Array[String]] = {
      val unsorted: Array[Array[String]] = for {
        (s, changes) <- all2.toArray
        serviceName = s.mkString
        c <- changes
      } yield {
        val comment = c.comments.getOrElse("").replace('\n', ' ')
        Array(serviceName, c.v1_line_string, c.v2_line_string, comment, c.self_service_error_code_id.getOrElse(""))
      }
      unsorted.sortBy(line => (line(0), line(1), line(2), line(3), line(4)))
    }

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

    println()
    println()
    println()

    val tableLinesWithoutUnchangedErrorCodes = for {
      line <- tableLines if line(1) != line(2)
    } yield line

    val reStTable = generateReStTable(tableLinesWithoutUnchangedErrorCodes,
      header = Array(
        "Service method",
        "gRPC error code (legacy errors)",
        "gRPC error code (self-service errors",
        "Remarks",
        "Sef-service error code id",
      ))
    println(reStTable)
  }

  def generateReStTable(rows: Array[Array[String]], header: Array[String]): String = {
    val table: Array[Array[String]] = header +: rows

    val columnMaxLengths: Array[Int] = table.transpose.map(column => column.map(_.length).max).map(_ + 1)

    def padRight(s: String, length: Int): String = {
      require(s.length <= length, s"String |$s| is longer then target lenght: $length")
      s + (" " * (length - s.length))
    }

    val textTableRows: Array[String] = table.map { row =>
      val textTableRow: String = row.zipWithIndex.map { case (entry, index) =>
        padRight(entry, columnMaxLengths(index))
      }.mkString("|", "|", "|")
      textTableRow
    }

    val textTableRowSeparator = columnMaxLengths.map("-" * _).mkString("+", "+", "+")

    val textTable = textTableRows.mkString(textTableRowSeparator + "\n", "\n" + textTableRowSeparator + "\n", "\n" + textTableRowSeparator)

    textTable
  }


  def bar_submit(): Seq[Change] = {
    val json: JsonObject = readYmlFileAsJsonObject("pbatko/submit_pbatko.yml")
    val services = parseServices(json("service").get)
    val changes = parseChanges(json("changes").get)
    //    println(services)
    //    println(changes)
    //    println("111")
    for {
      s <- services
    } yield
      Change(
        service = s, changes = changes.toList
      )
  }

  def parseChanges(json: Json): Seq[Change0] = {
    val rawChanges: Seq[JsonObject] = json.asArray.get.map(_.asObject.get)
    rawChanges.map(parseChange)
  }

  def parseChange(json: JsonObject): Change0 = {
    val desc = json("desc").get.asString.get
    val fromRaw = json("from").get.asArray.get
    val toRaw = json("to").get.asArray.get

    val from_codes: Seq[String] = parseFromOrTo(fromRaw).map(_._1)
    val to = parseFromOrTo(toRaw)
    val to_codes = to.map(_._1)
    val comments = to.map { case (code, comment) => s"$code: $comment" }.mkString("\n")
    val comment: String = {
      s"""$desc
         |
         |$comments
         |""".stripMargin
    }

    Change0(
      v1_grpc_codes = from_codes.toList,
      v2_grpc_codes = to_codes.toList,
      comments = Some(comment),
      self_service_error_code_id = None,
    )
  }

  def parseFromOrTo(raw: Vector[Json]): Seq[(String, String)] = {
    raw.map {
      case json if json.isString => (json.asString.get, "")
      case json if json.isObject =>
        val h = json.asObject.get.toList.head
        (h._1, h._2.asString.get)
      case x => throw new Exception(x.toString())
    }
  }

  def parseServices(json: Json): Seq[Service] = {
    val x: Seq[(String, Json)] = json.asObject.get.toList
    val name = x.head._1
    val methods: Seq[String] = x.head._2.asArray.get.map(_.asString.get)
    Service(name, methods)
  }

  def parseErrorsYml(): Seq[Change] = {
    val changesDocument: JsonObject = readYmlFileAsJsonObject("pbatko/errors_pbatko.yml")
    val changesToCommentsMap: mutable.Map[Change, mutable.Set[String]] = mutable.Map.empty[Change, mutable.Set[String]]

    changesDocument.toList.foreach { case (errorFactories_methodName: String, changeEntryJson: Json) =>

      val changeEntryJsonObject: JsonObject = changeEntryJson.asObject.get
      // "change"
      val grpcOldAndNewErrorCode: Seq[Json] = changeEntryJsonObject.apply("change").get.asArray.get.toList
      val oldGrpcCode = grpcOldAndNewErrorCode(0).asString.get
      val newGrpcCode = grpcOldAndNewErrorCode(1).asString.get
      // "services"
      val affectedServicesJson: Seq[(String, Json)] = changeEntryJsonObject.apply("services").get.asObject.get.toList
      val affectedServices: Seq[Service] = affectedServicesJson.flatMap { case (serviceName: String, methodsJson: Json) =>
        val methods: Seq[String] = methodsJson.asArray.get.map(_.asString.get).toList
        val services = for (m <- methods)
          yield Service(name = serviceName, method = m)
        services
      }
      // "changeExplanation"
      val changeExplanation: String = changeEntryJsonObject("changeExplanation").fold(errorFactories_methodName)(_.asString.get)
      // "selfServiceErrorCodeId"
      val selfServiceCodeId: String = changeEntryJsonObject("selfServiceErrorCodeId").fold("")(_.asString.get)

      for {
        service: Service <- affectedServices
      } {
        val change = Change(
          service = service,
          v1_grpc_code = oldGrpcCode,
          v2_grpc_code = newGrpcCode,
          // Comments will be filled further down. Don't fill it now as
          comment = Some(changeExplanation),
          self_service_error_code_id = Some(selfServiceCodeId)
        )

        if (!changesToCommentsMap.contains(change)) {
          changesToCommentsMap.put(change, new mutable.HashSet[String]())
        }
        changesToCommentsMap.apply(change).addOne(changeExplanation)
      }
    }

    changesToCommentsMap.map { case (change: Change, comments: mutable.Set[String]) =>
      require(comments.size == 1, s"Actual length: ${comments.size}, expected: 1. Value: ${comments}")
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
