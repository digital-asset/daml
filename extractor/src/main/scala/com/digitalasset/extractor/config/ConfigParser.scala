// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import java.io.File
import java.nio.file.Paths

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.extractor.targets._
import com.digitalasset.ledger.api.tls.TlsConfiguration
import CustomScoptReaders._

import scalaz.OneAnd

import scala.util.Try
import scopt.OptionParser

object ConfigParser {
  private sealed trait CliTarget
  private case object SimpleText extends CliTarget
  private case object PrettyPrint extends CliTarget
  private case object PostgreSQL extends CliTarget

  // It's better to add an intermediate type similarly as Scallop works
  // (see [[ScallopConfig]] in git history which is already nicer),
  // so we can save some craziness when building a configuration which contains values of ADTs.
  // Also https://github.com/backuity/clist is worth checking out.
  private case class CliParams(
      target: CliTarget = PrettyPrint,
      pprintWidth: Int = 200,
      pprintHeight: Int = 1200,
      postgresConnectUrl: String = "",
      postgresUser: String = "",
      postgresPassword: String = "",
      postgresOutputFormat: String = "single-table",
      postgresMultiTableUseSchemes: Boolean = false,
      postgresMultiTableMergeIdentical: Boolean = false,
      postgresStripPrefix: Option[String] = None,
      ledgerHost: String = "127.0.0.1",
      ledgerPort: Int = 6865,
      party: ExtractorConfig.Parties = OneAnd(Party assertFromString "placeholder", Nil),
      from: Option[String] = None,
      to: Option[String] = None,
      tlsPem: Option[String] = None,
      tlsCrt: Option[String] = None,
      tlsCaCrt: Option[String] = None
  )

  private val configParser: OptionParser[CliParams] =
    new scopt.OptionParser[CliParams]("extractor") {

      override def showUsageOnError: Boolean = true

      val colSpacer = "                           "

      cmd("simpletext")
        .text("Print contract template and transaction data to stdout in bare text format.")
        .action((_, c) => c.copy(target = SimpleText))
        .hidden()

      // hide this while ^ is hidden
//    note("") // newline

      cmd("prettyprint")
        .text("Pretty print contract template and transaction data to stdout.")
        .action((_, c) => c.copy(target = PrettyPrint))
        .children(
          opt[Int]("width")
            .text(
              "How wide to allow a pretty-printed value to become before wrapping.\n" +
                s"${colSpacer}Optional, default is 120."
            )
            .optional()
            .action((w, c) => c.copy(pprintWidth = w)),
          opt[Int]("height")
            .text(
              "How tall to allow each pretty-printed output to become before\n" +
                s"${colSpacer}it is truncated with a `...`.\n" +
                s"${colSpacer}Optional, default is 1000."
            )
            .optional()
            .action((h, c) => c.copy(pprintHeight = h))
        )

      note("") // newline

      cmd("postgresql")
        .text("Extract data into a PostgreSQL database.")
        .action((_, c) => c.copy(target = PostgreSQL))
        .children(
          opt[String]("connecturl")
            .text(
              "Connection url for the `org.postgresql.Driver` driver. For examples,\n" +
                s"${colSpacer}visit https://jdbc.postgresql.org/documentation/80/connect.html"
            )
            .required()
            .action((x, c) => c.copy(postgresConnectUrl = x)),
          opt[String]("user")
            .text(
              "The database user on whose behalf the connection is being made."
            )
            .required()
            .action((x, c) => c.copy(postgresUser = x)),
          opt[String]("password")
            .text(
              "The user's password. Optional."
            )
            .optional()
            .action((x, c) => c.copy(postgresPassword = x)),
          opt[String]("output-format")
            .optional()
            .hidden()
            .validate { s =>
              if (List("single-table", "multi-table", "combined").contains(s))
                Right(())
              else
                Left(
                  "Invalid value for parameter `--output-format`." +
                    """Must be one of "single-table", "multi-table" or "combined"."""
                )
            }
            .valueName("<single-table|multi-table|combined>")
            .action((s, c) => c.copy(postgresOutputFormat = s))
            .text(
              s"""Format of the extracted data in the database. One of "single-table", "multi-table" or "combined".\n""" +
                s"""${colSpacer}"single-table" = All contracts are put into a JSON encoded column of the same table.\n""" +
                s"""${colSpacer}"multi-table" = Templates get their own tables, and create arguments their own column.\n""" +
                s"""${colSpacer}"combined" = Use both data formats. Data will be duplicated."""" +
                s"""Optional, default is "single-table"."""
            ),
          opt[Boolean]("schema-per-package")
            .optional()
            .hidden()
            .valueName("<true|false>")
            .action((x, c) => c.copy(postgresMultiTableUseSchemes = x))
            .text(
              "Whether to put contacts of separate packages into separate schemes. Optional, default is false."
            ),
          opt[Boolean]("merge-identical")
            .optional()
            .hidden()
            .valueName("<true|false>")
            .action((x, c) => c.copy(postgresMultiTableMergeIdentical = x))
            .text(
              "Whether to merge identical templates of separate packages into the same table.\n" +
                s"${colSpacer}Optional, default is false."
            ),
          opt[String]("strip-prefix")
            .optional()
            .hidden()
            .valueName("<value>")
            .action((x, c) => c.copy(postgresStripPrefix = Some(x)))
            .text(
              "Parts of template names to cut from the beginning when using the multi-table strategy\n" +
                s"${colSpacer}Optional."
            )
        )

      note("\nCommon options:\n")

      opt[String]('h', "ledger-host")
        .optional()
        .action((x, c) => c.copy(ledgerHost = x))
        .valueName("<h>")
        .text("The address of the Ledger host. Default is 127.0.0.1")

      opt[Int]('p', "ledger-port")
        .optional()
        .action((x, c) => c.copy(ledgerPort = x))
        .valueName("<p>")
        .text("The port of the Ledger host. Default is 6865.")

      opt[ExtractorConfig.Parties]("party")
        .required()
        .action((x, c) => c.copy(party = x))
        .text("The party or parties whose contract data should be extracted.\n" +
          s"${colSpacer}Specify multiple parties separated by a comma, e.g. Foo,Bar")

      opt[String]("from")
        .action((x, c) => c.copy(from = Some(x)))
        .optional()
        .text(
          "The transaction offset (exclusive) for the snapshot start position.\n" +
            s"${colSpacer}Must not be greater than the current latest transaction offset.\n" +
            s"${colSpacer}Optional, defaults to the beginning of the ledger.\n" +
            s"${colSpacer}Currently, only the integer-based Sandbox offsets are supported."
        )

      opt[String]("to")
        .optional()
        .action((x, c) => c.copy(to = Some(x)))
        .text(
          "The transaction offset (inclusive) for the snapshot end position.\n" +
            s"${colSpacer}Use “head” to use the latest transaction offset at the time\n" +
            s"${colSpacer}the extraction first started, or “follow” to stream indefinitely.\n" +
            s"${colSpacer}Must not be greater than the current latest offset.\n" +
            s"${colSpacer}Optional, defaults to “follow”."
        )

      help("help").text("Prints this usage text.")

      note("\nTLS configuration:")

      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .validate(validatePath(_, "The file specified via --pem does not exist"))
        .action { (path, c) =>
          c.copy(tlsPem = Some(path))
        }
      opt[String]("crt")
        .optional()
        .text(
          s"TLS: The crt file to be used as the cert chain.\n${colSpacer}" +
            s"Required if any other TLS parameters are set."
        )
        .validate(validatePath(_, "The file specified via --crt does not exist"))
        .action { (path, c) =>
          c.copy(tlsCrt = Some(path))
        }
      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the the trusted root CA.")
        .validate(validatePath(_, "The file specified via --cacrt does not exist"))
        .action { (path, c) =>
          c.copy(tlsCaCrt = Some(path))
        }

      checkConfig { c =>
        if (c.postgresMultiTableUseSchemes && !List("multi-table", "combined").contains(
            c.postgresOutputFormat)) {
          failure(
            "\n`--schema-per-package` was set `true`, while the data format strategy wasn't set to\n" +
              "use separate tables per contract. This setting won't have any effects.\n" +
              "Change the `--output-format` parameter to \"multi-table\" or \"combined\" to have a multi-table setup,\n" +
              "or remove this parameter.\n"
          )
        } else if (c.postgresMultiTableMergeIdentical && !List("multi-table", "combined").contains(
            c.postgresOutputFormat
          )) {
          failure(
            "\n`--merge-identical` was set `true`, while the data format strategy wasn't set to\n" +
              "use separate tables per contract. This setting won't have any effects.\n" +
              "Change the `--output-format` parameter to \"multi-table\" or \"combined\" to have a multi-table setup,\n" +
              "or remove this parameter.\n"
          )
        } else if (c.postgresMultiTableMergeIdentical && c.postgresMultiTableUseSchemes) {
          failure(
            "\nBoth `--merge-identical` and `--schema-per-package` parameter are set to `true`.\n" +
              "Pick at most one of those.\n"
          )
        } else {
          success
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  def parse(args: Seq[String]): Option[(ExtractorConfig, Target)] = {
    configParser.parse(args, CliParams()).map { cliParams =>
      val from = cliParams.from.fold(
        LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
      )(x => LedgerOffset(LedgerOffset.Value.Absolute(x)))

      val to = cliParams.to.fold(
        SnapshotEndSetting.Follow: SnapshotEndSetting
      ) {
        case "head" => SnapshotEndSetting.Head
        case "follow" => SnapshotEndSetting.Follow
        case x => SnapshotEndSetting.Until(x)
      }

      val tlsConfig = TlsConfiguration(
        enabled = cliParams.tlsPem.isDefined || cliParams.tlsCrt.isDefined || cliParams.tlsCaCrt.isDefined,
        cliParams.tlsPem.map(new File(_)),
        cliParams.tlsCrt.map(new File(_)),
        cliParams.tlsCaCrt.map(new File(_))
      )

      val config = ExtractorConfig(
        cliParams.ledgerHost,
        cliParams.ledgerPort,
        from,
        to,
        cliParams.party,
        tlsConfig
      )

      val target = cliParams.target match {
        case SimpleText => TextPrintTarget
        case PrettyPrint => PrettyPrintTarget(cliParams.pprintWidth, cliParams.pprintHeight)
        case PostgreSQL =>
          PostgreSQLTarget(
            cliParams.postgresConnectUrl,
            cliParams.postgresUser,
            cliParams.postgresPassword,
            cliParams.postgresOutputFormat,
            cliParams.postgresMultiTableUseSchemes,
            cliParams.postgresMultiTableMergeIdentical,
            cliParams.postgresStripPrefix
          )
      }

      (config, target)
    }
  }

  def showUsage(): Unit =
    configParser.showUsage()

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (valid) Right(()) else Left(message)
  }
}
