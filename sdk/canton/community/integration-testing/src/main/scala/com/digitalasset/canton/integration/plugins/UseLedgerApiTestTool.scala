// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{
  CantonConfig,
  ClientConfig,
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
}
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  EnvVarTestOverrides,
  LAPITTVersion,
  getArtifactoryHttpClient,
  latestVersionFromArtifactory,
}
import com.digitalasset.canton.integration.util.ExternalCommandExecutor
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.OptionUtil
import monocle.macros.syntax.lens.*
import spray.json.*
import spray.json.DefaultJsonProtocol.*

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{Authenticator, PasswordAuthentication, URI}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import scala.concurrent.blocking
import scala.io.Source
import scala.sys.process.*
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** Plugin to provide the LedgerApiTestTool to a
  * [[com.digitalasset.canton.integration.BaseIntegrationTest]] instance for
  *   - invoking ledger api test tool in an external java process
  *   - configuring canton with config settings required for conformance tests
  */
class UseLedgerApiTestTool(
    protected val loggerFactory: NamedLoggerFactory,
    connectedSynchronizersCount: Int,
    lfVersion: UseLedgerApiTestTool.LfVersion = UseLedgerApiTestTool.LfVersion.Stable,
    // If set, unique benchmark name for uploading benchmark results to datadog.
    benchmarkReportFileO: Option[String] = None,
    version: LAPITTVersion = LAPITTVersion.Latest,
    javaOpts: String = "-Xmx500m",
    defaultExtraArguments: Map[String, String] = Map("--timeout-scale-factor" -> "4"),
) extends EnvironmentSetupPlugin
    with NoTracing
    with EnvVarTestOverrides {

  private def defaultExtraArgumentsSeq: Seq[String] = defaultExtraArguments.flatMap { case (k, v) =>
    Seq(k, v)
  }.toSeq

  require(
    benchmarkReportFileO.forall(_.startsWith("benchmark_")),
    s"Benchmark report file must start with 'benchmark_', otherwise it won't be reported to DataDog. Found: $benchmarkReportFileO",
  )

  private val testToolName: String = s"ledger-api-test-tool${lfVersion.testToolSuffix}"

  private var testTool: File = _

  private val tempDir = File.newTemporaryDirectory()

  private val commandExecutor = new ExternalCommandExecutor(loggerFactory)

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    def ensurePrerequisites(): Unit = {
      // First ensure we are able to find and invoke java as that is needed to invoke the test tool.
      commandExecutor.exec(cmd = "java --version", errorHint = "Is 'java' not on the path?")

      lazy val httpClient = getArtifactoryHttpClient

      val testToolVersion =
        version match {
          case LAPITTVersion.Latest =>
            latestVersionFromArtifactory(logger)
          case LAPITTVersion.Explicit(v) => v
          case LAPITTVersion.LocalJar =>
            BuildInfo.version
        }

      val url =
        version match {
          case LAPITTVersion.LocalJar =>
            // Requires running `sbt ledger-test-tool-<lfVersion>/assembly` first.
            s"file://${System.getProperty("user.dir")}/community/ledger-test-tool/tool/lf-v${lfVersion.testToolSuffix.tail}/target/scala-2.13/$testToolName-$testToolVersion.jar"
          case _ =>
            val relativeUrl =
              s"com/digitalasset/canton/ledger-api-test-tool_2.13/$testToolVersion/${testToolName}_2.13-$testToolVersion.jar"
            s"https://digitalasset.jfrog.io/artifactory/canton-internal/$relativeUrl"
        }

      testTool = File(
        System.getProperty("user.home")
      ) / ".cache" / testToolName / s"${testToolName}_2.13-$testToolVersion.jar"

      UseLedgerApiTestTool.download(url, testTool, logger, httpClient)
    }

    ensurePrerequisites()

    // ensure we use production seeding setting in ledger api conformance and performance tests
    (ConfigTransforms.updateContractIdSeeding(Seeding.Weak) andThen
      // static time tests require this
      (_.focus(_.monitoring.logging.delayLoggingThreshold)
        .replace(NonNegativeFiniteDurationConfig.ofSeconds(1000))))(config)
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    // clear dars in temp dir
    tempDir.clear()

  def runSuites(
      suites: String, // comma-separated list of suites
      exclude: Seq[String],
      concurrency: Int,
      kv: (String, String)*
  )(implicit env: TestConsoleEnvironment): String = {
    val excludeParameter = NonEmpty.from(exclude) match {
      case Some(suitesNE) => Seq("--exclude", suitesNE.mkString(","))
      case None => Nil
    }

    val additionalParameters = (defaultExtraArguments ++ kv).flatMap { case (k, v) => Seq(k, v) }

    runTestsInternal(
      concurrentTestRuns = concurrency,
      connectedSynchronizersCount = connectedSynchronizersCount,
      testInclusions = suites.split(",").toSeq,
      extraArgs = additionalParameters.toSeq ++ excludeParameter,
      testParticipants = testParticipants,
    )
  }

  def runSuitesSerially(
      suites: String, // comma-separated list of suites
      exclude: Seq[String],
      kv: (String, String)*
  )(implicit env: TestConsoleEnvironment): String =
    runSuites(suites = suites, exclude = exclude, concurrency = 1, kv*)

  def runShardedSuites(
      shard: Int,
      numShards: Int,
      exclude: Seq[String],
      concurrentTestRuns: Int = 4,
  )(implicit
      env: TestConsoleEnvironment
  ): String = {
    val allTests = execTestTool("--list-all").split("\n")
    val listing = allTests
      .filter(line => exclude.forall(not => !line.contains(not)))
      .filter(_.contains(":"))
      .map(_.trim)
      .zipWithIndex
      .filter { case (_, idx) =>
        idx % numShards == shard
      }
      .map(_._1)

    runTestsInternal(
      concurrentTestRuns = concurrentTestRuns,
      connectedSynchronizersCount = connectedSynchronizersCount,
      testInclusions = listing.toSeq,
      extraArgs = defaultExtraArgumentsSeq,
      testParticipants = testParticipants,
    )
  }

  private def runTestsInternal(
      concurrentTestRuns: Int,
      connectedSynchronizersCount: Int,
      testInclusions: Seq[String],
      extraArgs: Seq[String],
      testParticipants: Seq[String],
  ): String = {
    val testInclusionsAfterEnvArgConsideration = envArgTestsInclusion
      .map { selectedTests =>
        val filtered = testInclusions.filter(selectedTests.testCaseEnabled)
        if (filtered.isEmpty) {
          // Fine to use the scalatest cancel here as this method is expected to be invoked from
          // from a ScalaTest case.
          org.scalatest.Assertions.cancel(
            s"After applying the restriction from the env var $LapittRunOnlyEnvVarName no tests remain to be run. " +
              s"Original test selection: $testInclusions. Restriction applied: $selectedTests."
          )
        }

        filtered
      }
      .getOrElse(testInclusions)

    execTestTool(
      Seq(
        "--concurrent-test-runs",
        concurrentTestRuns.toString,
        "--connected-synchronizers",
        connectedSynchronizersCount.toString,
        "--include",
        testInclusionsAfterEnvArgConsideration.mkString(","),
      ) ++ extraArgs ++ testParticipants: _*
    )
  }

  private def execTestTool(option: String*): String =
    commandExecutor.exec(
      cmd = s"java $javaOpts -jar ${testTool.toString} ${option.mkString(" ")}",
      errorHint = s"Failures in aforementioned test suite.",
    )

  private def endpointAsString(config: ClientConfig) = s"${config.address}:${config.port.toString}"

  private def testParticipants(implicit env: TestConsoleEnvironment): Seq[String] =
    env.participants.all
      .map { p =>
        val ledgerApiEndpoint = endpointAsString(p.config.clientLedgerApi)
        val adminApiEndpoint = endpointAsString(p.config.clientAdminApi)
        s"$ledgerApiEndpoint;$adminApiEndpoint"
      }

}

object UseLedgerApiTestTool {
  sealed trait TestInclusions extends Product with Serializable {
    def testCaseEnabled(testCaseName: String): Boolean
  }

  object TestInclusions {
    case object AllIncluded extends TestInclusions {
      def testCaseEnabled(testCaseName: String): Boolean = true
    }

    /** @param includedSuites
      *   Full suites to include
      * @param includedTestCases
      *   Specific test cases to include. Adding individual test cases here is redundant if their
      *   suite is already included in [[includedSuites]].
      */
    final case class SelectedTests(
        includedSuites: Set[String],
        includedTestCases: Set[String] = Set.empty,
    ) extends TestInclusions {
      def testCaseEnabled(testCaseName: String): Boolean =
        testCaseName.split(":").map(_.trim).toSeq match {
          case Seq(suite, _) =>
            includedSuites.contains(suite) || includedTestCases.contains(testCaseName)
          case Seq(suite) => includedSuites.contains(suite)
          case _other =>
            throw new IllegalArgumentException(
              s"Invalid test case name: $testCaseName. Expected format: SuiteName:TestCaseName"
            )
        }
    }
  }

  trait EnvVarTestOverrides {
    this: NamedLogging =>

    protected val LapittRunOnlyEnvVarName = "LAPI_CONFORMANCE_TEST_RUN_ONLY"

    /** Set the environment variable `LAPI_CONFORMANCE_TEST_RUN_ONLY` to a comma-separated list of
      * test suite names or test case names to restrict the tests being run as part of a specific
      * conformance test suite target.
      *
      * e.g. LAPI_CONFORMANCE_TEST_RUN_ONLY=CommandServiceIT sbt "testOnly
      * *JsonApiConformanceIntegrationShardedTest_Shard_0"
      */
    protected lazy val envTestFilterO: Option[Seq[String]] =
      sys.env.get(LapittRunOnlyEnvVarName).map(_.split(",").view.map(_.trim).toSeq)

    // Implementors of this trait should use this value to filter the tests being run
    // with the restriction provided via the env var.
    protected lazy val envArgTestsInclusion: Option[TestInclusions.SelectedTests] = envTestFilterO
      .flatMap { envTestFilter =>
        val selectedTestsO = envTestFilter
          .map(_.split(":").toSeq match {
            case Seq(suite, test) =>
              TestInclusions
                .SelectedTests(includedSuites = Set.empty, includedTestCases = Set(s"$suite:$test"))
            case Seq(suite) => TestInclusions.SelectedTests(includedSuites = Set(suite))
            case _ => throw new IllegalArgumentException(s"Invalid test filter: $envTestFilter")
          })
          .reduceOption((s1, s2) =>
            TestInclusions.SelectedTests(
              includedSuites = s1.includedSuites ++ s2.includedSuites,
              includedTestCases = s1.includedTestCases ++ s2.includedTestCases,
            )
          )

        selectedTestsO.foreach(selectedTests =>
          logger.debug(
            s"$LapittRunOnlyEnvVarName set to $envTestFilterO. Filtering current test selection in ${getClass.getSimpleName} using restriction $selectedTests."
          )(TraceContext.empty)
        )

        selectedTestsO
      }
  }

  sealed trait LfVersion {
    def testToolSuffix: String
  }

  object LfVersion {
    case object Stable extends LfVersion {
      override def testToolSuffix: String = "-2.2"
    }

    case object Dev extends LfVersion {
      override def testToolSuffix: String = "-2.dev"
    }
  }

  sealed trait LAPITTVersion

  object LAPITTVersion {
    // The lapitt runs only for the latest version of latest release.
    case object Latest extends LAPITTVersion

    // The lapitt runs only for the specified version.
    final case class Explicit(version: String) extends LAPITTVersion

    // The lapitt runs with the local lapitt jar.
    // Requires running `sbt ledger-test-tool-<lfVersion>/assembly` first
    case object LocalJar extends LAPITTVersion
  }

  // Check if test tool resides in destination. If not, download test tool.
  // Ideally we'd rely on sbt and coursier to manage the dependency to avoid having to deal with  caching ourselves,
  // but these does not seem to be a straightforward way to keep the test tool off from the classpath so that the
  // fat jar contents don't interfere with canton dependencies (e.g. fastparse).
  def download(
      url: String,
      destination: File,
      logger: TracedLogger,
      httpClient: HttpClient,
      retries: Int = 3,
  )(implicit
      tc: TraceContext
  ): Unit =
    blocking(this.synchronized {
      destination.parent.createDirectoryIfNotExists(createParents = true)
      // We don't want to cache for files, that's very (very) annoying
      if (url.startsWith("file")) {
        val source = File(new URI(url))
        logger.info(s"Copying local file from $source to $destination")
        Files.copy(source.path, destination.path, StandardCopyOption.REPLACE_EXISTING)
      } else if (!destination.exists) {
        downloadFromArtifactory(url, destination, logger, httpClient, retries)
      }
    })

  private def downloadFromArtifactory(
      url: String,
      destination: File,
      logger: TracedLogger,
      httpClient: HttpClient,
      retries: Int,
  )(implicit tc: TraceContext): Unit = {
    logger.info(
      s"File ${destination.toString} does not exist locally. Downloading tool from $url"
    )

    val processLogger = new BufferedProcessLogger()
    var response: HttpResponse[Path] = null
    Try {
      // not using new URL(url) #> destination.toJava !! processLogger
      // as IOExceptions during the download will be written to stdout and there is no way to override this
      // behaviour. https://github.com/scala/scala/blob/2.13.x/src/library/scala/sys/process/ProcessImpl.scala#L192

      val request = HttpRequest.newBuilder(new URI(url)).build()
      response = httpClient.send(
        request,
        HttpResponse.BodyHandlers.ofFileDownload(
          destination.parent.path,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING,
        ),
      )
      if (response.statusCode() != 200) {
        sys.error(s"Failed to download ledger api test tool. Response: $response")
      }
      logger.debug("Verifying downloaded archive")
      s"jar -tvf ${destination.toJava.getAbsolutePath}" !! processLogger
    } match {
      case Success(str) =>
        if (str.nonEmpty)
          logger.info(str)
        logger.info(processLogger.output("OUTPUT: "))
      case Failure(t) =>
        if (retries > 0) {
          if (destination.exists)
            destination.delete(swallowIOExceptions = true)
          logger.info(s"Failed to download from $url. Exception ${t.getMessage}. Will retry")
          logger.info(processLogger.output("OUTPUT: "))
          Threading.sleep(2000)
          downloadFromArtifactory(url, destination, logger, httpClient, retries - 1)
        } else {
          logger.error(
            s"Failed to download from $url. Exception ${t.getMessage}. Giving up",
            t,
          )
          logger.warn(processLogger.output("OUTPUT: "))
          throw t
        }
    }
  }

  final case class ArtifactoryItem(uri: String, folder: Boolean)

  implicit val artifactoryItemFormat: RootJsonFormat[ArtifactoryItem] = jsonFormat2(
    ArtifactoryItem.apply
  )

  private def credentialsFromNetrcFile: (Option[String], Option[String]) = {
    val netrcPath = System.getProperty("user.home") + "/.netrc"
    val machine = "digitalasset.jfrog.io"

    lazy val netrcFile = Source.fromFile(netrcPath)
    lazy val content = netrcFile.mkString

    lazy val pattern = s"(?m)^machine\\s+$machine\\s+login\\s+(\\S+)\\s+password\\s+(\\S+)".r

    val usernameO =
      pattern.findFirstMatchIn(content).map(_.group(1))
    val passwordO =
      pattern.findFirstMatchIn(content).map(_.group(2))

    netrcFile.close()

    (usernameO, passwordO)
  }

  private def allToolVersionsFromArtifactory(httpClient: HttpClient) = {
    val artifactoryDirectoryUrl =
      "https://digitalasset.jfrog.io/artifactory/api/storage/canton-internal/com/digitalasset/canton/ledger-api-test-tool_2.13"
    val request = HttpRequest.newBuilder(new URI(artifactoryDirectoryUrl)).build()
    val response = httpClient.send(
      request,
      HttpResponse.BodyHandlers.ofString(),
    )
    if (response.statusCode() != 200)
      sys.error(
        s"Failed to fetch ledger api test tool versions from $artifactoryDirectoryUrl. Response: $response"
      )

    // from jfrog api a json object is returned which contains the folders in children field
    val obj = response.body.parseJson.asJsObject
    val files = obj.fields("children").convertTo[Seq[ArtifactoryItem]]

    files
      .map(_.uri)
      .map(str => if (str.startsWith("/")) str.drop(1) else str)
      .filterNot(_.contains("100000000"))
  }

  private val versionPattern: Regex = """^(\d+\.\d+\.\d+)-(ad-hoc|snapshot)\.(\d{8}\.\d{4}).*""".r

  def extractVersionString: PartialFunction[String, String] = {
    case versionPattern(majorMinorPatch, _, _) =>
      majorMinorPatch // keep only major.minor.patch versions
  }

  // finds all major.minor.patch releases
  def findAllReleases(toolVersions: Seq[String]): Seq[String] =
    toolVersions
      .collect(extractVersionString)
      .distinct
      .sorted

  // finds version of the release given and sorts them by the date produced
  def findMatchingVersions(toolVersions: Seq[String], latestRelease: String): Seq[String] =
    toolVersions
      .collect { // keep only specific major.minor.patch version
        case v @ versionPattern(`latestRelease`, _, date) => (v, date)
      }
      .sortBy(_._2) // sort by time
      .map(_._1)

  def latestVersionFromArtifactory(logger: TracedLogger)(implicit
      tc: TraceContext
  ): String = {
    val toolVersions = allToolVersionsFromArtifactory(getArtifactoryHttpClient)
    val releases = findAllReleases(toolVersions)
    val latestRelease = releases.lastOption.getOrElse(
      throw new RuntimeException(
        s"No releases found in artifactory among the following files: $toolVersions"
      )
    )

    val matchingVersions = findMatchingVersions(toolVersions, latestRelease)
    val matchingVersion = matchingVersions.lastOption.getOrElse(
      throw new RuntimeException(
        s"No matching version found for release $latestRelease"
      )
    )
    logger.debug(s"found $matchingVersion as latest version of $latestRelease in the artifactory ")

    matchingVersion
  }

  def releasesFromArtifactory(logger: TracedLogger)(implicit
      tc: TraceContext
  ): Seq[String] = {
    val httpClient: HttpClient = getArtifactoryHttpClient

    val toolVersions = allToolVersionsFromArtifactory(httpClient)
    val releases = findAllReleases(toolVersions)

    val latestVersionForAllReleases = for {
      release <- releases
    } yield {
      findMatchingVersions(toolVersions, release).lastOption
    }

    logger.debug(
      s"found $latestVersionForAllReleases as latest versions for each release in the artifactory "
    )

    latestVersionForAllReleases.flatten
  }

  lazy val getArtifactoryHttpClient: HttpClient =
    HttpClient
      .newBuilder()
      .authenticator(new Authenticator {
        override def getPasswordAuthentication: PasswordAuthentication =
          new PasswordAuthentication(
            sys.env
              .get("ARTIFACTORY_USER")
              .flatMap(OptionUtil.emptyStringAsNone)
              .orElse(sys.env.get("ARTIFACTORY_USERNAME").flatMap(OptionUtil.emptyStringAsNone))
              .orElse(credentialsFromNetrcFile._1.flatMap(OptionUtil.emptyStringAsNone))
              .getOrElse(
                throw new IllegalArgumentException(
                  "env vars ARTIFACTORY_USER or ARTIFACTORY_USERNAME not set or empty" +
                    "and no login entry found in the netrc file (if existing)"
                )
              ),
            sys.env
              .get("ARTIFACTORY_PASSWORD")
              .flatMap(OptionUtil.emptyStringAsNone)
              .orElse(credentialsFromNetrcFile._2)
              .getOrElse("")
              .toCharArray,
          )

      })
      .build()
}
