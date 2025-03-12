// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.Functor
import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor, Threading}
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCryptoProvider
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLogging, SuppressingLogger}
import com.digitalasset.canton.metrics.OpenTelemetryOnDemandMetricsReader
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, W3CTraceContext}
import com.digitalasset.canton.util.CheckedT
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionValidation,
  ReleaseProtocolVersion,
}
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.mockito.{ArgumentMatchers, ArgumentMatchersSugar}
import org.scalacheck.Test
import org.scalactic.source.Position
import org.scalactic.{Prettifier, source}
import org.scalatest.*
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.scalacheck.CheckerAsserting
import org.slf4j.bridge.SLF4JBridgeHandler
import org.typelevel.discipline.Laws

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait ScalaFuturesWithPatience extends ScalaFutures {

  /** Increase default timeout when evaluating futures.
    */
  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(20, Millis))
}

/** Tests' essentials disaggregated from scalatest's traits.
  */
trait TestEssentials
    extends ScalaFuturesWithPatience
    // There are many MockitoSugar implementations, but only this one is not deprecated and
    // supports when, verify, ...
    with org.mockito.MockitoSugar
    with ArgumentMatchersSugar
    with NamedLogging {

  protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

  protected lazy val testedProtocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion
  protected lazy val testedProtocolVersionValidation: ProtocolVersionValidation =
    BaseTest.testedProtocolVersionValidation
  protected lazy val testedReleaseProtocolVersion: ReleaseProtocolVersion =
    BaseTest.testedReleaseProtocolVersion
  protected lazy val defaultStaticSynchronizerParameters: StaticSynchronizerParameters =
    BaseTest.defaultStaticSynchronizerParameters

  // default to providing an empty trace context to all tests
  protected implicit def traceContext: TraceContext = TraceContext.empty
  // default to providing no reporting tracer to all tests
  protected implicit lazy val tracer: Tracer = NoReportingTracerProvider.tracer

  protected lazy val nonEmptyTraceContext1: TraceContext =
    W3CTraceContext("00-9caf33ee8c95383e5563f3b99a2bf90f-fdd860fe948aa866-01").toTraceContext
  protected lazy val nonEmptyTraceContext2: TraceContext =
    W3CTraceContext("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").toTraceContext

  // increase default patience from 5s to 20s to account for noisy CI neighbours
  implicit override val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(25, Millis))

  // when mocking methods touching transactions it's very common to need to mock the traceContext as a an additional argument list
  def anyTraceContext: TraceContext = ArgumentMatchers.any[TraceContext]()

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  val futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

  lazy val wallClock = new WallClock(timeouts, loggerFactory)

  // Make sure that JUL logging is redirected to SLF4J
  if (!SLF4JBridgeHandler.isInstalled) {
    // we want everything going to slf4j so remove any default loggers
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
  }

  /** Execution context for running "quick" computations. As there are many implementations of
    * DirectExecutionContext, this is provided as a convenience field, so that tests don't have to
    * deal with imports.
    */
  lazy val directExecutionContext: ExecutionContext = DirectExecutionContext(noTracingLogger)
}

trait FutureHelpers extends Assertions with ScalaFuturesWithPatience { self =>

  import scala.language.implicitConversions

  /** Allows for invoking `myEitherT.futureValue` when `myEitherT: EitherT[Future, _, _]`.
    */
  implicit def futureConceptOfEitherTFuture[A, B](eitherTFuture: EitherT[Future, A, B])(implicit
      ec: ExecutionContext
  ): FutureConcept[B] =
    eitherTFuture.valueOr(err => fail(s"Unexpected left value $err"))

  /** Allows for returning an `EitherT[Future, _, Assertion]` instead of `Future[Assertion]` in
    * asynchronous test suites.
    */
  implicit def futureAssertionOfEitherTAssertion[A](
      eitherTAssertion: EitherT[Future, A, Assertion]
  )(implicit ec: ExecutionContext, pos: source.Position): Future[Assertion] =
    eitherTAssertion.valueOr(left => fail(s"Unexpected left value $left"))

  /** Allows for returning an `OptionT[Future, Assertion]` instead of `Future[Assertion]` in
    * asynchronous test suites.
    */
  implicit def futureAssertionOfOptionTAssertion(
      optionTAssertion: OptionT[Future, Assertion]
  )(implicit ec: ExecutionContext, pos: source.Position): Future[Assertion] =
    optionTAssertion.getOrElse(fail(s"Unexpected None value"))

  /** Converts an EitherT into a Future, failing in case of a [[scala.Left$]]. */
  def valueOrFail[F[_], A, B](e: EitherT[F, A, B])(
      clue: String
  )(implicit position: Position, F: Functor[F]): F[B] =
    e.fold(x => fail(s"$clue: ${x.toString}"), Predef.identity)

  /** Converts a CheckedT into a Future, failing in case of aborts or non-aborts. */
  def valueOrFail[F[_], A, N, R](
      c: CheckedT[F, A, N, R]
  )(clue: String)(implicit position: Position, F: Functor[F]): F[R] =
    c.fold(
      (a, ns) => fail(s"$clue: ${a.toString}, ${ns.toString}"),
      (ns, x) => if (ns.isEmpty) x else fail(s"$clue: ${ns.toString}, ${x.toString}"),
    )

  /** Converts an OptionT into a Future, failing in case of a [[scala.None$]]. */
  def valueOrFail[A](e: OptionT[Future, A])(
      clue: String
  )(implicit ec: ExecutionContext, position: Position): Future[A] =
    e.fold(fail(clue))(Predef.identity)

  /** Converts an OptionT into a FutureUnlessShutdown, failing in case of a [[scala.None$]]. */
  def valueOrFailUS[A](e: OptionT[FutureUnlessShutdown, A])(
      clue: String
  )(implicit ec: ExecutionContext, position: Position): FutureUnlessShutdown[A] =
    e.fold(fail(clue))(Predef.identity)

  /** Converts an OptionT into a Future, failing in case of a [[scala.Some$]]. */
  def noneOrFail[A](e: OptionT[Future, A])(
      clue: String
  )(implicit ec: ExecutionContext, position: Position): Future[Assertion] =
    e.fold(succeed)(some => fail(s"$clue, value is $some"))

  /** Converts an OptionT into a FutureUnlessShutdown, failing in case of a [[scala.Some$]]. */
  def noneOrFailUS[A](e: OptionT[FutureUnlessShutdown, A])(
      clue: String
  )(implicit ec: ExecutionContext, position: Position): FutureUnlessShutdown[Assertion] =
    e.fold(succeed)(some => fail(s"$clue, value is $some"))

  /** Converts an Either into a B value, failing in case of a [[scala.Left$]]. */
  def valueOrFail[A, B](e: Either[A, B])(clue: String)(implicit position: Position): B =
    e.fold(x => fail(s"$clue: ${x.toString}"), Predef.identity)

  /** Converts an Option into a A value, failing in case of a [[scala.None$]]. */
  def valueOrFail[A](o: Option[A])(clue: String)(implicit position: Position): A =
    o.getOrElse(fail(s"$clue"))

  /** Converts an EitherT into a Future, failing in a case of a [[scala.Right$]] */
  def leftOrFail[F[_], A, B](e: EitherT[F, A, B])(
      clue: String
  )(implicit position: Position, F: Functor[F]): F[A] =
    valueOrFail(e.swap)(clue)

  /** Converts an EitherT into a Future, failing in a case of a [[scala.Right$]] or shutdown */
  def leftOrFailShutdown[A, B](e: EitherT[FutureUnlessShutdown, A, B])(
      clue: String
  )(implicit ec: ExecutionContext, position: Position): Future[A] =
    e.swap.valueOrFailShutdown(clue)

  /** Converts an Either into an A value, failing in a case of a [[scala.Right$]] */
  def leftOrFail[A, B](e: Either[A, B])(clue: String)(implicit position: Position): A =
    valueOrFail(e.swap)(clue)

  // Syntax extensions for valueOrFail
  implicit class OptionTestSyntax[A](option: Option[A]) {
    def valueOrFail(clue: String)(implicit pos: Position): A = self.valueOrFail(option)(clue)
  }

  implicit class EitherTestSyntax[E, A](either: Either[E, A]) {
    def valueOrFail(clue: String)(implicit pos: Position): A = self.valueOrFail(either)(clue)

    def leftOrFail(clue: String)(implicit pos: Position): E = self.leftOrFail(either)(clue)
  }

  implicit class EitherTTestSyntax[F[_], E, A](eitherT: EitherT[F, E, A]) {
    def valueOrFail(clue: String)(implicit pos: Position, F: Functor[F]): F[A] =
      self.valueOrFail(eitherT)(clue)

    def leftOrFail(clue: String)(implicit pos: Position, F: Functor[F]): F[E] =
      self.leftOrFail(eitherT)(clue)
  }

  implicit class EitherTFutureUnlessShutdownSyntax[E, A](
      eitherT: EitherT[FutureUnlessShutdown, E, A]
  ) {
    def valueOrFail(
        clue: String
    )(implicit ec: ExecutionContext, pos: Position): FutureUnlessShutdown[A] =
      self.valueOrFail(eitherT)(clue)

    def valueOrFailShutdown(clue: String)(implicit ec: ExecutionContext, pos: Position): Future[A] =
      self.valueOrFail(eitherT)(clue).onShutdown(fail(s"Shutdown during $clue"))

    def leftOrFailShutdown(clue: String)(implicit ec: ExecutionContext, pos: Position): Future[E] =
      self.leftOrFail(eitherT)(clue).onShutdown(fail(s"Shutdown during $clue"))

    def failOnShutdown(implicit ec: ExecutionContext, pos: Position): EitherT[Future, E, A] =
      eitherT.onShutdown(fail("Unexpected shutdown"))

    def futureValueUS(implicit pos: Position): Either[E, A] =
      eitherT.value.futureValueUS
  }

  implicit class CheckedTFutureUnlessShutdownSyntax[A, N, R](
      checkedT: CheckedT[FutureUnlessShutdown, A, N, R]
  ) {
    def failOnShutdown(implicit ec: ExecutionContext, pos: Position): CheckedT[Future, A, N, R] =
      CheckedT(checkedT.value.onShutdown(fail("Unexpected shutdown")))
  }

  implicit class EitherTUnlessShutdownSyntax[E, A](
      eitherT: EitherT[UnlessShutdown, E, A]
  ) {
    def valueOrFailShutdown(clue: String)(implicit pos: Position): A =
      self.valueOrFail(eitherT)(clue).onShutdown(fail(s"Shutdown during $clue"))

    def leftOrFailShutdown(clue: String)(implicit pos: Position): E =
      self.leftOrFail(eitherT)(clue).onShutdown(fail(s"Shutdown during $clue"))
  }

  implicit class FutureUnlessShutdownSyntax[A](fut: FutureUnlessShutdown[A]) {
    def failOnShutdown(clue: String)(implicit ec: ExecutionContext, pos: Position): Future[A] =
      fut.onShutdown(fail(s"Shutdown during $clue"))
    def failOnShutdown(implicit ec: ExecutionContext, pos: Position): Future[A] =
      fut.onShutdown(fail(s"Unexpected shutdown"))
    def futureValueUS(implicit pos: Position): A =
      fut.unwrap.futureValue.onShutdown(fail("Unexpected shutdown"))
  }

  implicit class UnlessShutdownSyntax[A](us: UnlessShutdown[A]) {
    def failOnShutdown(clue: String)(implicit pos: Position): A =
      us.onShutdown(fail(s"Shutdown during $clue"))
    def failOnShutdown(implicit pos: Position): A =
      us.onShutdown(fail(s"Unexpected shutdown"))
  }

}

/** Base traits for tests. Makes syntactic sugar and logging available.
  */
trait BaseTest
    extends TestEssentials
    with Matchers
    with Inspectors
    with LoneElement
    with TableDrivenPropertyChecks
    with Inside
    with EitherValues
    with OptionValues
    with TryValues
    with AppendedClues
    with FutureHelpers { self =>

  /** A metrics factory constructed from an OpenTelemetryOnDemandMetricsReader which allows to make
    * assertion on the content of the metrics registry.
    */
  def testableMetricsFactory(
      testName: String,
      onDemandMetricsReader: OpenTelemetryOnDemandMetricsReader,
      histograms: Set[String],
  ): OpenTelemetryMetricsFactory = {
    val sdkBuilder = OpenTelemetrySdk.builder()
    val meterProvider = SdkMeterProvider.builder()
    meterProvider.registerMetricReader(onDemandMetricsReader)
    sdkBuilder.setMeterProvider(meterProvider.build())
    val openTelemetry = ConfiguredOpenTelemetry(
      sdkBuilder.build(),
      SdkTracerProvider.builder(),
      onDemandMetricsReader,
    )
    new OpenTelemetryMetricsFactory(
      openTelemetry.openTelemetry.meterBuilder(testName).build(),
      histograms,
      None,
      MetricsContext.Empty,
    )
  }

  def clue[T](message: String)(expr: => T): T = {
    logger.debug(s"Running clue: $message")
    Try(expr) match {
      case Success(value) =>
        logger.debug(s"Finished clue: $message")
        value
      case Failure(ex) =>
        logger.error(s"Failed clue: $message", ex)
        throw ex
    }
  }
  def clueF[T](message: String)(expr: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    logger.debug(s"Running clue: $message")
    Try(expr) match {
      case Success(value) =>
        value.onComplete {
          case Success(_) =>
            logger.debug(s"Finished clue: $message")
          case Failure(ex) =>
            logger.error(s"Failed clue: $message", ex)
        }
        value
      case Failure(ex) =>
        throw ex
    }
  }
  def clueFUS[T](
      message: String
  )(expr: => FutureUnlessShutdown[T])(implicit ec: ExecutionContext): FutureUnlessShutdown[T] = {
    logger.debug(s"Running clue: $message")
    Try(expr) match {
      case Success(value) =>
        value.onComplete {
          case Success(_) =>
            logger.debug(s"Finished clue: $message")
          case Failure(ex) =>
            logger.error(s"Failed clue: $message", ex)
        }
        value
      case Failure(ex) =>
        throw ex
    }
  }

  private val scalaTestEventually = new Eventually {}
  def eventuallyAsync[T](
      timeUntilSuccess: FiniteDuration = 20.seconds,
      maxPollInterval: FiniteDuration = 5.seconds,
  )(testCode: => T)(implicit ec: ExecutionContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown.outcomeF(
      scalaTestEventually.eventually(
        PatienceConfiguration.Timeout(timeUntilSuccess),
        PatienceConfiguration.Interval(maxPollInterval),
      )(Future(testCode))
    )

  def eventually[T](
      timeUntilSuccess: FiniteDuration = 20.seconds,
      maxPollInterval: FiniteDuration = 5.seconds,
      retryOnTestFailuresOnly: Boolean = true,
  )(testCode: => T): T =
    BaseTest.eventually(
      timeUntilSuccess,
      maxPollInterval,
      retryOnTestFailuresOnly = retryOnTestFailuresOnly,
    )(testCode)

  /** Keeps evaluating `testCode` until it fails or a timeout occurs.
    * @return
    *   the result the last evaluation of `testCode`
    * @throws java.lang.Throwable
    *   if `testCode` terminates with a throwable
    * @throws java.lang.IllegalArgumentException
    *   if `timeout` or `pollIntervalMs` is negative
    */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def always[T](durationOfSuccess: FiniteDuration = 2.seconds, pollIntervalMs: Long = 10)(
      testCode: => T
  ): T = BaseTest.always(durationOfSuccess, pollIntervalMs)(testCode)

  def eventuallyForever[T](
      timeUntilSuccess: FiniteDuration = 2.seconds,
      durationOfSuccess: FiniteDuration = 2.seconds,
      pollIntervalMs: Long = 10,
  )(testCode: => T): T =
    BaseTest.eventuallyForever(timeUntilSuccess, durationOfSuccess, pollIntervalMs)(testCode)

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def withClueF[A](clue: String)(sut: => Future[A])(implicit ec: ExecutionContext): Future[A] =
    withClue(clue)(sut.transform(outcome => Try(withClue(clue)(outcome.get))))

  def forEveryParallel[A](inputs: Seq[A])(
      body: A => Assertion
  )(implicit executionContext: ExecutionContext): Assertion = forEvery(inputs.parTraverse { input =>
    Future(Try(body(input)))
  }.futureValue)(_.success.value)

  lazy val CantonExamplesPath: String = BaseTest.CantonExamplesPath
  lazy val CantonTestsPath: String = BaseTest.CantonTestsPath
  lazy val CantonTestsDevPath: String = BaseTest.CantonTestsDevPath
  lazy val PerformanceTestPath: String = BaseTest.PerformanceTestPath
  lazy val DamlTestFilesPath: String = BaseTest.DamlTestFilesPath
  lazy val DamlTestLfDevFilesPath: String = BaseTest.DamlTestLfDevFilesPath
}

object BaseTest {

  /** Keeps evaluating `testCode` until it fails or a timeout occurs.
    * @return
    *   the result the last evaluation of `testCode`
    * @throws java.lang.Throwable
    *   if `testCode` terminates with a throwable
    * @throws java.lang.IllegalArgumentException
    *   if `timeout` or `pollIntervalMs` is negative
    */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def always[T](durationOfSuccess: FiniteDuration = 2.seconds, pollIntervalMs: Long = 10)(
      testCode: => T
  ): T = {
    require(
      durationOfSuccess >= Duration.Zero,
      s"The timeout must not be negative, but is $durationOfSuccess",
    )
    require(pollIntervalMs >= 0)
    val deadline = durationOfSuccess.fromNow
    while (deadline.hasTimeLeft()) {
      testCode
      Threading.sleep(pollIntervalMs)
    }
    testCode
  }

  def eventuallyForever[T](
      timeUntilSuccess: FiniteDuration = 2.seconds,
      durationOfSuccess: FiniteDuration = 2.seconds,
      pollIntervalMs: Long = 10,
  )(testCode: => T): T = {
    eventually(timeUntilSuccess)(testCode)
    always(durationOfSuccess, pollIntervalMs)(testCode)
  }

  /** Keeps evaluating `testCode` until it succeeds or a timeout occurs.
    * @throws org.scalatest.exceptions.TestFailedException
    *   if `testCode` keeps throwing such an exception even after `timeout`
    * @throws java.lang.Throwable
    *   if `testCode` throws any other throwable
    * @throws java.lang.IllegalArgumentException
    *   if `timeUntilSuccess` is negative
    */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Return",
    )
  )
  def eventually[T](
      timeUntilSuccess: FiniteDuration = 20.seconds,
      maxPollInterval: FiniteDuration = 5.seconds,
      retryOnTestFailuresOnly: Boolean = true,
  )(testCode: => T): T = {
    require(
      timeUntilSuccess >= Duration.Zero,
      s"The timeout must not be negative, but is $timeUntilSuccess",
    )
    val deadline = timeUntilSuccess.fromNow
    var sleepMs = 1L
    def sleep(): Unit = {
      val timeLeft = deadline.timeLeft.toMillis max 0
      Threading.sleep(sleepMs min timeLeft)
      sleepMs = (sleepMs * 2) min maxPollInterval.toMillis
    }
    while (deadline.hasTimeLeft()) {
      try {
        return testCode
      } catch {
        case _: TestFailedException =>
          sleep()
        case _: Throwable if !retryOnTestFailuresOnly =>
          sleep()
      }
    }
    testCode // try one last time and throw exception, if assertion keeps failing
  }

  // Uses SymbolicCrypto for the configured crypto schemes
  lazy val defaultStaticSynchronizerParameters: StaticSynchronizerParameters =
    defaultStaticSynchronizerParametersWith()

  def defaultStaticSynchronizerParametersWith(
      protocolVersion: ProtocolVersion = testedProtocolVersion
  ): StaticSynchronizerParameters = StaticSynchronizerParameters(
    requiredSigningSpecs = SymbolicCryptoProvider.supportedSigningSpecs,
    requiredEncryptionSpecs = SymbolicCryptoProvider.supportedEncryptionSpecs,
    requiredSymmetricKeySchemes = SymbolicCryptoProvider.supportedSymmetricKeySchemes,
    requiredHashAlgorithms = SymbolicCryptoProvider.supportedHashAlgorithms,
    requiredCryptoKeyFormats = SymbolicCryptoProvider.supportedCryptoKeyFormats,
    requiredSignatureFormats = SymbolicCryptoProvider.supportedSignatureFormats,
    protocolVersion = protocolVersion,
  )

  lazy val testedProtocolVersion: ProtocolVersion =
    tryGetProtocolVersionFromEnv.getOrElse(ProtocolVersion.latest)

  lazy val testedStaticSynchronizerParameters: StaticSynchronizerParameters =
    defaultStaticSynchronizerParametersWith(testedProtocolVersion)

  lazy val testedProtocolVersionValidation: ProtocolVersionValidation =
    ProtocolVersionValidation(testedProtocolVersion)

  lazy val testedReleaseProtocolVersion: ReleaseProtocolVersion = ReleaseProtocolVersion(
    testedProtocolVersion
  )

  lazy val CantonExamplesPath: String = getResourcePath("CantonExamples.dar")
  lazy val CantonTestsPath: String = getResourcePath("CantonTests-3.3.0.dar")
  lazy val CantonTestsDevPath: String = getResourcePath("CantonTestsDev-3.3.0.dar")
  lazy val CantonLfDev: String = getResourcePath("CantonLfDev-3.3.0.dar")
  lazy val CantonLfV21: String = getResourcePath("CantonLfV21-3.3.0.dar")
  lazy val PerformanceTestPath: String = getResourcePath("PerformanceTest.dar")
  lazy val DamlScript3TestFilesPath: String = getResourcePath("DamlScript3TestFiles-3.3.0.dar")
  lazy val DamlTestFilesPath: String = getResourcePath("DamlTestFiles-3.3.0.dar")
  lazy val DamlTestLfDevFilesPath: String = getResourcePath("DamlTestLfDevFiles-3.3.0.dar")

  def getResourcePath(name: String): String =
    Option(getClass.getClassLoader.getResource(name))
      .map(_.getPath)
      .getOrElse(throw new IllegalArgumentException(s"Cannot find resource $name"))

  /** @return
    *   Parsed protocol version if found in environment variable `CANTON_PROTOCOL_VERSION`
    * @throws java.lang.RuntimeException
    *   if the given parameter cannot be parsed to a protocol version
    */
  protected def tryGetProtocolVersionFromEnv: Option[ProtocolVersion] = sys.env
    .get("CANTON_PROTOCOL_VERSION")
    .map(ProtocolVersion.tryCreate)
}

trait BaseTestWordSpec extends BaseTest with AnyWordSpecLike {
  def checkAllLaws(name: String, ruleSet: Laws#RuleSet)(implicit position: Position): Unit =
    for ((id, prop) <- ruleSet.all.properties) {
      (name + "." + id) in {
        CheckerAsserting.assertingNatureOfAssertion.check(
          prop,
          Test.Parameters.default,
          Prettifier.default,
          position,
        )
      }
    }
}
