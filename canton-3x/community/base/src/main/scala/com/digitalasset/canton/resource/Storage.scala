// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.{Chain, EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.{Eval, Functor, Monad}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveNumeric}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  CloseableHealthComponent,
  ComponentHealthState,
}
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.metrics.{DbQueueMetrics, DbStorageMetrics}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey, LfHash}
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Oracle, Postgres}
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.StorageFactory.StorageCreationException
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.{DbDeserializationException, DbSerializationException}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.RetryEither
import com.digitalasset.canton.util.retry.RetryUtil.DbExceptionRetryable
import com.digitalasset.canton.util.{Thereafter, *}
import com.digitalasset.canton.{LfPackageId, LfPartyId}
import com.google.protobuf.ByteString
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.Logger
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException
import org.slf4j.event.Level
import slick.SlickException
import slick.dbio.*
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton
import slick.jdbc.canton.*
import slick.jdbc.{ActionBasedSQLInterpolation as _, SQLActionBuilder as _, *}
import slick.lifted.Aliases
import slick.util.{AsyncExecutor, AsyncExecutorWithMetrics, ClassLoaderUtil}

import java.io.ByteArrayInputStream
import java.sql.{Blob, SQLException, Statement}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import javax.sql.rowset.serial.SerialBlob
import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** Storage resources (e.g., a database connection pool) that must be released on shutdown.
  *
  * The only common functionality defined is the shutdown through AutoCloseable.
  * Using storage objects after shutdown is unsafe; thus, they should only be closed when they're ready for
  * garbage collection.
  */
sealed trait Storage extends CloseableHealthComponent with AtomicHealthComponent {
  self: NamedLogging =>

  /** Indicates if the storage instance is active and ready to perform updates/writes. */
  def isActive: Boolean

}

trait StorageFactory {
  def config: StorageConfig

  /** Throws an exception in case of errors or shutdown during storage creation. */
  def tryCreate(
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Storage =
    create(
      connectionPoolForParticipant,
      logQueryCost,
      clock,
      scheduler,
      metrics,
      timeouts,
      loggerFactory,
    )
      .valueOr(err => throw new StorageCreationException(err))
      .onShutdown(throw new StorageCreationException("Shutdown during storage creation"))

  def create(
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, Storage]
}

object StorageFactory {
  class StorageCreationException(message: String) extends RuntimeException(message)
}

class CommunityStorageFactory(val config: CommunityStorageConfig) extends StorageFactory {
  override def create(
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, Storage] =
    config match {
      case CommunityStorageConfig.Memory(_, _) =>
        EitherT.rightT(new MemoryStorage(loggerFactory, timeouts))
      case db: DbConfig =>
        DbStorageSingle
          .create(
            db,
            connectionPoolForParticipant,
            logQueryCost,
            clock,
            scheduler,
            metrics,
            timeouts,
            loggerFactory,
          )
          .widen[Storage]
    }
}

class MemoryStorage(
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Storage
    with NamedLogging
    with FlagCloseable {
  override val name = "memory_storage"
  override val initialHealthState: ComponentHealthState = ComponentHealthState.Ok()

  override def isActive: Boolean = true
}

trait DbStore extends FlagCloseable with NamedLogging with HasCloseContext {
  protected val storage: DbStorage
}

trait DbStorage extends Storage { self: NamedLogging =>

  val profile: DbStorage.Profile
  val dbConfig: DbConfig
  protected val logOperations: Boolean

  override val name: String = DbStorage.healthName

  override def initialHealthState: ComponentHealthState =
    ComponentHealthState.NotInitializedState

  object DbStorageConverters {

    /** We use `bytea` in Postgres and Oracle and `binary large object` in H2.
      * The reason is that starting from version 2.0, H2 imposes a limit of 1M
      * for the size of a `bytea`. Hence, depending on the profile, SetParameter
      * and GetResult for `Array[Byte]` are different for H2 and Oracle/Postgres.
      */
    private lazy val byteArraysAreBlobs = profile match {
      case _: H2 => true
      case _ => false
    }

    /** We use .setBinaryStream for Oracle instead of .setBytes, due to an ORA-03146 code which happens sometimes with:
      * - BLOB sql field
      * - MERGE query
      * - new field value of size > 32K
      *
      * Canton #11644, support case #4136
      * Solution is based on:
      * https://stackoverflow.com/questions/7794197/inserting-byte-array-as-blob-in-oracle-database-getting-ora-01460-unimplement
      */
    private lazy val bytesArraysSetBinaryStream = profile match {
      case _: Oracle => true
      case _ => false
    }

    implicit val setParameterByteArray: SetParameter[Array[Byte]] = (v, pp) =>
      if (byteArraysAreBlobs) pp.setBlob(bytesToBlob(v))
      else if (bytesArraysSetBinaryStream) {
        val npos = pp.pos + 1
        pp.ps.setBinaryStream(npos, new ByteArrayInputStream(v))
        pp.pos = npos
      } else {
        pp.setBytes(v)
      }

    implicit val getResultByteArray: GetResult[Array[Byte]] =
      if (byteArraysAreBlobs) GetResult(r => blobToBytes(r.nextBlob()))
      else GetResult(_.nextBytes())

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val setParameterOptionalByteArray: SetParameter[Option[Array[Byte]]] = (v, pp) =>
      if (byteArraysAreBlobs) pp.setBlobOption(v.map(bytesToBlob))
      else // Postgres profile will fail with setBytesOption if given None. Easy fix is to use setBytes with null instead
        pp.setBytes(v.orNull)

    implicit val getResultOptionalByteArray: GetResult[Option[Array[Byte]]] =
      if (byteArraysAreBlobs)
        GetResult(_.nextBlobOption().map(blobToBytes))
      else
        GetResult(_.nextBytesOption())

    private def blobToBytes(blob: Blob): Array[Byte] =
      if (blob.length() == 0) Array[Byte]() else blob.getBytes(1, blob.length().toInt)

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private def bytesToBlob(bytes: Array[Byte]): SerialBlob =
      if (bytes != null) new SerialBlob(bytes) else null
  }

  /** Returns database specific limit [offset] clause.
    * Safe to use in a select slick query with #$... interpolation
    */
  def limit(numberOfItems: Int, skipItems: Long = 0L): String = profile match {
    case _: DbStorage.Profile.Oracle =>
      (if (skipItems != 0L) s"offset $skipItems rows "
       else "") + s"fetch next $numberOfItems rows only"
    case _ => s"limit $numberOfItems" + (if (skipItems != 0L) s" offset $skipItems" else "")
  }

  /** Automatically performs #$ interpolation for a call to `limit` */
  def limitSql(numberOfItems: Int, skipItems: Long = 0L): SQLActionBuilder = {
    sql" #${limit(numberOfItems, skipItems)} "
  }

  /** Runs the given `query` transactionally with synchronous commit replication if
    * the database provides the ability to configure synchronous commits per transaction.
    *
    * Currently only Postgres supports this.
    */
  def withSyncCommitOnPostgres[A, E <: Effect](
      query: DBIOAction[A, NoStream, E]
  ): DBIOAction[A, NoStream, Effect.Write with E with Effect.Transactional] = {
    import profile.DbStorageAPI.jdbcActionExtensionMethods
    profile match {
      case _: Profile.Postgres =>
        val syncCommit = sqlu"set local synchronous_commit=on"
        syncCommit.andThen(query).transactionally
      case _: Profile.H2 | _: Profile.Oracle =>
        // Don't do anything for H2/Oracle. According to our docs it is up to the user to enforce synchronous replication.
        // Any changes here are on a best-effort basis, but we won't guarantee they will be sufficient.
        query.transactionally
    }
  }

  def metrics: DbStorageMetrics

  // Size of the pool available for writing, it may be a combined r/w pool or a dedicated write pool
  def threadsAvailableForWriting: PositiveInt

  lazy val api: profile.DbStorageAPI.type = profile.DbStorageAPI
  lazy val converters: DbStorageConverters.type = DbStorageConverters

  protected implicit def ec: ExecutionContext
  protected def timeouts: ProcessingTimeout

  private val defaultMaxRetries = retry.Forever

  protected def run[A](action: String, operationName: String, maxRetries: Int)(
      body: => Future[A]
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] = {
    if (logOperations) {
      logger.debug(s"started $action: $operationName")
    }
    import Thereafter.syntax.*
    implicit val success: retry.Success[A] = retry.Success.always
    retry
      .Backoff(
        logger,
        closeContext.context,
        maxRetries = maxRetries,
        initialDelay = 50.milliseconds,
        maxDelay = timeouts.storageMaxRetryInterval.unwrap,
        operationName = operationName,
        suspendRetries = Eval.always(
          if (isActive) Duration.Zero
          else dbConfig.parameters.connectionTimeout.asFiniteApproximation
        ),
      )
      .apply(body, DbExceptionRetryable)
      .thereafter { _ =>
        if (logOperations) {
          logger.debug(s"completed $action: $operationName")
        }
      }

  }

  protected[canton] def runRead[A](
      action: DbAction.ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A]

  protected[canton] def runWrite[A](
      action: DbAction.All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A]

  /** Read-only query, possibly transactional */
  def query[A](
      action: DbAction.ReadTransactional[A],
      operationName: String,
      maxRetries: Int = defaultMaxRetries,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    runRead(action, operationName, maxRetries)

  def sequentialQueryAndCombine[A](
      actions: immutable.Iterable[DbAction.ReadOnly[immutable.Iterable[A]]],
      operationName: String,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[immutable.Iterable[A]] =
    if (actions.nonEmpty) {
      MonadUtil.foldLeftM(actions.iterableFactory.empty[A], actions) { case (acc, action) =>
        query(action, operationName)(traceContext, closeContext).map(acc ++ _)
      }
    } else Future.successful(immutable.Iterable.empty[A])

  def querySingle[A](
      action: DBIOAction[Option[A], NoStream, Effect.Read with Effect.Transactional],
      operationName: String,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): OptionT[Future, A] =
    OptionT(query(action, operationName))

  /** Write-only action, possibly transactional
    *
    * The action must be idempotent because it may be retried multiple times.
    * Only the result of the last retry will be reported.
    * If the action reports the number of rows changed,
    * this number may be lower than actual number of affected rows
    * because updates from earlier retries are not accounted.
    */
  def update[A](
      action: DBIOAction[A, NoStream, Effect.Write with Effect.Transactional],
      operationName: String,
      maxRetries: Int = defaultMaxRetries,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    runWrite(action, operationName, maxRetries)

  /** Write-only action, possibly transactional
    * The action must be idempotent because it may be retried multiple times.
    */
  def update_(
      action: DBIOAction[_, NoStream, Effect.Write with Effect.Transactional],
      operationName: String,
      maxRetries: Int = defaultMaxRetries,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] =
    runWrite(action, operationName, maxRetries).map(_ => ())

  /** Query and update in a single action.
    *
    * Note that the action is not transactional by default, but can be made so
    * via using `queryAndUpdate(action.transactionally..withTransactionIsolation(Serializable), "name")`
    *
    * The action must be idempotent because it may be retried multiple times.
    * Only the result of the last retry will be reported.
    * If the action reports the number of rows changed,
    * this number may be lower than actual number of affected rows
    * because updates from earlier retries are not accounted.
    */
  def queryAndUpdate[A](
      action: DBIOAction[A, NoStream, Effect.All],
      operationName: String,
      maxRetries: Int = defaultMaxRetries,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    runWrite(action, operationName, maxRetries)

}

object DbStorage {
  val healthName: String = "db-storage"

  final case class PassiveInstanceException(reason: String)
      extends RuntimeException(s"DbStorage instance is not active: $reason")

  sealed trait Profile extends Product with Serializable with PrettyPrinting {
    def jdbc: JdbcProfile

    object DbStorageAPI extends Aliases {
      lazy val jdbcProfile = jdbc

      implicit def jdbcActionExtensionMethods[E <: Effect, R, S <: NoStream](
          a: DBIOAction[R, S, E]
      ): jdbcProfile.JdbcActionExtensionMethods[E, R, S] =
        new jdbcProfile.JdbcActionExtensionMethods[E, R, S](a)

      implicit def actionBasedSQLInterpolationCanton(
          s: StringContext
      ): ActionBasedSQLInterpolation =
        new ActionBasedSQLInterpolation(s)
    }
  }

  /** Indicate if the Db profile supports DB locks. */
  sealed trait DbLockSupport extends Product with Serializable

  object Profile {
    final case class H2(jdbc: H2Profile) extends Profile {
      override def pretty: Pretty[H2] = prettyOfObject[H2]
    }
    final case class Oracle(jdbc: OracleProfile) extends Profile with DbLockSupport {
      override def pretty: Pretty[Oracle] = prettyOfObject[Oracle]
    }
    final case class Postgres(jdbc: PostgresProfile) extends Profile with DbLockSupport {
      override def pretty: Pretty[Postgres] = prettyOfObject[Postgres]
    }
  }

  object DbAction {
    type ReadOnly[+A] = DBIOAction[A, NoStream, Effect.Read]
    type ReadTransactional[+A] = DBIOAction[A, NoStream, Effect.Read with Effect.Transactional]
    type WriteOnly[+A] = DBIOAction[A, NoStream, Effect.Write]
    type All[+A] = DBIOAction[A, NoStream, Effect.All]

    /** Use `.andThen(unit)` instead of `.map(_ => ())` for DBIOActions
      * because `andThen` doesn't need an execution context and can thus be executed more efficiently
      * according to the slick documentation https://scala-slick.org/doc/3.3.3/dbio.html#sequential-execution
      */
    val unit: DBIOAction[Unit, NoStream, Effect] = DBIOAction.successful(())
  }

  object Implicits {

    implicit def functorDBIO[E <: Effect](implicit
        ec: ExecutionContext
    ): Functor[DBIOAction[*, NoStream, E]] = new Functor[DBIOAction[*, NoStream, E]] {
      def map[A, B](fa: DBIOAction[A, NoStream, E])(f: A => B): DBIOAction[B, NoStream, E] =
        fa.map(f)
    }

    implicit def monadDBIO[E <: Effect](implicit
        ec: ExecutionContext
    ): Monad[DBIOAction[*, NoStream, E]] =
      new Monad[DBIOAction[*, NoStream, E]] {
        def flatMap[A, B](fa: DBIOAction[A, NoStream, E])(
            f: A => DBIOAction[B, NoStream, E]
        ): DBIOAction[B, NoStream, E] = fa.flatMap(f)

        def tailRecM[A, B](a: A)(
            f: A => DBIOAction[Either[A, B], NoStream, E]
        ): DBIOAction[B, NoStream, E] =
          f(a).flatMap(_.fold(tailRecM(_)(f), pure))

        def pure[A](x: A): DBIOAction[A, NoStream, E] = DBIOAction.successful(x)
      }

    implicit val getResultUuid: GetResult[UUID] = GetResult(r => UUID.fromString(r.nextString()))
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString")) // UUIDs are length-limited
    implicit val setParameterUuid: SetParameter[UUID] = (v, pp) => pp.setString(v.toString)

    @SuppressWarnings(Array("com.digitalasset.canton.SlickString")) // LfPartyIds are length-limited
    implicit val setParameterLfPartyId: SetParameter[LfPartyId] = (v, pp) => pp.setString(v)
    implicit val getResultLfPartyId: GetResult[LfPartyId] = GetResult(r => r.nextString()).andThen {
      LfPartyId
        .fromString(_)
        .valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize party ID: $err")
        )
    }

    implicit val absCoidGetResult: GetResult[LfContractId] = GetResult(r =>
      ProtoConverter
        .parseLfContractId(r.nextString())
        .fold(err => throw new DbDeserializationException(err.toString), Predef.identity)
    )
    implicit val absCoidSetParameter: SetParameter[LfContractId] =
      (c, pp) => pp >> c.toLengthLimitedString

    // We assume that the HexString of the hash of the global key will fit into 255 characters
    // Please consult the team, if you want to increase this limit
    implicit val lfGlobalKeySetParameter: SetParameter[LfGlobalKey] =
      (key: LfGlobalKey, pp: PositionedParameters) =>
        pp >> String255.tryCreate(key.hash.toHexString)
    implicit val lfHashGetResult: GetResult[LfHash] = GetResult { r =>
      LfHash
        .fromString(r.nextString())
        .valueOr(err =>
          throw new DbSerializationException(s"Failed to deserialize contract key hash: $err")
        )
    }

    // LfPackageIds are length-limited
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterLfPackageId: SetParameter[LfPackageId] = (v, pp) => pp.setString(v)
    implicit val getResultPackageId: GetResult[LfPackageId] =
      GetResult(r => r.nextString()).andThen {
        LfPackageId
          .fromString(_)
          .valueOr(err =>
            throw new DbDeserializationException(s"Failed to deserialize package id: $err")
          )
      }

    implicit val setParameterByteString: SetParameter[ByteString] = (bs, pp) =>
      pp.setBytes(bs.toByteArray)
    implicit val getResultByteString: GetResult[ByteString] =
      GetResult(r => ByteString.copyFrom(r.nextBytes()))

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val setParameterByteStringOption: SetParameter[Option[ByteString]] = (b, pp) =>
      // Postgres profile will fail with setBytesOption if given None. Easy fix is to use setBytes with null instead
      pp.setBytes(b.map(_.toByteArray).orNull)
    implicit val getResultByteStringOption: GetResult[Option[ByteString]] =
      GetResult(r => r.nextBytesOption().map(ByteString.copyFrom))

    implicit val setContractSalt: SetParameter[Option[Salt]] =
      (c, pp) => pp >> c.map(_.toProtoV0.toByteString)
    implicit val getContractSalt: GetResult[Option[Salt]] =
      implicitly[GetResult[Option[ByteString]]] andThen {
        _.map(byteString =>
          ProtoConverter
            .parse(
              // Even though it is versioned, the Salt is considered static
              // as it's used for authenticating contract ids which are immutable
              com.digitalasset.canton.crypto.v0.Salt.parseFrom,
              Salt.fromProtoV0,
              byteString,
            )
            .valueOr(err =>
              throw new DbDeserializationException(
                s"Failed to deserialize contract salt: $err"
              )
            )
        )
      }

    object BuilderChain {

      import scala.language.implicitConversions
      implicit def toSQLActionBuilderChain(a1: SQLActionBuilder): SQLActionBuilderChain =
        SQLActionBuilderChain(a1)
      implicit def fromSQLActionBuilderChain(op: SQLActionBuilderChain): SQLActionBuilder =
        op.toActionBuilder
      implicit def mapBuilderChain(op: Option[SQLActionBuilderChain]): Option[SQLActionBuilder] =
        op.map(_.toActionBuilder)
      implicit def mergeBuildersIntoChain(op: Seq[SQLActionBuilder]): SQLActionBuilderChain =
        SQLActionBuilderChain(op)

    }

  }

  class SQLActionBuilderChain(private val builders: Chain[SQLActionBuilder]) extends AnyVal {
    def ++(a2: SQLActionBuilder): SQLActionBuilderChain = {
      new SQLActionBuilderChain(builders.append(a2))
    }
    def ++(other: SQLActionBuilderChain): SQLActionBuilderChain = {
      new SQLActionBuilderChain(builders.concat(other.builders))
    }
    def ++(others: Seq[SQLActionBuilderChain]): SQLActionBuilderChain = {
      others.foldLeft(this)(_ ++ _)
    }
    def intercalate(item: SQLActionBuilder): SQLActionBuilderChain = {
      new SQLActionBuilderChain(
        builders.foldLeft(Chain.empty[SQLActionBuilder]) { case (acc, elem) =>
          if (acc.isEmpty) Chain.one(elem)
          else acc.append(item).append(elem)
        }
      )
    }
    def toActionBuilder: SQLActionBuilder = {
      val lst = builders.toList
      SQLActionBuilder(
        builders.flatMap(x => Chain.fromSeq(x.queryParts)).toList,
        (p: Unit, pp: PositionedParameters) => {
          lst.foreach(_.unitPConv.apply(p, pp))
        },
      )
    }
  }

  object SQLActionBuilderChain {
    def intercalate(lst: Seq[SQLActionBuilder], item: SQLActionBuilder): SQLActionBuilderChain =
      apply(lst).intercalate(item)
    def apply(item: SQLActionBuilder): SQLActionBuilderChain = new SQLActionBuilderChain(
      Chain.one(item)
    )
    def apply(items: Seq[SQLActionBuilder]): SQLActionBuilderChain = new SQLActionBuilderChain(
      Chain.fromSeq(items)
    )
  }

  /** Used to create unique connection pool names.
    */
  private val poolNameIndex: AtomicInteger = new AtomicInteger(0)

  def profile(config: DbConfig): Profile =
    config match {
      case _: H2DbConfig => H2(H2Profile)
      case _: PostgresDbConfig => Postgres(PostgresProfile)
      // TODO(i11009): assume unknown config is for oracle until we have proper oracle factory support
      case _ => Oracle(OracleProfile)
    }

  def createDatabase(
      config: DbConfig,
      numThreads: PositiveInt,
      metrics: Option[DbQueueMetrics] = None,
      logQueryCost: Option[QueryCostMonitoringConfig] = None,
      scheduler: Option[ScheduledExecutorService],
      forMigration: Boolean = false,
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast,
  )(
      loggerFactory: NamedLoggerFactory
  )(implicit closeContext: CloseContext): EitherT[UnlessShutdown, String, Database] = {
    val baseLogger = loggerFactory.getLogger(classOf[DbStorage])
    val logger = TracedLogger(baseLogger)

    TraceContext.withNewTraceContext { implicit traceContext =>
      // Must be called to set proper defaults in case of H2
      val configWithFallbacks: Config =
        DbConfig.configWithFallback(config)(
          numThreads,
          s"slick-${loggerFactory.threadName}-${poolNameIndex.incrementAndGet()}",
          logger,
        )

      val configWithMigrationFallbacks: Config = if (forMigration) {
        if (configWithFallbacks.hasPath("numThreads")) {
          // The migration requires at least 2 threads.
          val numThreads = configWithFallbacks.getInt("numThreads")
          if (numThreads < 2) {
            logger.info(
              s"Overriding numThreads from $numThreads to 2 for the purpose of db migration, as flyway needs at least 2 threads."
            )
          }
          configWithFallbacks.withValue(
            "numThreads",
            ConfigValueFactory.fromAnyRef(numThreads max 2),
          )
        } else {
          // no fallback needed as default value works with migration
          configWithFallbacks
        }
      } else {
        // no fallback requested
        configWithFallbacks
      }

      logger.debug(
        s"Initializing database storage with config: ${DbConfig.hideConfidential(configWithMigrationFallbacks)}"
      )

      RetryEither.retry[String, Database](
        maxRetries = retryConfig.maxRetries,
        waitInMs = retryConfig.retryWaitingTime.toMillis,
        operationName = functionFullName,
        logger = logger,
        retryLogLevel = retryConfig.retryLogLevel,
        failLogLevel = Level.ERROR,
      ) {
        for {
          db <- createJdbcBackendDatabase(
            configWithMigrationFallbacks,
            metrics,
            logQueryCost,
            scheduler,
            config.parameters,
            baseLogger,
          )
          _ <- Either
            .catchOnly[SQLException](db.createSession().close())
            .leftMap(err => show"Failed to create session with database: $err")
        } yield db
      }(ErrorLoggingContext.fromTracedLogger(logger), closeContext)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def createJdbcBackendDatabase(
      config: Config,
      metrics: Option[DbQueueMetrics],
      logQueryCost: Option[QueryCostMonitoringConfig],
      scheduler: Option[ScheduledExecutorService],
      parameters: DbParametersConfig,
      logger: Logger,
  ): Either[String, Database] = {
    // copy paste from JdbcBackend.forConfig
    import slick.util.ConfigExtensionMethods.*
    try {
      val classLoader: ClassLoader = ClassLoaderUtil.defaultClassLoader
      val source = JdbcDataSource.forConfig(config, null, "", classLoader)
      val poolName = config.getStringOr("poolName", "")
      val numThreads = config.getIntOr("numThreads", 20)
      val maxConnections = source.maxConnections.getOrElse(numThreads)
      val registerMbeans = config.getBooleanOr("registerMbeans", false)

      val executor = metrics match {
        // inject our own Canton Async executor with metrics
        case Some(m) =>
          new AsyncExecutorWithMetrics(
            poolName,
            numThreads,
            numThreads,
            queueSize = config.getIntOr("queueSize", 1000),
            maxConnections = maxConnections,
            registerMbeans = registerMbeans,
            logQueryCost = logQueryCost,
            metrics = m,
            scheduler = scheduler,
            warnOnSlowQueryO = parameters.warnOnSlowQuery.map(_.toInternal),
            warnInterval = parameters.warnOnSlowQueryInterval.toInternal,
            logger = logger,
          )
        case None =>
          AsyncExecutor(
            poolName,
            numThreads,
            numThreads,
            queueSize = config.getIntOr("queueSize", 1000),
            maxConnections = maxConnections,
            registerMbeans = registerMbeans,
          )
      }

      Right(JdbcBackend.Database.forSource(source, executor))
    } catch {
      case ex: SlickException => Left(show"Failed to setup database access: $ex")
      case ex: PoolInitializationException => Left(show"Failed to connect to database: $ex")
    }

  }

  /** Construct a bulk operation (e.g., insertion, deletion).
    * The operation must not return a result set!
    *
    * The returned action will run as a single big database transaction. If the execution of the transaction results
    * in deadlocks, you should order `values` according to some consistent order.
    *
    * The returned update counts are merely lower bounds to the number of affected rows
    * or SUCCESS_NO_INFO, because `Statement.executeBatch`
    * reports partial execution of a batch as a `BatchUpdateException` with
    * partial update counts therein and those update counts are not taken into consideration.
    *
    * This operation is idempotent if the statement is idempotent for each value.
    *
    * @throws java.lang.IllegalArgumentException if `statement` contains `"IGNORE_ROW_ON_DUPKEY_INDEX"`
    *                                            (See UpsertTestOracle for details.)
    */
  def bulkOperation[A](
      statement: String,
      values: immutable.Iterable[A],
      profile: Profile,
  )(
      setParams: PositionedParameters => A => Unit
  )(implicit loggingContext: ErrorLoggingContext): DBIOAction[Array[Int], NoStream, Effect.All] = {
    // Bail out if the statement contains IGNORE_ROW_ON_DUPKEY_INDEX, because update counts are known to be broken.
    // Use MERGE instead.
    // Ignoring update counts is not an option, because the JDBC driver reads them internally and may fail with
    // low-level exceptions.
    // See UpsertTestOracle for details.
    ErrorUtil.requireArgument(
      !statement.toUpperCase.contains("IGNORE_ROW_ON_DUPKEY_INDEX"),
      s"Illegal usage of bulkOperation with IGNORE_ROW_ON_DUPKEY_INDEX. $statement",
    )

    if (values.isEmpty) DBIOAction.successful(Array.empty)
    else {
      val action = SimpleJdbcAction { session =>
        ResourceUtil.withResource {
          session.connection.prepareStatement(statement)
        } { preparedStatement =>
          values.foreach { v =>
            val pp = new PositionedParameters(preparedStatement)
            setParams(pp)(v)
            preparedStatement.addBatch()
          }
          val updateCounts = preparedStatement.executeBatch()
          ErrorUtil.requireState(
            values.sizeIs == updateCounts.length,
            s"executeBatch returned ${updateCounts.length} update counts for ${values.size} rows. " +
              s"${updateCounts.mkString("Array(", ", ", ")")}",
          )
          ErrorUtil.requireState(
            updateCounts.forall(x => x == Statement.SUCCESS_NO_INFO || x >= 0),
            show"Batch operation update counts must be either ${Statement.SUCCESS_NO_INFO} or non-negative. " +
              show"Actual results were ${updateCounts.mkString("Array(", ", ", ")")}",
          )
          // The JDBC documentation demands that `executeBatch` throw a `BatchUpdateException`
          // if some updates in the batch fail and set the corresponding row update entry to `EXECUTE_FAILED`.
          // We check here for EXECUTE_BATCH not being reported without an exception to be super safe.
          updateCounts
        }
      }

      import profile.DbStorageAPI.*
      profile match {
        case _: Oracle =>
          // Oracle has the habit of not properly rolling back, if an error occurs and
          // there is no transaction (i.e. autoCommit = true). Further details on this can be found in UpsertTestOracle.
          action.transactionally

        case _ if values.sizeCompare(1) <= 0 =>
          // Disable auto-commit for better performance.
          action

        case _ => action.transactionally
      }
    }
  }

  /** Same as [[bulkOperation]] except that no update counts are returned. */
  def bulkOperation_[A](
      statement: String,
      values: immutable.Iterable[A],
      profile: Profile,
  )(
      setParams: PositionedParameters => A => Unit
  )(implicit loggingContext: ErrorLoggingContext): DBIOAction[Unit, NoStream, Effect.All] =
    bulkOperation(statement, values, profile)(setParams).andThen(DbAction.unit)

  /* Helper methods to make usage of EitherT[DBIO,] possible without requiring type hints */
  def dbEitherT[A, B](value: DBIO[Either[A, B]]): EitherT[DBIO, A, B] = EitherT[DBIO, A, B](value)
  def dbEitherT[A]: DbEitherTRight[A] = new DbEitherTRight[A]
  class DbEitherTRight[A] private[resource] {
    def apply[B](value: DBIO[B])(implicit ec: ExecutionContext): EitherT[DBIO, A, B] = {
      import DbStorage.Implicits.functorDBIO
      EitherT.right[A](value)
    }
  }

  /** Construct an in clause for a given field. If there are too many elements,
    * splits the clause into several ones. We need to split into several terms
    * because Oracle imposes a limit on the number of elements in an
    * in-clause (currently: 1000).
    *
    * @return An iterable of the grouped values and the in clause for the grouped values
    */
  @nowarn("cat=unused") // somehow, f is wrongly reported as unused by the compiler
  def toInClauses[T](
      field: String,
      values: NonEmpty[Seq[T]],
      maxValuesInSqlList: PositiveNumeric[Int],
  )(implicit f: SetParameter[T]): immutable.Iterable[(Seq[T], SQLActionBuilder)] = {
    import DbStorage.Implicits.BuilderChain.*

    values
      .grouped(maxValuesInSqlList.unwrap)
      .map { groupedValues =>
        val inClause = sql"#$field in (" ++
          groupedValues
            .map(value => sql"$value")
            .intercalate(sql", ") ++ sql")"

        groupedValues -> inClause.toActionBuilder
      }
      .to(immutable.Iterable)
  }

  def toInClauses_[T](
      field: String,
      values: NonEmpty[Seq[T]],
      maxValuesSqlInListSize: PositiveNumeric[Int],
  )(implicit f: SetParameter[T]): immutable.Iterable[SQLActionBuilder] =
    toInClauses(field, values, maxValuesSqlInListSize).map { case (_, builder) => builder }

  class DbStorageCreationException(message: String) extends RuntimeException(message)

  final case class RetryConfig(
      retryLogLevel: Level,
      retryWaitingTime: Duration,
      maxRetries: Int,
  )

  object RetryConfig {
    val failFast = RetryConfig(
      retryLogLevel = Level.WARN,
      retryWaitingTime = Duration(300, TimeUnit.MILLISECONDS),
      maxRetries = 3,
    )
    val failSlow = RetryConfig(
      retryLogLevel = Level.INFO,
      retryWaitingTime = 1.second,
      maxRetries = 30,
    )
    val forever = RetryConfig(
      retryLogLevel = Level.INFO,
      retryWaitingTime = Duration(1, TimeUnit.SECONDS),
      maxRetries = Int.MaxValue,
    )
  }
}

object Storage {
  def threadsAvailableForWriting(storage: Storage): PositiveInt = {
    storage match {
      case _: MemoryStorage => PositiveInt.one
      case dbStorage: DbStorage => dbStorage.threadsAvailableForWriting
    }
  }
}
