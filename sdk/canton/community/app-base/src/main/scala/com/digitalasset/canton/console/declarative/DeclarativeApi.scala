// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.declarative.DeclarativeApi.UpdateResult
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.metrics.DeclarativeApiMetrics
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.{NoExceptionRetryPolicy, Success}
import com.digitalasset.canton.util.{MonadUtil, retry}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait DeclarativeApiHandle[Cfg] {
  def newConfig(cfg: Cfg)(implicit traceContext: TraceContext): Boolean
  def poke()(implicit traceContext: TraceContext): Unit
  def invalidConfig()(implicit traceContext: TraceContext): Unit
}

object DeclarativeApiHandle {
  def mapConfig[S, T](handle: DeclarativeApiHandle[T], map: S => T): DeclarativeApiHandle[S] =
    new DeclarativeApiHandle[S] {
      override def newConfig(cfg: S)(implicit traceContext: TraceContext): Boolean =
        handle.newConfig(map(cfg))
      override def poke()(implicit traceContext: TraceContext): Unit = handle.poke()
      override def invalidConfig()(implicit traceContext: TraceContext): Unit =
        handle.invalidConfig()
    }
}

/** Base classes to synchronize a state in a file with the state managed through the admin api
  *
  * The admin api for Canton is imperative, which allows us to manage large states, but it makes
  * simple deployments more complex. In such cases, the declarative API allows to define the desired
  * state in a config file, with a process that will in the background attempt to change the node
  * state accordingly.
  */
trait DeclarativeApi[Cfg, Prep] extends DeclarativeApiHandle[Cfg] with NamedLogging {

  private val startLimit = PositiveInt.tryCreate(1000)
  // state to orchestrate trade safe invocation of runSync
  // the tuple means (shouldRun, isRunning)
  // if runSync is called concurrently, the runState reference will be used to ensure
  // that we only run once. this can happen if someone edits the file during a sync
  // triggered by a synchronizer connection being added via the console commands
  // we don't protect using blocking calls but just use atomic operations.
  // there is a small caveat: normally, adding a synchronizer connection will trigger a sync,
  // but if a sync is already running, the adding of the synchronizer connection will not wait
  // for the sync to finish.
  private val runState = new AtomicReference[(Boolean, Boolean)]((false, false))

  protected def name: String
  protected def metrics: DeclarativeApiMetrics
  protected def closeContext: CloseContext
  protected def activeAdminToken: Option[CantonAdminToken]
  protected def consistencyTimeout: config.NonNegativeDuration
  protected implicit def executionContext: ExecutionContext

  protected def prepare(config: Cfg)(implicit traceContext: TraceContext): Either[String, Prep]

  protected lazy val currentConfig = new AtomicReference[Cfg]()
  private val needsSync: AtomicBoolean = new AtomicBoolean(true)
  override def newConfig(cfg: Cfg)(implicit traceContext: TraceContext): Boolean = {
    currentConfig.set(cfg)
    if (activeAdminToken.nonEmpty) {
      val ret = runSync()
      if (!ret)
        needsSync.set(true)
      ret
    } else {
      // if node is passive, we don't try to run the sync
      needsSync.set(true)
      true
    }
  }

  override def poke()(implicit traceContext: TraceContext): Unit =
    if (needsSync.compareAndSet(true, false)) {
      if (!runSync()) needsSync.set(true)
    }

  override def invalidConfig()(implicit traceContext: TraceContext): Unit = {
    needsSync.set(false)
    metrics.errors.updateValue(-1)
  }

  /** Generic self-consistency update runner
    *
    * This function can be used to determine and apply a set of changes to a system. We generally
    * refer to the state on a per key basis (e.g. user name, party name, dar-id etc). Each key
    * points to a value. So effectively, we compare for each key whether the values match and if
    * they don't, we update the state such that they do.
    *
    * @param name
    *   the name of the operation (e.g. dars, parties)*
    * @param removeExcess
    *   if true, then items which are on the node but not found in the config will be removed. this
    *   is off by default to ensure that state that has been added via the admin api is not deleted.
    * @param checkSelfConsistent
    *   if true (default), then the system will check whether it successfully updated the rows by
    *   checking the state after an update and verifying that it matches now the "wanted" state.
    * @param want
    *   the state as desired
    * @param fetch
    *   a function to fetch the current state. the function takes a limit argument. If the fetch
    *   returns the max limit, we assume that we need to fetch more. The system will start to emit
    *   warnings but increase the fetch limit to remain functional but to warn the user that they
    *   are reaching the limits of managing a node through a config file. This is used when
    *   removeExcess is configured
    * @param get
    *   a function to fetch the current state for a specific key. this is used for scalability
    *   reasons to avoid fetching the entire state.
    * @param add
    *   if the runner finds a value (K,V) in the want set which is not in the have set, it will
    *   invoke the add function.
    * @param upd
    *   if the runner finds a value (K,V) where the want value V differs from the current have value
    *   V', then the update function is invoked. The first V is the desired, the second is the
    *   existing state.
    * @param rm
    *   if removeExcess is set to true and the runner finds a K in the have set but not in the want
    *   set, it will invoke the rm function to remove K.
    * @param compare
    *   supply distinct comparison function that checks whether an update is necessary (in case x:V
    *   \== y:V) needs adjustment
    * @param await
    *   the await function can be used to wait for a specific result after the update cycle
    *   (concretely, we wait for the ledger api server to observe the parties before we start adding
    *   users that refer to these parties)
    * @param onlyCheckKeys:
    *   if true, then we won't update values if they differ from the desired one
    */
  protected def run[K, V](
      name: String,
      removeExcess: Boolean,
      checkSelfConsistent: Boolean,
      want: Seq[(K, V)],
      fetch: PositiveInt => Either[String, Seq[
        (K, V)
      ]],
      get: K => Either[String, Option[V]],
      add: (K, V) => Either[String, Unit],
      upd: (K, V, V) => Either[String, Unit],
      rm: (K, V) => Either[String, Unit],
      compare: Option[(V, V) => Boolean] = None,
      await: Option[Seq[K] => Either[String, Boolean]] = None,
      onlyCheckKeys: Boolean = false,
  )(implicit traceContext: TraceContext): Either[String, UpdateResult] = {

    def wrapResult(id: K, op: String, result: => Either[String, Unit]): Either[Unit, Boolean] = {
      logger.info(s"$op $name $id")
      result.map(_ => true).leftMap { err =>
        logger.warn(s"Operation=$op failed for $name with key=$id: $err")
        ()
      }
    }

    def update(item: (K, V, Option[V])): Either[Unit, Boolean] = item match {
      // Add items
      case (id, desired, None) =>
        logger.info(
          s"adding new $name: $id to $desired"
        )
        wrapResult(id, "add", add(id, desired))
      // Update items
      case (id, desired, Some(existing))
          if !onlyCheckKeys && !compare.map(_(desired, existing)).getOrElse(desired == existing) =>
        logger.info(
          s"updating existing $name: $id to $desired"
        )
        wrapResult(id, "update", upd(id, desired, existing))
      // No change
      case (id, _, Some(_)) =>
        logger.debug(s"No change for $name $id")
        Right(false)
    }

    // positive cycle
    def addOrUpdate(): Either[String, UpdateResult] =
      MonadUtil.foldLeftM(UpdateResult(), want) { case (acc, (k, v)) =>
        get(k).map(current => acc.accumulate(addOrUpdate = true)(update((k, v, current))))
      }

    // negative cycle
    def removeItems(result: UpdateResult): Either[String, UpdateResult] = if (!removeExcess)
      Right(result)
    else {
      for {
        all <- fetchAll(fetch).map(_.toMap)
      } yield {
        val toRemove = all.keySet.diff(want.map(_._1).toSet)
        toRemove.foldLeft(result) { case (acc, id) =>
          acc.accumulate(addOrUpdate = false)(wrapResult(id, "remove", rm(id, all(id))))
        }
      }
    }

    // check self-consistency check to detect if an API sync did not work as expected
    def runSelfConsistencyCheck(result: UpdateResult): Either[String, Unit] = if (
      checkSelfConsistent && result.failed == 0
    ) {
      val consistentE =
        want
          .traverse { case (k, v) => get(k).map(vn => (k, v, vn)) }
          .map(_.map {
            case (k, _, None) =>
              logger.error(s"$name not found after sync: $k")
              false
            case (k, desired, Some(stored)) if desired != stored && !onlyCheckKeys =>
              logger.error(s"Mismatching $name after sync: $k, desired=$desired, stored=$stored")
              false
            case _ => true
          }
            .forall(identity))

      val noExcessE = if (removeExcess) {
        fetchAll(fetch).map { haveMap =>
          haveMap
            .map(_._1)
            .toSet
            .diff(want.map(_._1).toSet)
            .map { k =>
              logger.error(s"$name not removed after sync: $k")
              false
            }
            .forall(identity)
        }
      } else Right(true)

      for {
        consistent <- consistentE
        _ <- Either.cond(
          consistent,
          (),
          s"Self-consistency check failed for $name, due to want items not matching",
        )
        noExcess <- noExcessE
        _ <- Either.cond(
          noExcess,
          (),
          s"Self-consistency check failed for $name, as some items were not removed",
        )
      } yield ()

    } else Either.unit

    def waitUntilItemsAreRegistered(): Either[String, Unit] = await
      .map { awaiter =>
        withRetry(
          awaiter(want.map(_._1)),
          s"Changes of type=$name consistency check",
        )
      }
      .getOrElse(Either.unit)

    if (want.nonEmpty || removeExcess) {
      for {
        resultAfterAddOrUpdate <- addOrUpdate()
        resultAfterAll <- removeItems(resultAfterAddOrUpdate)
        _ <- waitUntilItemsAreRegistered()
        _ <- runSelfConsistencyCheck(resultAfterAll)
      } yield resultAfterAll
    } else Right(UpdateResult())

  }

  protected def withRetry(
      action: => Either[String, Boolean],
      description: String,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Either[String, Unit] = {
    implicit val success: Success[Either[String, Boolean]] = Success(_.contains(true))
    consistencyTimeout
      .await(description)(
        retry
          .Pause(
            logger,
            closeContext.context,
            maxRetries = 10,
            delay = consistencyTimeout.asFiniteApproximation.div(13),
            operationName = "waiting for " + description,
          )
          .apply(Future.successful(action), NoExceptionRetryPolicy)
      )
      .flatMap(Either.cond(_, (), s"Retry failed after $consistencyTimeout for $description"))
  }

  protected final def fetchAll[S](
      request: PositiveInt => Either[String, Seq[S]],
      lessThan: PositiveInt = startLimit,
  ): Either[String, Seq[S]] =
    request(lessThan).flatMap { result =>
      if (result.sizeIs < lessThan.value) Right(result)
      else {
        noTracingLogger.warn(
          "Note that the fetch limit was reached and had to be extended. You are pushing the " +
            "declarative API to its limits and should consider managing the state through API calls instead."
        )
        fetchAll(request, lessThan * PositiveInt.two)
      }
    }

  protected def sync(config: Cfg, prep: Prep)(implicit
      traceContext: TraceContext
  ): Either[String, UpdateResult]

  /** Trigger a synchronisation
    *
    * This method is thread safe as it will only run one synchronisation at a time (managed through
    * an atomic reference). If no sync is currently running, the method will return only when the
    * sync is finished. If a sync is already running, the method will return immediately.
    *
    * Concurrent sync runs are very rare, but can happen if the file is changed while a manual
    * synchronizer connection is being added via the console commands or if the sync is currently
    * failing due to a config issue or a bug.
    */
  @tailrec
  final def runSync()(implicit
      traceContext: TraceContext
  ): Boolean = if (activeAdminToken.isEmpty) {
    if (runState.get()._1) {
      logger.debug("Not running declarative API sync because the node is passive")
    }
    false
  } else {
    val (_, isRunningAlready) = runState.getAndUpdate {
      case (_, true) =>
        (true, true) // if already running, we need to run again
      case (_, false) =>
        (false, true) // otherwise, we run now
    }
    if (isRunningAlready) {
      logger.debug("Already running a sync. Scheduling another one")
      false // don't mark as a successful run
    } else {
      val ret =
        try {
          def withErrorCode[R](step: String, code: Int)(e: Either[String, R]) = e.leftMap { c =>
            logger.warn(s"State synchronisation step $step failed with $c")
            metrics.errors.updateValue(code)
            c
          }
          // first, prepare everything that must be in place for the sync
          val config = currentConfig.get()
          (for {
            prep <- withErrorCode("prepare", -2)(prepare(config))
            itemsUpdated <- withErrorCode("sync", -3)(sync(config, prep))
          } yield {
            logger.info(
              s"Completed state update with items=${itemsUpdated.items}, updated=${itemsUpdated.updated}, removed=${itemsUpdated.removed}, failed=${itemsUpdated.failed}"
            )
            metrics.items.updateValue(itemsUpdated.items)
            metrics.errors.updateValue(itemsUpdated.failed)
          }).isRight
        } catch {
          case NonFatal(e) =>
            metrics.errors.updateValue(-9)
            logger.error("Failed to run background update due to unhandled exception", e)
            false
        }
      val (shouldReRun, _) = runState.getAndUpdate { case (needsToRun, _) =>
        (needsToRun, false)
      }
      if (ret && shouldReRun) {
        logger.debug("Scheduling another run")
        runSync()
      } else ret
    }
  }

}

object DeclarativeApi {

  final case class UpdateResult(
      failed: Int = 0,
      updated: Int = 0,
      removed: Int = 0,
      items: Int = 0,
  ) {
    def accumulate(addOrUpdate: Boolean)(result: Either[Unit, Boolean]): UpdateResult = {
      val tmp = (result match {
        case Left(_) => copy(failed = failed + 1)
        case Right(true) =>
          if (addOrUpdate) copy(updated = updated + 1)
          else copy(removed = removed + 1)
        case Right(false) => this
      })
      if (addOrUpdate)
        tmp.copy(items = items + 1)
      else tmp
    }

    def merge(other: UpdateResult): UpdateResult = UpdateResult(
      failed = failed + other.failed,
      updated = updated + other.updated,
      removed = removed + other.removed,
      items = items + other.items,
    )
  }

}
