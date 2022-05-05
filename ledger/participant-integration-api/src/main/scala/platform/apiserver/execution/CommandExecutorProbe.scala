package com.daml.platform.apiserver.execution

import com.daml.concurrent.ExecutionContext
import com.daml.error.definitions.ErrorCause
import com.daml.ledger.api.domain.Commands
import com.daml.ledger.configuration.Configuration
import com.daml.lf.crypto
import com.daml.logging.LoggingContext
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

object CommandExecutorProbe {
  def apply(delegate: CommandExecutor): (CommandExecutor, () => Int) = {
    val probe = new AtomicInteger(0)
    val executor = new CommandExecutorProbe(delegate, probe)
    (executor, probe.get)
  }
}

private class CommandExecutorProbe(delegate: CommandExecutor, probe: AtomicInteger)
    extends CommandExecutor {

  override def execute(
      commands: Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {

    probe.incrementAndGet()
    val future = delegate.execute(commands, submissionSeed, ledgerConfiguration)
    future.onComplete(_ => probe.decrementAndGet())(ExecutionContext.parasitic)
    future
  }: Future[Either[ErrorCause, CommandExecutionResult]]
}
