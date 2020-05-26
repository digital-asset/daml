// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.CommandSubmissionClient;
import com.daml.ledger.rxjava.LedgerClient;
import com.daml.ledger.rxjava.TransactionsClient;
import com.daml.ledger.rxjava.components.helpers.CommandsAndPendingSet;
import com.daml.ledger.rxjava.components.helpers.CreatedContract;
import com.daml.ledger.rxjava.components.helpers.Pair;
import com.daml.ledger.rxjava.util.FlowableLogger;
import com.google.rpc.Code;
import io.reactivex.*;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A Bot is an automation that reacts to changes in the ledger by submitting zero or more
 * commands. This class contains helpers to create bots. The main helper is
 * {@link #wire(String, LedgerClient, TransactionFilter, Function, Function)}.
 *
 */
public class Bot {

    private final static Logger logger = LoggerFactory.getLogger(Bot.class);

    static Flowable<LedgerViewFlowable.CompletionFailure> failuresCommandIds(Set<String> parties, Flowable<CompletionStreamResponse> completionStreamResponseFlowable) {
        return completionStreamResponseFlowable
                .concatMapIterable(CompletionStreamResponse::getCompletions)
                .filter(s -> s.getStatus().getCode() != Code.OK.getNumber())
                .map(c -> new LedgerViewFlowable.CompletionFailure(c.getCommandId(), c.getStatus()));
    }

    /**
     * Wires the Bot logic to an existing {@link LedgerClient} instance.
     *
     * @param applicationId The application identifier that will be sent to the Ledger
     * @param ledgerClient The {@link LedgerClient} instance which will be wired to the
     *                     bot.
     * @param transactionFilter A server-side filter of incoming transactions
     * @param bot The business logic of the bot.
     * @param transform A function from the arguments of a Contract on the Ledger to
     *                  a more refined type R. This can be used by the developer to, for
     *                  instance, discard the fields of a Contract that are not needed
     *                  and save space.
     * @param <R> The type of the result of transform.
     */

    public static <R> void wire(String applicationId,
                                LedgerClient ledgerClient,
                                TransactionFilter transactionFilter,
                                Function<LedgerViewFlowable.LedgerView<R>, Flowable<CommandsAndPendingSet>> bot,
                                Function<CreatedContract, R> transform) {
        wire(applicationId, ledgerClient, transactionFilter, bot, transform, Schedulers.io());
    }

    /**
     * Wires the Bot logic to an existing {@link LedgerClient} instance.
     *
     * @param applicationId The application identifier that will be sent to the Ledger
     * @param ledgerClient The {@link LedgerClient} instance which will be wired to the
     *                     bot.
     * @param transactionFilter A server-side filter of incoming transactions
     * @param bot The business logic of the bot.
     * @param transform A function from the arguments of a Contract on the Ledger to
     *                  a more refined type R. This can be used by the developer to, for
     *                  instance, discard the fields of a Contract that are not needed
     *                  and save space.
     * @param scheduler The scheduler used to run the flows
     * @param <R> The type of the result of transform.
     */
    public static <R> void wire(String applicationId,
                                LedgerClient ledgerClient,
                                TransactionFilter transactionFilter,
                                Function<LedgerViewFlowable.LedgerView<R>, Flowable<CommandsAndPendingSet>> bot,
                                Function<CreatedContract, R> transform,
                                Scheduler scheduler) {

        logger.info("Bot wiring started for parties {}", transactionFilter.getParties());

        TransactionsClient transactionsClient = ledgerClient.getTransactionsClient();
        // ACS is disabled until the Ledger supports verbose = true for it
//        Flowable<GetActiveContractsResponse> acs = FlowableLogger.log(ledgerClient.getActiveContractSetClient().getActiveContracts(transactionFilter, true), "acs");
        Flowable<GetActiveContractsResponse> acs = Flowable.empty();
        Single<Pair<LedgerViewFlowable.LedgerView<R>, LedgerOffset>> acsLedgerViewSingle = LedgerViewFlowable.ledgerViewAndOffsetFromACS(acs, transform).observeOn(scheduler);

        Single<Pair<LedgerViewFlowable.LedgerView<R>, LedgerOffset>> ledgerViewAndOffsetSingle = acsLedgerViewSingle.flatMap(
                acsLedgerViewAndOffset -> {
                    LedgerViewFlowable.LedgerView<R> acsLedgerView = acsLedgerViewAndOffset.getFirst();
                    LedgerOffset acsOffset = acsLedgerViewAndOffset.getSecond();
                    logger.debug("LedgerView accumulated from acs completed. Offset: {} LedgerView: {}", acsOffset, acsOffset);
                    return ledgerClient.getTransactionsClient().getLedgerEnd().flatMap(
                            ledgerEnd -> {
                                logger.debug("LedgerEnd: {}", ledgerEnd);
                                Flowable<Transaction> transactions = FlowableLogger.log(transactionsClient.getTransactions(acsOffset, ledgerEnd, transactionFilter, true), "initTransactions");
                                Single<LedgerViewFlowable.LedgerView<R>> ledgerViewSingle = LedgerViewFlowable.ledgerViewFromFlowable(acsLedgerView, transactions.map(t -> (WorkflowEvent) t), transform);
                                return ledgerViewSingle.map(ledgerView -> new Pair<>(ledgerView, ledgerEnd));
                            }
                    );
                }
        );

        Single<Pair<LedgerViewFlowable.LedgerView<R>, LedgerOffset>> mainFlow = ledgerViewAndOffsetSingle.doOnSuccess(ledgerViewAndOffset -> {
            LedgerViewFlowable.@NonNull LedgerView<R> initialLedgerView = ledgerViewAndOffset.getFirst();
            @NonNull LedgerOffset ledgerOffset = ledgerViewAndOffset.getSecond();
            logger.debug("LedgerView accumulated from acs and transactions completed. Offset: {} LedgerView: {}", ledgerOffset, initialLedgerView);
            Flowable<Transaction> transactions = FlowableLogger.log(transactionsClient.getTransactions(ledgerOffset, transactionFilter, true), "transactions").observeOn(scheduler);
            Flowable<LedgerViewFlowable.CompletionFailure> completionFailures = FlowableLogger.log(failuresCommandIds(transactionFilter.getParties(), ledgerClient.getCommandCompletionClient().completionStream(applicationId, LedgerOffset.LedgerEnd.getInstance(), transactionFilter.getParties())), "completionFailures")
                    .observeOn(scheduler);

            Subject<LedgerViewFlowable.SubmissionFailure> submissionFailuresSubject = ReplaySubject.create();
            Subject<CommandsAndPendingSet> commandsAndPendingSetSubject = ReplaySubject.create();

            Flowable<LedgerViewFlowable.SubmissionFailure> submissionFailures = FlowableLogger.log(submissionFailuresSubject.toFlowable(BackpressureStrategy.BUFFER), "submissionsFailures")
                    .observeOn(scheduler);
            Flowable<CommandsAndPendingSet> commandsAndPendingsSet = FlowableLogger.log(commandsAndPendingSetSubject.toFlowable(BackpressureStrategy.BUFFER), "commandsAndPendingSet")
                    .observeOn(scheduler);

            Flowable<LedgerViewFlowable.LedgerView<R>> ledgerViews = LedgerViewFlowable.<R>of(
                    initialLedgerView,
                    submissionFailures,
                    completionFailures,
                    transactions.map(t -> (WorkflowEvent) t),
                    commandsAndPendingsSet,
                    transform
            );
            Flowable<CommandsAndPendingSet> botResult = ledgerViews.concatMap(ledgerView -> {
                Flowable<CommandsAndPendingSet> result = null;
                try {
                    Flowable<CommandsAndPendingSet> commandsToSend = bot.apply(ledgerView);
                    result = Flowable.concat(commandsToSend, Flowable.just(CommandsAndPendingSet.empty));
                } catch (Throwable t) {
                    logger.error("Error during execution of bot.", t);
                    result = Flowable.error(t);
                }
                return FlowableLogger.log(result, "bot.execution");
            }).share();

            // to the ledger
            Flowable<SubmitCommandsRequest> commands = botResult
                    .filter(command -> !command.getSubmitCommandsRequest().getCommandId().isEmpty())
                    .map(CommandsAndPendingSet::getSubmitCommandsRequest);
            CommandSubmissionClient commandSubmissionClient = ledgerClient.getCommandSubmissionClient();
            commands.concatMapMaybe(commandsFailuresFromSubmissions(commandSubmissionClient)).toObservable().subscribe(submissionFailuresSubject);

            // to the ledger view flowable
            botResult.toObservable().subscribe(commandsAndPendingSetSubject);
            logger.info("Bot wiring complete for parties {}", transactionFilter.getParties());
        });
        // Since we have removed the blockingGet call, we now need to make sure that the flow is actually triggered
        mainFlow.toFlowable().observeOn(scheduler).publish().connect();
    }

    /**
     * Wires the Bot logic to an existing {@link LedgerClient} instance, storing {@link CreatedContract}
     * instances in the {@link com.daml.ledger.rxjava.components.LedgerViewFlowable.LedgerView}.
     *
     * @param appId The application identifier that will be sent to the Ledger
     * @param ledgerClient The {@link LedgerClient} instance which will be wired to the
     *                     bot.
     * @param transactionFilter A server-side filter of incoming transactions
     * @param bot The business logic of the bot.
     */
    public static void wireSimple(String appId,
                                  LedgerClient ledgerClient,
                                  TransactionFilter transactionFilter,
                                  Function<LedgerViewFlowable.LedgerView<CreatedContract>, Flowable<CommandsAndPendingSet>> bot) {
        Bot.<CreatedContract>wire(appId, ledgerClient, transactionFilter, bot, r -> r);
    }

    static Flowable<WorkflowEvent> activeContractSetAndNewTransactions(LedgerClient ledgerClient, TransactionFilter filter) {
        CompletableFuture<LedgerOffset> offsetFuture = new CompletableFuture<>();
        BiConsumer<LedgerOffset, String> setOffset = (ledgerOffset, name) -> {
            if (!offsetFuture.isDone()) {
                logger.debug(name + ".completeOffsetFuture: " + ledgerOffset);
                offsetFuture.complete(ledgerOffset);
            }
        };
        Flowable<GetActiveContractsResponse> activeContracts =
                FlowableLogger.log(ledgerClient.getActiveContractSetClient().getActiveContracts(filter, true), "acs")
                .doOnNext(r -> {
                    r.getOffset().ifPresent(off -> setOffset.accept(new LedgerOffset.Absolute(off), "acs.next"));
                }).doOnComplete(() -> setOffset.accept(LedgerOffset.LedgerBegin.getInstance(), "acs.complete"));
        Flowable<Transaction> transactions = Single.fromFuture(offsetFuture)
                .doOnSuccess(o -> logger.debug("offset.success: " + o))
                .doOnError(t -> logger.error("offset.error: " + t))
                .flatMapPublisher(offset ->
                        FlowableLogger.log(ledgerClient.getTransactionsClient().getTransactions(offset, filter, true), "transactions"));
        return Flowable.concat(activeContracts, transactions);
    }

    private static io.reactivex.functions.Function<@NonNull SubmitCommandsRequest, MaybeSource<? extends LedgerViewFlowable.SubmissionFailure>> commandsFailuresFromSubmissions(CommandSubmissionClient commandSubmissionClient) {
        return cs -> {
            logger.debug("Submitting: {}", cs);
            return FlowableLogger.log(commandSubmissionClient.submit(cs.getWorkflowId(), cs.getApplicationId(),
                    cs.getCommandId(), cs.getParty(), cs.getMinLedgerTimeAbsolute(), cs.getMinLedgerTimeRelative(),
                    cs.getDeduplicationTime(), cs.getCommands())
                    .flatMapMaybe(s -> Maybe.<LedgerViewFlowable.SubmissionFailure> empty())
                    .doOnError(t -> logger.error("Error submitting commands {} for party {}: {}", cs.getCommandId(), cs.getParty(), t.getMessage()))
                    .onErrorReturn(t -> new LedgerViewFlowable.SubmissionFailure(cs.getCommandId(), t))
                    , "commandSubmissions");
        };
    }
}
