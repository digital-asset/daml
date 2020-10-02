// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components;

import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.components.helpers.*;
import com.google.rpc.Status;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.pcollections.HashTreePMap;
import org.pcollections.HashTreePSet;
import org.pcollections.PMap;
import org.pcollections.PSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class LedgerViewFlowable {

    private final static Logger logger = LoggerFactory.getLogger(LedgerViewFlowable.class);

    static private class StateWithShouldEmit<R> {
        final LedgerView<R> ledgerView;
        final int commandsCounter;
        final boolean shouldEmit;

        public StateWithShouldEmit(LedgerView<R> ledgerView, int commandsCounter, boolean shouldEmit) {
            this.ledgerView = ledgerView;
            this.commandsCounter = commandsCounter;
            this.shouldEmit = shouldEmit;
        }

        // create a new ledgerView which should not be emitted
        public static <R> StateWithShouldEmit<R> create() {
            return new StateWithShouldEmit<>(LedgerView.create(), 0, false);
        }

        public static <R> StateWithShouldEmit<R> of(LedgerView<R> initialLedgerView) {
            return new StateWithShouldEmit<>(initialLedgerView, 0, false);
        }

        public StateWithShouldEmit<R> emit(LedgerView<R> newLedgerView) {
            return new StateWithShouldEmit<>(newLedgerView, this.commandsCounter, true);
        }

        @Override
        public String toString() {
            return "StateWithShouldEmit{" +
                    "ledgerView=" + ledgerView +
                    ", commandsCounter=" + commandsCounter +
                    ", shouldEmit=" + shouldEmit +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StateWithShouldEmit<?> that = (StateWithShouldEmit<?>) o;
            return commandsCounter == that.commandsCounter &&
                    shouldEmit == that.shouldEmit &&
                    Objects.equals(ledgerView, that.ledgerView);
        }

        @Override
        public int hashCode() {

            return Objects.hash(ledgerView, commandsCounter, shouldEmit);
        }
    }

    static abstract class LedgerViewFlowableInput { }
    static class SubmissionFailure extends LedgerViewFlowableInput {
        final String commandId;
        final Throwable throwable;

        SubmissionFailure(String commandId, Throwable throwable) {
            this.commandId = commandId;
            this.throwable = throwable;
        }

        @Override
        public String toString() {
            return "SubmissionFailure{" +
                    "commandId='" + commandId + '\'' +
                    ", throwable=" + throwable +
                    '}';
        }


    }
    static class CompletionFailure extends LedgerViewFlowableInput {
        final String commandId;
        final Status status;

        CompletionFailure(String commandId, Status status) {
            this.commandId = commandId;
            this.status = status;
        }

        @Override
        public String toString() {
            return "CompletionFailure{" +
                    "commandId='" + commandId + '\'' +
                    ", status=" + status +
                    '}';
        }

    }
    static class WorkflowEventWrapper extends LedgerViewFlowableInput {
        final WorkflowEvent workflowEvent;

        public WorkflowEventWrapper(WorkflowEvent workflowEvent) {
            this.workflowEvent = workflowEvent;
        }

        @Override
        public String toString() {
            return "WorkflowEventWrapper{" +
                    "workflowEvent=" + workflowEvent +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WorkflowEventWrapper that = (WorkflowEventWrapper) o;
            return Objects.equals(workflowEvent, that.workflowEvent);
        }

        @Override
        public int hashCode() {

            return Objects.hash(workflowEvent);
        }
    }
    static private class CommandIdAndPendingSet extends LedgerViewFlowableInput {
        final String commandId;

        @Override
        public String toString() {
            return "CommandIdAndPendingSet{" +
                    "commandId='" + commandId + '\'' +
                    ", templateToContracts=" + templateToContracts +
                    '}';
        }

        public CommandIdAndPendingSet(String commandId, PMap<Identifier, PSet<String>> templateToContracts) {
            this.commandId = commandId;
            this.templateToContracts = templateToContracts;
        }

        final PMap<Identifier, PSet<String>> templateToContracts;

        public static CommandIdAndPendingSet from(CommandsAndPendingSet commandsAndPendingSet) {
            return new CommandIdAndPendingSet(commandsAndPendingSet.getSubmitCommandsRequest().getCommandId(), commandsAndPendingSet.getContractIdsPendingIfSucceed());
        }
    }

    public static <R> Flowable<LedgerView<R>> of(LedgerView<R> initialLedgerView,
                                                 Flowable<SubmissionFailure> submissionFailures,
                                                 Flowable<CompletionFailure> completionFailures,
                                                 Flowable<WorkflowEvent> workflowEvents,
                                                 Flowable<CommandsAndPendingSet> commandsAndPendingsSet,
                                                 Function<CreatedContract, R> transform) {

        Flowable<LedgerViewFlowableInput> workflowEventsWrapper = workflowEvents.map(WorkflowEventWrapper::new);
        Flowable<LedgerViewFlowableInput> commandIdAndPendingsSet = commandsAndPendingsSet.map(CommandIdAndPendingSet::from);
        Flowable<LedgerViewFlowableInput> all = Flowable.merge(submissionFailures, completionFailures, workflowEventsWrapper, commandIdAndPendingsSet);

        boolean shouldEmitInitial = !initialLedgerView.activeContractSet.isEmpty();
        StateWithShouldEmit<R> initialState = new StateWithShouldEmit<>(initialLedgerView, 0, shouldEmitInitial);

        return all.scan(initialState, (input, state) -> LedgerViewFlowable.accumulate(input, state, transform))
                .filter(s -> s.shouldEmit)
                .map(s -> s.ledgerView);
    }

    public static <R> Flowable<LedgerView<R>> of(Flowable<SubmissionFailure> submissionFailuresCommandIds,
                                                 Flowable<CompletionFailure> completionFailuresCommandIds,
                                                 Flowable<WorkflowEvent> workflowEvents,
                                                 Flowable<CommandsAndPendingSet> commandsAndPendingsSet,
                                                 Function<CreatedContract, R> transform) {
        return of(LedgerView.<R>create(),
                  submissionFailuresCommandIds,
                  completionFailuresCommandIds,
                  workflowEvents,
                  commandsAndPendingsSet,
                  transform);
    }

    static <R> StateWithShouldEmit<R> accumulate(StateWithShouldEmit<R> stateWithShouldEmit, LedgerViewFlowableInput ledgerViewFlowableInput, Function<CreatedContract, R> transform) {
        logger.debug("LedgerView.accumulate({}, {})", stateWithShouldEmit, ledgerViewFlowableInput);
        if (ledgerViewFlowableInput instanceof SubmissionFailure) {
            SubmissionFailure submissionFailure = (SubmissionFailure) ledgerViewFlowableInput;
            LedgerView<R> newLedgerView = stateWithShouldEmit.ledgerView.unsetPendingForContractsOfCommandId(submissionFailure.commandId);
            return stateWithShouldEmit.emit(newLedgerView);
        } else if (ledgerViewFlowableInput instanceof CompletionFailure) {
            CompletionFailure completionFailure = (CompletionFailure) ledgerViewFlowableInput;
            LedgerView<R> newLedgerView = stateWithShouldEmit.ledgerView.unsetPendingForContractsOfCommandId(completionFailure.commandId);
            if (newLedgerView.equals(stateWithShouldEmit.ledgerView)) {
                return stateWithShouldEmit;
            }
            else {
                logger.info("Command {} failed: {}", completionFailure.commandId, completionFailure.status);
                return stateWithShouldEmit.emit(newLedgerView);
            }
        } else if (ledgerViewFlowableInput instanceof WorkflowEventWrapper) {
            WorkflowEvent workflowEvent = ((WorkflowEventWrapper) ledgerViewFlowableInput).workflowEvent;
            if (workflowEvent instanceof Transaction) {
                Transaction transaction = (Transaction) workflowEvent;
                LedgerView<R> newLedgerView = stateWithShouldEmit.ledgerView;
                TransactionContext transactionContext = TransactionContext.forTransaction(transaction);
                for (Event event : transaction.getEvents()) {
                    if (event instanceof CreatedEvent) {
                        CreatedEvent createdEvent = (CreatedEvent) event;
                        R r = transform.apply(new CreatedContract(createdEvent.getTemplateId(), createdEvent.getArguments(), transactionContext));
                        newLedgerView = newLedgerView.addActiveContract(createdEvent.getTemplateId(), createdEvent.getContractId(), r);
                    } else if (event instanceof ArchivedEvent) {
                        ArchivedEvent archivedEvent = (ArchivedEvent) event;
                        newLedgerView = newLedgerView.archiveContract(archivedEvent.getTemplateId(), archivedEvent.getContractId());
                    }
                }
                // the remaining contracts that were considered pending when the command was submitted
                // but are not archived by this transaction are set as active again
                String commandId = transaction.getCommandId();
                newLedgerView = newLedgerView.unsetPendingForContractsOfCommandId(commandId);
                return stateWithShouldEmit.emit(newLedgerView);
            } else if (workflowEvent instanceof GetActiveContractsResponse) {
                GetActiveContractsResponse activeContracts = (GetActiveContractsResponse) workflowEvent;
                LedgerView<R> newLedgerView = stateWithShouldEmit.ledgerView;
                GetActiveContractsResponseContext context = new GetActiveContractsResponseContext(activeContracts.getWorkflowId());
                for (CreatedEvent createdEvent : activeContracts.getCreatedEvents()) {
                    R r = transform.apply(new CreatedContract(createdEvent.getTemplateId(), createdEvent.getArguments(), context));
                    newLedgerView = newLedgerView.addActiveContract(createdEvent.getTemplateId(), createdEvent.getContractId(), r);
                }
                return stateWithShouldEmit.emit(newLedgerView);
            } else {
                throw new IllegalStateException(workflowEvent.toString());
            }
        } else if (ledgerViewFlowableInput instanceof CommandIdAndPendingSet) {
            CommandIdAndPendingSet commandIdAndPendingSet = (CommandIdAndPendingSet) ledgerViewFlowableInput;
            LedgerView<R> newLedgerView = stateWithShouldEmit.ledgerView;
            for (Identifier templateId : commandIdAndPendingSet.templateToContracts.keySet()) {
                PSet<String> contractIds = commandIdAndPendingSet.templateToContracts.get(templateId);
                for (String contractId : contractIds) {
                    newLedgerView = newLedgerView.setContractPending(commandIdAndPendingSet.commandId, templateId, contractId);
                }
                logger.debug("newLedgerView: {}", newLedgerView.getContracts(templateId));
            }
            // commands with commandId empty is the signal that the bot is done with the commands
            boolean isBoundary = commandIdAndPendingSet.commandId.isEmpty();
            if (isBoundary) {
                // if we found a boundary, we emit a value only in case there have been commands in between this and the
                // previous boundary:
                // previous_boundary ~> any non commands ~> this_boundary =====> don't emit anything
                // previous_boundary ~> one or more commands ~> this_boundary =====> emit the new ledgerView
                return new StateWithShouldEmit<>(newLedgerView, 0, stateWithShouldEmit.commandsCounter > 0);
            } else {
                return new StateWithShouldEmit<>(newLedgerView, stateWithShouldEmit.commandsCounter + 1, false);
            }
        }
//        return stateWithShouldEmit;
        throw new IllegalStateException(stateWithShouldEmit.toString());
    }

    static <R> Single<Pair<LedgerView<R>, LedgerOffset>> ledgerViewAndOffsetFromACS(Flowable<GetActiveContractsResponse> responses, Function<CreatedContract, R> transform) {
        Single<Pair<StateWithShouldEmit<R>, Optional<LedgerOffset>>> lastViewAndOffset = responses.reduce(
                new Pair<>(StateWithShouldEmit.create(), Optional.empty()),
                (viewAndOffset, response) -> {
                    @NonNull StateWithShouldEmit<R> ledgerView = viewAndOffset.getFirst();
                    StateWithShouldEmit<R> newLedgerView = accumulate(ledgerView, new WorkflowEventWrapper(response), transform);
                    @NonNull Optional<LedgerOffset> offset = response.getOffset().map(off -> new LedgerOffset.Absolute(off));
                    return new Pair<>(newLedgerView, offset);
                }
        );
        return lastViewAndOffset.map(m -> new Pair<>(m.getFirst().ledgerView, m.getSecond().orElse(LedgerOffset.LedgerBegin.getInstance())));
    }

    static <R> Single<LedgerView<R>> ledgerViewFromFlowable(LedgerView<R> initial, Flowable<WorkflowEvent> events, Function<CreatedContract, R> transform) {
        return events.reduce(
                new StateWithShouldEmit<>(initial, 0, false),
                (ledgerView, event) -> accumulate(ledgerView, new WorkflowEventWrapper(event), transform)
        ).map(m -> m.ledgerView);
    }

    /**
     * The local view of the Ledger. This view is created and updated every time a new event is received
     * by the client.
     *
     * @param <R> The type of the contracts in this application.
     */
    public static class LedgerView<R> {

        protected final PMap<String, PMap<Identifier, PSet<String>>> commandIdToContractIds;
        protected final PMap<Identifier, PMap<String, PSet<String>>> contractIdsToCommandIds;
        protected final PMap<Identifier, PMap<String, R>> pendingContractSet;
        protected final PMap<Identifier, PMap<String, R>> activeContractSet;

        LedgerView(PMap<String, PMap<Identifier, PSet<String>>> commandIdToContractIds,
                           PMap<Identifier, PMap<String, PSet<String>>> contractIdsToCommandIds,
                           PMap<Identifier, PMap<String, R>> pendingContractSet,
                           PMap<Identifier, PMap<String, R>> activeContractSet) {
            this.commandIdToContractIds = commandIdToContractIds;
            this.contractIdsToCommandIds = contractIdsToCommandIds;
            this.pendingContractSet = pendingContractSet;
            this.activeContractSet = activeContractSet;
        }

        /**
         * Creates a new empty view of the Ledger.
         */
        public static <R> LedgerView<R> create() {
            return new LedgerView<R>(HashTreePMap.empty(), HashTreePMap.empty(), HashTreePMap.empty(), HashTreePMap.empty());
        }

        // transaction with created
        LedgerView<R> addActiveContract(Identifier templateId, String contractId, R r) {
            logger.debug("ledgerView.addActiveContract({}, {}, {})", templateId, contractId, r);
            PMap<String, R> oldContractIdsToR = this.activeContractSet.getOrDefault(templateId, HashTreePMap.empty());
            PMap<String, R> newContractIdsToR = oldContractIdsToR.plus(contractId, r);
            PMap<Identifier, PMap<String, R>> newActiveContractSet = this.activeContractSet.plus(templateId, newContractIdsToR);
            return new LedgerView<R>(this.commandIdToContractIds, this.contractIdsToCommandIds, this.pendingContractSet, newActiveContractSet);
        }

        // set pending is received
        LedgerView<R> setContractPending(String commandId, Identifier templateId, String contractId) {
            logger.debug("ledgerView.setContractPending({}, {}, {})", commandId, templateId, contractId);
            // remove from the active contract set
            PMap<String, R> oldActiveContracts = this.activeContractSet.getOrDefault(templateId, HashTreePMap.empty());
            R r = oldActiveContracts.get(contractId);
            PMap<String, R> newActiveContracts = oldActiveContracts.minus(contractId);
            PMap<Identifier, PMap<String, R>> newActiveContractSet =
                    newActiveContracts.isEmpty() ?
                            this.activeContractSet.minus(templateId) :
                            this.activeContractSet.plus(templateId, newActiveContracts);

            // add to the pending contract set
            PMap<String, R> oldPendingContracts = this.pendingContractSet.getOrDefault(templateId, HashTreePMap.empty());
            PMap<String, R> newPendingContracts = oldPendingContracts.plus(contractId, r);
            PMap<Identifier, PMap<String, R>> newPendingContractSet = this.pendingContractSet.plus(templateId, newPendingContracts);

            // add to commandId -> contractIds
            PMap<Identifier, PSet<String>> oldTemplateIdToContracts = this.commandIdToContractIds.getOrDefault(commandId, HashTreePMap.empty());
            PSet<String> oldContracts = oldTemplateIdToContracts.getOrDefault(templateId, HashTreePSet.empty());
            PSet<String> newContracts = oldContracts.plus(contractId);
            PMap<Identifier, PSet<String>> newTemplateIdToContracts = oldTemplateIdToContracts.plus(templateId, newContracts);
            PMap<String, PMap<Identifier, PSet<String>>> newCommandIdToContracts =
                    this.commandIdToContractIds.plus(commandId, newTemplateIdToContracts);

            // add to contractId -> commandIds
            PMap<String, PSet<String>> oldContractToCommandIds = this.contractIdsToCommandIds.getOrDefault(templateId, HashTreePMap.empty());
            PSet<String> oldCommandIds = oldContractToCommandIds.getOrDefault(contractId, HashTreePSet.empty());
            PSet<String> newCommandIds = oldCommandIds.plus(commandId);
            PMap<String, PSet<String>> newContractToCommandIds = oldContractToCommandIds.plus(contractId, newCommandIds);
            PMap<Identifier, PMap<String, PSet<String>>> newContractIdsToCommandIds =
                    this.contractIdsToCommandIds.plus(templateId, newContractToCommandIds);

            LedgerView<R> result = new LedgerView<R>(newCommandIdToContracts, newContractIdsToCommandIds, newPendingContractSet, newActiveContractSet);
            logger.debug("ledgerView.setContractPending({}, {}, {})).result: ", commandId, templateId, contractId, result);
            return result;
        }

        // completion failure
        LedgerView<R> unsetPendingForContractsOfCommandId(String commandId) {
            logger.debug("ledgerView.unsetPendingForContractsOfCommandId({})", commandId);
            final PMap<String, PMap<Identifier, PSet<String>>> newCommandIdToContractIds = this.commandIdToContractIds.minus(commandId);

            // we go over each contract, see if this is the only command locking it and if the answer is yes we move
            // the contract from pending to active
            final PMap<Identifier, PSet<String>> contractsToUnset = this.commandIdToContractIds.getOrDefault(commandId, HashTreePMap.empty());

            // these variables can potentially mutate for each contract
            PMap<Identifier, PMap<String, PSet<String>>> newContractIdsToCommandIds = this.contractIdsToCommandIds;
            PMap<Identifier, PMap<String, R>> newPendingContractSet = this.pendingContractSet;
            PMap<Identifier, PMap<String, R>> newActiveContractSet = this.activeContractSet;

            for (Identifier templateId : contractsToUnset.keySet()) {
                final PSet<String> oldContractIds = contractsToUnset.get(templateId);

                // there variables can potentially mutate
                PMap<String, PSet<String>> contractIdToCommandIds = newContractIdsToCommandIds.getOrDefault(templateId, HashTreePMap.empty());
                PMap<String, R> pendingContractSet = newPendingContractSet.getOrDefault(templateId, HashTreePMap.empty());
                PMap<String, R> activeContractSet = newActiveContractSet.getOrDefault(templateId, HashTreePMap.empty());

                for (String contractId : oldContractIds) {
                    final PSet<String> oldCommandIds = contractIdToCommandIds.getOrDefault(contractId, HashTreePSet.empty());
                    final PSet<String> newCommandIds = oldCommandIds.minus(commandId);
                    if (newCommandIds.isEmpty()) {
                        // unset pending for the contract
                        contractIdToCommandIds = contractIdToCommandIds.minus(contractId);
                        R r = pendingContractSet.get(contractId);
                        pendingContractSet = pendingContractSet.minus(contractId);
                        activeContractSet = activeContractSet.plus(contractId, r);
                    } else {
                        contractIdToCommandIds = contractIdToCommandIds.plus(contractId, newCommandIds);
                        // do nothing because other commands are still locking the contract
                    }
                }

                newContractIdsToCommandIds = contractIdToCommandIds.isEmpty() ?
                        newContractIdsToCommandIds.minus(templateId) :
                        newContractIdsToCommandIds.plus(templateId, contractIdToCommandIds);
                newPendingContractSet = pendingContractSet.isEmpty() ?
                        newPendingContractSet.minus(templateId) :
                        newPendingContractSet.plus(templateId, pendingContractSet);
                newActiveContractSet = activeContractSet.isEmpty() ?
                        newActiveContractSet.minus(templateId) :
                        newActiveContractSet.plus(templateId, activeContractSet);
            }


            return new LedgerView<R>(newCommandIdToContractIds, newContractIdsToCommandIds, newPendingContractSet, newActiveContractSet);
        }

        private static <R> PMap<Identifier, PMap<String, R>> removeContract(Identifier templateId, String contractId, PMap<Identifier, PMap<String, R>> from) {
            if (from.containsKey(templateId)) {
                PMap<String, R> oldActiveContracts = from.get(templateId);
                if (oldActiveContracts.containsKey(contractId)) {
                    PMap<String, R> newActiveContracts = oldActiveContracts.minus(contractId);
                    if (newActiveContracts.isEmpty()) {
                        return from.minus(templateId);
                    } else {
                        return from.plus(templateId, newActiveContracts);
                    }
                }
            }
            return from;
        }

        // transaction with archived
        LedgerView<R> archiveContract(Identifier templateId, String contractId) {
            logger.debug("ledgerView.archiveContract({}, {})", templateId, contractId);
            PMap<Identifier, PMap<String, R>> newPendingContractSet = removeContract(templateId, contractId, this.pendingContractSet);
            PMap<Identifier, PMap<String, R>> newActiveContractSet = removeContract(templateId, contractId, this.activeContractSet);

            PMap<String, PMap<Identifier, PSet<String>>> newCommandIdToContractIds = this.commandIdToContractIds;
            if (this.contractIdsToCommandIds.containsKey(templateId)) {
                PMap<String, PSet<String>> contractIdToCommandIds = this.contractIdsToCommandIds.get(templateId);
                if (contractIdToCommandIds.containsKey(contractId)) {
                    PSet<String> commandIds = contractIdToCommandIds.get(contractId);
                    for (String commandId : commandIds) {
                        PMap<Identifier, PSet<String>> oldContracts = newCommandIdToContractIds.getOrDefault(commandId, HashTreePMap.empty());
                        PSet<String> oldContractIds = oldContracts.getOrDefault(templateId, HashTreePSet.empty());
                        PSet<String> newContractIds = oldContractIds.minus(contractId);
                        PMap<Identifier, PSet<String>> newContracts =
                                newContractIds.isEmpty() ?
                                        oldContracts.minus(templateId) :
                                        oldContracts.plus(templateId, newContractIds);
                        newCommandIdToContractIds =
                                newContracts.isEmpty() ?
                                        newCommandIdToContractIds.minus(commandId) :
                                        newCommandIdToContractIds.plus(commandId, newContracts);
                    }
                }
            }

            PMap<Identifier, PMap<String, PSet<String>>> newContractIdsToCommandIds = this.contractIdsToCommandIds;
            if (newContractIdsToCommandIds.containsKey(templateId)) {
                PMap<String, PSet<String>> oldContracts = newContractIdsToCommandIds.get(templateId);
                PMap<String, PSet<String>> newContracts = oldContracts.minus(contractId);
                newContractIdsToCommandIds = newContracts.isEmpty() ?
                                             newContractIdsToCommandIds.minus(templateId) :
                                             newContractIdsToCommandIds.plus(templateId, newContracts);
            }

            return new LedgerView<R>(newCommandIdToContractIds, newContractIdsToCommandIds, newPendingContractSet, newActiveContractSet);
        }

        /**
         * @param templateId The template of the contracts in which the client is interested
         * @return A map contractId to contract containing all the contracts with template id equal
         *         to the one passed as argument
         */
        public PMap<String, R> getContracts(Identifier templateId) {
            return this.activeContractSet.getOrDefault(templateId, HashTreePMap.empty());
        }

        @Override
        public String toString() {
            return "LedgerView{" +
                    "activeContractSet=" + activeContractSet +
                    ", pendingContractSet=" + pendingContractSet +
                    ", contractIdsToCommandIds=" + contractIdsToCommandIds +
                    ", commandIdToContractIds=" + commandIdToContractIds +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LedgerView<?> ledgerView = (LedgerView<?>) o;
            return Objects.equals(commandIdToContractIds, ledgerView.commandIdToContractIds) &&
                    Objects.equals(contractIdsToCommandIds, ledgerView.contractIdsToCommandIds) &&
                    Objects.equals(pendingContractSet, ledgerView.pendingContractSet) &&
                    Objects.equals(activeContractSet, ledgerView.activeContractSet);
        }

        @Override
        public int hashCode() {

            return Objects.hash(commandIdToContractIds, contractIdsToCommandIds, pendingContractSet, activeContractSet);
        }
    }

    /**
     * A ledger view for unit testing of bots.
     *
     * @param <R> The type of the contracts in this application.
     */
    public static class LedgerTestView<R> extends LedgerView<R> {
        public LedgerTestView(PMap<String, PMap<Identifier, PSet<String>>> commandIdToContractIds,
                              PMap<Identifier, PMap<String, PSet<String>>> contractIdsToCommandIds,
                              PMap<Identifier, PMap<String, R>> pendingContractSet,
                              PMap<Identifier, PMap<String, R>> activeContractSet) {
            super(commandIdToContractIds, contractIdsToCommandIds, pendingContractSet, activeContractSet);
        }

        @Override
        public LedgerTestView<R> addActiveContract(Identifier templateId, String contractId, R r) {
            LedgerView lv = super.addActiveContract(templateId, contractId, r);
            return new LedgerTestView<R>(
                    lv.commandIdToContractIds,
                    lv.contractIdsToCommandIds,
                    lv.pendingContractSet,
                    lv.activeContractSet);
        }
    }
}
