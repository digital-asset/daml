// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.SubmitAndWaitRequest;
import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.daml.ledger.rxjava.CommandClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.digitalasset.ledger.api.v1.CommandServiceGrpc;
import com.digitalasset.ledger.api.v1.CommandServiceOuterClass;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class CommandClientImpl implements CommandClient {

    private final String ledgerId;
    private final CommandServiceGrpc.CommandServiceFutureStub serviceStub;

    public CommandClientImpl(@NonNull String ledgerId, @NonNull Channel channel) {
        this.ledgerId = ledgerId;
        this.serviceStub = CommandServiceGrpc.newFutureStub(channel);
    }

    private Single<Empty> submitAndWait(@NonNull String workflowId, @NonNull String applicationId,
                                       @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                       @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull Optional<String> accessToken) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).submitAndWait(request));
    }

    @Override
    public Single<Empty> submitAndWait(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        return submitAndWait(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.empty());
    }

    @Override
    public Single<Empty> submitAndWait(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull String accessToken) {
        return submitAndWait(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.of(accessToken));
    }

    private Single<String> submitAndWaitForTransactionId(@NonNull String workflowId, @NonNull String applicationId,
                                                         @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                                         @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull Optional<String> accessToken) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).submitAndWaitForTransactionId(request))
                .map(CommandServiceOuterClass.SubmitAndWaitForTransactionIdResponse::getTransactionId);
    }

    @Override
    public Single<String> submitAndWaitForTransactionId(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        return submitAndWaitForTransactionId(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.empty());
    }

    @Override
    public Single<String> submitAndWaitForTransactionId(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull String accessToken) {
        return submitAndWaitForTransactionId(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.of(accessToken));
    }

    private Single<Transaction> submitAndWaitForTransaction(@NonNull String workflowId, @NonNull String applicationId,
                                                           @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                                           @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull Optional<String> accessToken) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).submitAndWaitForTransaction(request))
                .map(CommandServiceOuterClass.SubmitAndWaitForTransactionResponse::getTransaction)
                .map(Transaction::fromProto);
    }

    @Override
    public Single<Transaction> submitAndWaitForTransaction(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        return submitAndWaitForTransaction(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.empty());
    }

    @Override
    public Single<Transaction> submitAndWaitForTransaction(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull String accessToken) {
        return submitAndWaitForTransaction(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.of(accessToken));
    }

    private Single<TransactionTree> submitAndWaitForTransactionTree(@NonNull String workflowId, @NonNull String applicationId,
                                                                   @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime,
                                                                   @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull Optional<String> accessToken) {
        CommandServiceOuterClass.SubmitAndWaitRequest request = SubmitAndWaitRequest.toProto(this.ledgerId,
                workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands);
        return Single.fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).submitAndWaitForTransactionTree(request))
                .map(CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse::getTransaction)
                .map(TransactionTree::fromProto);
    }

    @Override
    public Single<TransactionTree> submitAndWaitForTransactionTree(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands) {
        return submitAndWaitForTransactionTree(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.empty());
    }

    @Override
    public Single<TransactionTree> submitAndWaitForTransactionTree(@NonNull String workflowId, @NonNull String applicationId, @NonNull String commandId, @NonNull String party, @NonNull Instant ledgerEffectiveTime, @NonNull Instant maximumRecordTime, @NonNull List<@NonNull Command> commands, @NonNull String accessToken) {
        return submitAndWaitForTransactionTree(workflowId, applicationId, commandId, party, ledgerEffectiveTime, maximumRecordTime, commands, Optional.of(accessToken));
    }

}
