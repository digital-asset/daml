// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client.rs;

import io.grpc.stub.ClientCallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This class is not thread safe, synchronization must be handled outside of it.
 *
 * Buffers events that happen downstream of a ClientPublisher component, including:
 * - demand signals
 * - cancellation
 * These events can then be flushed into a ClientCallStreamObserver.
 */
class DownstreamEventBuffer {
    private static Logger logger = LoggerFactory.getLogger(DownstreamEventBuffer.class);
    private static final BigInteger LONG_MAX_AS_BIGINT = BigInteger.valueOf(Long.MAX_VALUE);
    private static final long INT_MAX_AS_LONG = ((long) Integer.MAX_VALUE);

    @Nonnull
    private final String logPrefix;
    private long demand = 0L;
    private DownstreamState downstreamState = DownstreamState.FLOW_CONTROLLED;

    enum DownstreamState {
        FLOW_CONTROLLED, UNBOUNDED, CANCELLATION_BUFFERED, CANCELLATION_FLUSHED
    }

    private final Consumer<ClientCallStreamObserver> propagateCancellation;
    private final BiConsumer<ClientCallStreamObserver, Integer> propagateDemand;

    DownstreamEventBuffer(@Nonnull Consumer<ClientCallStreamObserver> propagateCancellation,
                          @Nonnull BiConsumer<ClientCallStreamObserver, Integer> propagateDemand,
                          @Nonnull String logPrefix) {
        this.propagateCancellation = propagateCancellation;
        this.propagateDemand = propagateDemand;
        this.logPrefix = logPrefix;
    }

    /**
     * Takes note of the outstanding demand from the downstream.
     * If the amount of demand that was not yet signaled to upstream reaches Long.MAX_VALUE the DownstreamEventBuffer
     * changes its flow control mode to UNBOUNDED.
     */
    void bufferDemand(@Nonnegative long newDemand) {
        switch (downstreamState) {
            case FLOW_CONTROLLED:
                boolean reachedLongMaxValue = demand  >= Long.MAX_VALUE - newDemand;
                if (reachedLongMaxValue) {
                    logger.trace("{}Switched to unbounded downstreamState as new demand of {} total buffered demand reached Long.MAX_VALUE.", logPrefix, newDemand);
                    if (downstreamState.equals(DownstreamState.FLOW_CONTROLLED)) {
                        downstreamState = DownstreamState.UNBOUNDED;
                    }
                    demand = 0; // Don't care anymore
                } else {
                    demand = demand + newDemand;
                    logger.trace("{}Demand of {} buffered. Total buffered is {}.", logPrefix, newDemand, demand);
                }
                break;
            case UNBOUNDED:
                break;
            case CANCELLATION_BUFFERED:
                break;
            case CANCELLATION_FLUSHED:
                break;
        }


    }

    /**
     * Chops off part of the demand, capped at Integer.MAX_VALUE.
     * Returns this value to the caller, and stores the remaining amount in the demand variable.
     */
    private int getIntegerChunk() {
        long chunk = this.demand < INT_MAX_AS_LONG ? this.demand : INT_MAX_AS_LONG;
        this.demand -= chunk;
        return (int) chunk;
    }

    /**
     * Takes note of the fact that the downstream cancelled the stream.
     * This will be communicated to gRPC on the next call to propagateCancellationOrDemand.
     */
    void bufferCancellation() {
        if (!downstreamState.equals(DownstreamState.CANCELLATION_FLUSHED))
            downstreamState = DownstreamState.CANCELLATION_BUFFERED;
        logger.trace("{}Cancellation buffered.", logPrefix);
    }

    /**
     * Propagates demand or cancellation according to buffered state.
     *
     * @return The amount of demand propagated, if any.
     */
    int propagateCancellationOrDemand(@Nonnull ClientCallStreamObserver requestObserver) {
        switch (downstreamState) {
            case FLOW_CONTROLLED:
                int demandToPropagate = getIntegerChunk();
                if (demandToPropagate != 0) {
                    logger.trace("{}Flushing demand for {} elements. Remaining demand in buffer: {}", logPrefix,
                            demandToPropagate,
                            this.demand);
                    propagateDemand.accept(requestObserver, demandToPropagate);
                }
                return demandToPropagate;
            case UNBOUNDED:
                logger.trace("{}Flushing demand for Integer.MAX_VALUE elements in unbounded mode.", logPrefix);
                propagateDemand.accept(requestObserver, Integer.MAX_VALUE);
                return Integer.MAX_VALUE;
            case CANCELLATION_BUFFERED:
                logger.trace("{}Flushing buffered cancellation.", logPrefix);
                propagateCancellation.accept(requestObserver);
                downstreamState = DownstreamState.CANCELLATION_FLUSHED;
                return 0;
            default:
                return 0;
        }
    }
}
