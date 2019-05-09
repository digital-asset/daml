-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger( -- WIP: High level interface to the Ledger API services
    module DA.Ledger.Types,
    Port(..),
    LedgerHandle,
    Party(..),
    LL_Transaction, -- TODO: remove
    ResponseStream,
    connect,
    identity,
    getTransactionStream,
    nextResponse,
    submitCommands,
    getCompletionStream,
    ) where

import Control.Concurrent
import Control.Monad.Fix (fix)
import qualified Data.Map as Map
import qualified Data.Text.Lazy as Text
import Data.Vector as Vector (fromList, toList)
import Prelude hiding (log)

import DA.Ledger.Types
import DA.Ledger.Convert(lowerCommands)

import qualified DA.Ledger.LowLevel as LL

import DA.Ledger.LowLevel(

    ClientRequest(
            ClientReaderRequest,
            ClientNormalRequest
            ),
    ClientResult(
            ClientErrorResponse,
            ClientReaderResponse,
            ClientNormalResponse
            ),
    MetadataMap(..),
    GRPCMethodType(Normal,ServerStreaming),
    ClientConfig(..),
    Port(..),
    Host(..),

    LedgerIdentityService(..),
    GetLedgerIdentityRequest(..),
    GetLedgerIdentityResponse(..),

    TransactionService(..),
    GetTransactionsRequest(..),
    GetTransactionsResponse(..),

    CommandSubmissionService(..),
    SubmitRequest(..),
    Empty(..),

    CommandCompletionService(..),
    CompletionStreamRequest(..),
    CompletionStreamResponse(completionStreamResponseCompletions),

    TransactionFilter(..),
    Filters(..),
    LedgerOffset(..),
    LedgerOffsetValue(..),
    LedgerOffset_LedgerBoundary(..),
    TraceContext,
    )

data LedgerHandle = LedgerHandle { port :: Port, lid :: LedgerId }

identity :: LedgerHandle -> LedgerId
identity LedgerHandle{lid} = lid


newtype ResponseStream a = ResponseStream { chan :: Chan a }

nextResponse :: ResponseStream a -> IO a
nextResponse ResponseStream{chan} = readChan chan

connect :: Port -> IO LedgerHandle
connect port = do
    lid <- getLedgerIdentity port
    return $ LedgerHandle {port, lid}


getLedgerIdentity :: Port -> IO LedgerId
getLedgerIdentity port = do
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.ledgerIdentityServiceClient client
        let LedgerIdentityService rpc = service
        response <- rpc (wrap (GetLedgerIdentityRequest noTrace))
        GetLedgerIdentityResponse text <- unwrap response
        return $ LedgerId text


submitCommands :: LedgerHandle -> Commands -> IO ()
submitCommands h commands = do
    let request = wrap (SubmitRequest (Just (lowerCommands commands)) noTrace)
    let LedgerHandle{port} = h
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.commandSubmissionServiceClient client
        let CommandSubmissionService rpc = service
        response <- rpc request
        Empty{} <- unwrap response
        return ()


wrap :: r -> ClientRequest 'Normal r a
wrap r = ClientNormalRequest r timeout mdm
    where timeout = 3

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse e -> fail (show e)

-- wrap LL.Transaction to show summary
newtype LL_Transaction = LL_Transaction { low :: LL.Transaction } --TODO: remove

instance Show LL_Transaction where
    show LL_Transaction{low} = _summary
        where
            _summary = "Trans:id=" <> Text.unpack transactionTransactionId
            _full = show low
            LL.Transaction{transactionTransactionId} = low

-- TODO: return (HL) [Transaction]
getTransactionStream :: LedgerHandle -> Party -> IO (ResponseStream LL_Transaction)
getTransactionStream h party = do
    let tag = "getTransactionStream for " <> show party
    let LedgerHandle{port,lid} = h
    chan <- newChan
    let request = mkGetTransactionsRequest lid offsetBegin Nothing (filterEverthingForParty party)
    forkIO_ tag $
        LL.withGRPCClient (config port) $ \client -> do
            rpcs <- LL.transactionServiceClient client
            let (TransactionService rpc1 _ _ _ _ _ _) = rpcs
            sendToChan request f chan rpc1
    return $ ResponseStream{chan}
    where f = map LL_Transaction . Vector.toList . getTransactionsResponseTransactions

-- TODO: return (HL) [Completion]
getCompletionStream :: LedgerHandle -> ApplicationId -> [Party] -> IO (ResponseStream LL.Completion)
getCompletionStream h aid partys = do
    let tag = "getCompletionStream for " <> show (aid,partys)
    let LedgerHandle{port,lid} = h
    chan <- newChan
    let request = mkCompletionStreamRequest lid aid partys
    forkIO_ tag $
        LL.withGRPCClient (config port) $ \client -> do
            rpcs <- LL.commandCompletionServiceClient client
            let (CommandCompletionService rpc1 _) = rpcs
            sendToChan request (Vector.toList . completionStreamResponseCompletions) chan  rpc1
    return $ ResponseStream{chan}

sendToChan :: a -> (b -> [c]) -> Chan c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToChan request f chan rpc1 = do
    ClientReaderResponse _meta _code _details <- rpc1 $
        ClientReaderRequest request timeout mdm $ \ _mdm recv -> fix $
        \again -> do
            either <- recv
            case either of
                Left e -> fail (show e)
                Right Nothing -> return ()
                Right (Just x) -> do writeList2Chan chan (f x); again
    return ()
        -- After a minute, we stop collecting the events.
        -- But we ought to wait indefinitely.
        where timeout = 60

config :: Port -> ClientConfig
config port =
    ClientConfig { clientServerHost = Host "localhost"
                 , clientServerPort = port
                 , clientArgs = []
                 , clientSSLConfig = Nothing
                 }

mdm :: MetadataMap
mdm = MetadataMap Map.empty


-- Low level data mapping for Request

mkGetTransactionsRequest :: LedgerId -> LedgerOffset -> Maybe LedgerOffset -> TransactionFilter -> GetTransactionsRequest
mkGetTransactionsRequest (LedgerId id) begin end filter = GetTransactionsRequest {
    getTransactionsRequestLedgerId = id,
    getTransactionsRequestBegin = Just begin,
    getTransactionsRequestEnd = end,
    getTransactionsRequestFilter = Just filter,
    getTransactionsRequestVerbose = False,
    getTransactionsRequestTraceContext = noTrace
    }

mkCompletionStreamRequest :: LedgerId -> ApplicationId -> [Party] -> CompletionStreamRequest
mkCompletionStreamRequest (LedgerId id) aid parties = CompletionStreamRequest {
    completionStreamRequestLedgerId = id,
    completionStreamRequestApplicationId = unApplicationId aid,
    completionStreamRequestParties = Vector.fromList (map unParty parties),
    completionStreamRequestOffset = Just offsetBegin
    }

offsetBegin :: LedgerOffset
offsetBegin = LedgerOffset {ledgerOffsetValue = Just (LedgerOffsetValueBoundary (LL.Enumerated (Right boundaryBegin))) }

boundaryBegin :: LedgerOffset_LedgerBoundary
boundaryBegin = LedgerOffset_LedgerBoundaryLEDGER_BEGIN

filterEverthingForParty :: Party -> TransactionFilter
filterEverthingForParty party =
    TransactionFilter (Map.singleton (unParty party) (Just noFilters))

noFilters :: Filters
noFilters = Filters Nothing

noTrace :: Maybe TraceContext
noTrace = Nothing


-- Misc / logging

forkIO_ :: String -> IO () -> IO ()
forkIO_ tag m = do
    tid <- forkIO $ do m; log $ tag <> " is done"
    log $ "forking " <> tag <> " on " <> show tid
    return ()

log :: String -> IO ()
log s = do
    tid <- myThreadId
    putStrLn $ "[" <> show tid <> "]: " ++ s
