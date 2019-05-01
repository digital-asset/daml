-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module LedgerHL(
    Port(..),
    LedgerHandle,
    LedgerId,
    Party(..),
    Transaction,
    ResponseStream,
    connect,
    identity,
    getTransactionStream,
    nextResponse,
    ) where

import           Control.Concurrent
import           Control.Monad.Fix  (fix)
import qualified Data.Map           as Map
import           Data.Text.Lazy     (Text)
import qualified Data.Text.Lazy     as Text
import           Prelude            hiding (log)

import           LedgerId
import           Transaction

import Network.GRPC.HighLevel.Generated (
    withGRPCClient,
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
    )

import Com.Digitalasset.Ledger.Api.V1.LedgerIdentityService as LIS
import Com.Digitalasset.Ledger.Api.V1.TransactionService as TS

import Com.Digitalasset.Ledger.Api.V1.TransactionService(GetTransactionsRequest(..))
import Com.Digitalasset.Ledger.Api.V1.TransactionFilter(
    TransactionFilter(..),
    Filters(..))

import Com.Digitalasset.Ledger.Api.V1.LedgerOffset(
    LedgerOffset(..),
    LedgerOffsetValue(..),
    LedgerOffset_LedgerBoundary(..))

import Com.Digitalasset.Ledger.Api.V1.TraceContext(TraceContext)
import qualified Proto3.Suite.Types as PST


data LedgerHandle = LedgerHandle { port :: Port, id :: LedgerId }

identity :: LedgerHandle -> LedgerId
identity LedgerHandle{id} = id


newtype Party = Party { party :: Text }

instance Show Party where
    show Party{party} = Text.unpack party
    

newtype ResponseStream a = ResponseStream { chan :: Chan a }

nextResponse :: ResponseStream a -> IO a
nextResponse ResponseStream{chan} = readChan chan


connect :: Port -> IO LedgerHandle
connect port = do
    id <- getLedgerIdentity port
    return$ LedgerHandle {port, id}

getLedgerIdentity :: Port -> IO LedgerId
getLedgerIdentity port = do
    log "ledgerIdentity"
    withGRPCClient (config port) $ \client -> do
        rpcs <- LIS.ledgerIdentityServiceClient client
        callLedgerIdService rpcs

callLedgerIdService :: LIS.LedgerIdentityService ClientRequest ClientResult -> IO LedgerId
callLedgerIdService (LIS.LedgerIdentityService rpc) = do
    response <- rpc (wrap (LIS.GetLedgerIdentityRequest noTrace))
    LIS.GetLedgerIdentityResponse text <- unwrap response
    return$ LedgerId text

wrap :: r -> ClientRequest 'Normal r a
wrap r = ClientNormalRequest r timeout mdm
    where timeout = 3

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse e -> fail (show e)


getTransactionStream :: LedgerHandle -> Party -> IO (ResponseStream Transaction)
getTransactionStream h party = do
    let tag = "getTransactionStream for " <> show party
    let LedgerHandle{id,port} = h
    chan <- newChan
    let request = mkGetTransactionsRequest id offsetBegin Nothing (filterEverthingForParty party)
    forkIO_ tag $
        withGRPCClient (config port) $ \client -> do
            rpcs <- TS.transactionServiceClient client
            let (TS.TransactionService rpc1 _ _ _ _) = rpcs
            sendToChan request fromServiceResponse chan  rpc1
    return$ ResponseStream{chan}

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


-- Low level data mapping

mkGetTransactionsRequest :: LedgerId -> LedgerOffset -> Maybe LedgerOffset -> TransactionFilter -> GetTransactionsRequest
mkGetTransactionsRequest (LedgerId id) begin end filter = GetTransactionsRequest {
    getTransactionsRequestLedgerId = id,
    getTransactionsRequestBegin = Just begin,
    getTransactionsRequestEnd = end,
    getTransactionsRequestFilter = Just filter,
    getTransactionsRequestVerbose = False, --fixed
    getTransactionsRequestTraceContext = noTrace --fixed
    }

offsetBegin :: LedgerOffset
offsetBegin = LedgerOffset {ledgerOffsetValue = Just (LedgerOffsetValueBoundary (PST.Enumerated (Right boundaryBegin))) }

boundaryBegin :: LedgerOffset_LedgerBoundary
boundaryBegin = LedgerOffset_LedgerBoundaryLEDGER_BEGIN

filterEverthingForParty :: Party -> TransactionFilter
filterEverthingForParty Party{party} = TransactionFilter (Map.singleton party (Just noFilters))

noFilters :: Filters
noFilters = Filters Nothing

noTrace :: Maybe TraceContext
noTrace = Nothing


-- Misc / logging

forkIO_ :: String -> IO () -> IO ()
forkIO_ tag m = do
    tid <- forkIO$ do m; log$ tag <> " is done"
    log$ "forking " <> tag <> " on " <> show tid
    return ()
    
log :: String -> IO ()
log s = do
    tid <- myThreadId
    putStrLn$ "[" <> show tid <> "]: " ++ s
