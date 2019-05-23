-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger( -- WIP: High level interface to the Ledger API services

    -- services
    listPackages,
    getPackage, Package,
    transactions,
    completions,
    submitCommands,

    module DA.Ledger.Types,
    LL_Transaction, -- TODO: remove when coded `raise' (LL->HL) operation on Transaction types

    Port(..),

    Stream, takeStream, getStreamContents,

    LedgerHandle, connect, identity,

    ) where


import DA.Ledger.Types

import Control.Concurrent
import Control.Exception (Exception,SomeException,catch,throwIO)
import Control.Monad.Fix (fix)
import qualified Data.Map as Map(empty,singleton)
import qualified Data.Text.Lazy as Text(unpack)
import qualified Data.Vector as Vector(toList,fromList)
import DA.Ledger.Convert(lowerCommands)
import DA.Ledger.LowLevel as LL hiding(Commands)

import qualified Proto3.Suite(fromByteString)
import qualified DA.Daml.LF.Ast           as LF(Package)
import qualified DA.Daml.LF.Proto3.Decode as Decode(decodePayload)

data LedgerHandle = LedgerHandle { port :: Port, lid :: LedgerId }

identity :: LedgerHandle -> LedgerId
identity LedgerHandle{lid} = lid

connect :: Port -> IO LedgerHandle
connect port = wrapE "connect" $ do
    lid <- getLedgerIdentity port
    return $ LedgerHandle {port, lid}

getLedgerIdentity :: Port -> IO LedgerId
getLedgerIdentity port = wrapE "getLedgerIdentity" $ do
    let request = GetLedgerIdentityRequest noTrace
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.ledgerIdentityServiceClient client
        let LedgerIdentityService rpc = service
        response <- rpc (ClientNormalRequest request timeout mdm)
        GetLedgerIdentityResponse text <- unwrap response
        return $ LedgerId text

listPackages :: LedgerHandle -> IO [PackageId]
listPackages LedgerHandle{port,lid} = wrapE "listPackages" $ do
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.packageServiceClient client
        let PackageService rpc1 _ _ = service
        let request = ListPackagesRequest (unLedgerId lid) noTrace
        response <- rpc1 (ClientNormalRequest request timeout mdm)
        ListPackagesResponse xs <- unwrap response
        return $ map PackageId $ Vector.toList xs

data Package = Package LF.Package deriving Show

getPackage :: LedgerHandle -> PackageId -> IO Package
getPackage LedgerHandle{port,lid} pid = wrapE "getPackage" $ do
    let request = GetPackageRequest (unLedgerId lid) (unPackageId pid) noTrace
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.packageServiceClient client
        let PackageService _ rpc2 _ = service
        response <- rpc2 (ClientNormalRequest request timeout mdm)
        GetPackageResponse _ bs _ <- unwrap response
        let ap = either (error . show) id (Proto3.Suite.fromByteString bs)
        case Decode.decodePayload ap of
            Left e -> fail (show e)
            Right package -> return (Package package)

submitCommands :: LedgerHandle -> Commands -> IO ()
submitCommands LedgerHandle{port} commands = wrapE "submitCommands" $ do
    let request = SubmitRequest (Just (lowerCommands commands)) noTrace
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.commandSubmissionServiceClient client
        let CommandSubmissionService rpc = service
        response <- rpc (ClientNormalRequest request timeout mdm)
        Empty{} <- unwrap response
        return ()

timeout :: Int -- Seconds
timeout = 30 -- TODO: sensible default? user configuarable?

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
    ClientNormalResponse x _m1 _m2 _status _details -> return x
    ClientErrorResponse e -> fail (show e)

mdm :: MetadataMap
mdm = MetadataMap Map.empty



----------------------------------------------------------------------
-- Services with streaming responses

data Elem a = Elem a | Eend | Eerr String

deElem :: Elem a -> IO a
deElem = \case
    Elem a -> return a
    Eend -> fail "readStream, end"
    Eerr s -> fail $ "readStream, err: " <> s

newtype Stream a = Stream { mv :: MVar (Elem a) }

newStream :: IO (Stream a)
newStream = do
    mv <- newEmptyMVar
    return Stream{mv}

writeStream :: Stream a -> Elem a -> IO () -- internal use only
writeStream Stream{mv} elem = putMVar mv elem

takeStream :: Stream a -> IO a
takeStream Stream{mv} = takeMVar mv >>= deElem

data StreamState = SS -- TODO
getStreamContents :: Stream a -> IO ([a],StreamState)
getStreamContents Stream{mv} = do xs <- loop ; return (xs,SS)
    where
        loop = do
            tryTakeMVar mv >>= \case
                Nothing -> return []
                Just e -> do
                    x <- deElem e
                    xs <- loop
                    return (x:xs)

-- wrap LL.Transaction to show summary
newtype LL_Transaction = LL_Transaction { low :: LL.Transaction } --TODO: remove
    deriving Eq

instance Show LL_Transaction where
    show LL_Transaction{low} = _summary
        where
            _summary = "Trans:id=" <> Text.unpack transactionTransactionId
            _full = show low
            LL.Transaction{transactionTransactionId} = low


-- TODO: return (HL) [Transaction]
transactions :: LedgerHandle -> Party -> IO (Stream LL_Transaction)
transactions LedgerHandle{port,lid} party = wrapE "transactions" $ do
    stream <- newStream
    let request = mkGetTransactionsRequest lid offsetBegin Nothing (filterEverthingForParty party)
    _ <- forkIO $ --TODO: dont use forkIO
        LL.withGRPCClient (config port) $ \client -> do
            rpcs <- LL.transactionServiceClient client
            let (TransactionService rpc1 _ _ _ _ _ _) = rpcs
            sendToStream request f stream rpc1
    return stream
    where f = map LL_Transaction . Vector.toList . getTransactionsResponseTransactions


-- TODO: return (HL) [Completion]
completions :: LedgerHandle -> ApplicationId -> [Party] -> IO (Stream LL.Completion)
completions LedgerHandle{port,lid} aid partys = wrapE "completions" $ do
    stream <- newStream
    let request = mkCompletionStreamRequest lid aid partys
    _ <- forkIO $ --TODO: dont use forkIO
        LL.withGRPCClient (config port) $ \client -> do
            rpcs <- LL.commandCompletionServiceClient client
            let (CommandCompletionService rpc1 _) = rpcs
            sendToStream request (Vector.toList . completionStreamResponseCompletions) stream rpc1
    return stream

sendToStream :: a -> (b -> [c]) -> Stream c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToStream request f stream rpc1 = do
    ClientReaderResponse _meta _code _details <- rpc1 $
        ClientReaderRequest request timeout mdm $ \ _mdm recv -> fix $ -- TODO: whileM better?
        \again -> do
            either <- recv
            case either of
                Left e -> do
                    writeStream stream (Eerr (show e)) -- notify reader of error
                    return ()
                Right Nothing -> do
                    writeStream stream Eend -- notify reader of end-of-stream
                    return ()
                Right (Just x) ->
                    do
                        mapM_ (writeStream stream . Elem) (f x)
                        again
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


data LedgerApiException = LedgerApiException { tag :: String, underlying :: SomeException } deriving Show
instance Exception LedgerApiException

wrapE :: String -> IO a -> IO a
wrapE tag io = io `catch` \e -> throwIO (LedgerApiException {tag,underlying=e})
