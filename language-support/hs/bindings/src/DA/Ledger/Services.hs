-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger.Services( -- WIP: Ledger API services

    -- connect
    Port(..),
    LedgerHandle, connectLogging, identity,

    -- services
    listPackages,
    getPackage,
    getTransactionsPF, --TODO: make service combos like this distinct
    completions,
    submitCommands,

    ) where

import Control.Concurrent
import Control.Exception (Exception,SomeException,catch,throwIO)
import Control.Monad.Fix (fix)
import qualified Data.Map as Map(empty,singleton)
import qualified Data.Vector as Vector(toList,fromList)

import qualified Proto3.Suite(fromByteString)
import qualified DA.Daml.LF.Ast           as LF(Package)
import qualified DA.Daml.LF.Proto3.Decode as Decode(decodePayload)

import DA.Ledger.Stream
import DA.Ledger.PastAndFuture
import DA.Ledger.Types
import DA.Ledger.Convert(lowerCommands,raiseTransaction)
import DA.Ledger.LowLevel hiding(Commands,Transaction)
import qualified DA.Ledger.LowLevel as LL

data LedgerHandle = LedgerHandle {
    log :: String -> IO (),
    port :: Port,
    lid :: LedgerId
    }

identity :: LedgerHandle -> LedgerId
identity LedgerHandle{lid} = lid

connectLogging :: (String -> IO ()) -> Port -> IO LedgerHandle
connectLogging log port = wrapE "connect" $ do
    lid <- getLedgerIdentity port
    return $ LedgerHandle {log, port, lid}

getLedgerIdentity :: Port -> IO LedgerId
getLedgerIdentity port = wrapE "getLedgerIdentity" $ do
    let request = GetLedgerIdentityRequest noTrace
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.ledgerIdentityServiceClient client
        let LedgerIdentityService{ledgerIdentityServiceGetLedgerIdentity=rpc} = service
        response <- rpc (ClientNormalRequest request timeout mdm)
        GetLedgerIdentityResponse text <- unwrap response
        return $ LedgerId text

listPackages :: LedgerHandle -> IO [PackageId]
listPackages LedgerHandle{port,lid} = wrapE "listPackages" $ do
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.packageServiceClient client
        let PackageService {packageServiceListPackages=rpc} = service
        let request = ListPackagesRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout mdm)
        ListPackagesResponse xs <- unwrap response
        return $ map PackageId $ Vector.toList xs

data Package = Package LF.Package deriving Show

getPackage :: LedgerHandle -> PackageId -> IO Package
getPackage LedgerHandle{port,lid} pid = wrapE "getPackage" $ do
    let request = GetPackageRequest (unLedgerId lid) (unPackageId pid) noTrace
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.packageServiceClient client
        let PackageService {packageServiceGetPackage=rpc} = service
        response <- rpc (ClientNormalRequest request timeout mdm)
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
-- transaction_service

getLedgerEnd :: LedgerHandle -> IO LedgerOffset
getLedgerEnd LedgerHandle{port,lid} = wrapE "getLedgerEnd" $ do
    LL.withGRPCClient (config port) $ \client -> do
        service <- LL.transactionServiceClient client
        let TransactionService{transactionServiceGetLedgerEnd=rpc} = service
        let request = GetLedgerEndRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout mdm)
        GetLedgerEndResponse (Just offset) <- unwrap response --TODO: always be a Just?
        return offset

runTransRequest :: LedgerHandle -> GetTransactionsRequest -> IO (Stream Transaction)
runTransRequest LedgerHandle{port} request = wrapE "transactions" $ do
    stream <- newStream
    _ <- forkIO $
        LL.withGRPCClient (config port) $ \client -> do
            service <- LL.transactionServiceClient client
            let TransactionService {transactionServiceGetTransactions=rpc} = service
            sendToStream request f stream rpc
    return stream
    where f = map raise . Vector.toList . getTransactionsResponseTransactions
          raise x = case raiseTransaction x of
              Left reason -> Left (Abnormal $ "failed to parse transaction because: " <> show reason <> ":\n" <> show x)
              Right h -> Right h

getTransactionsPF :: LedgerHandle -> Party -> IO (PastAndFuture Transaction)
getTransactionsPF h@LedgerHandle{lid} party = do
    now <- getLedgerEnd h
    let req1 = transRequestUntil lid now party
    let req2 = transRequestFrom lid now party
    s1 <- runTransRequest h req1
    s2 <- runTransRequest h req2
    past <- streamToList h s1
    return PastAndFuture{past, future = s2}

streamToList :: LedgerHandle -> Stream a -> IO [a]
streamToList h@LedgerHandle{log} stream = do
    takeStream stream >>= \case
        Left EOS -> return []
        Left Abnormal{reason} -> do
            log $ "streamToList, stream closed because: " <> reason
            return []
        Right x -> fmap (x:) $ streamToList h stream


-- TODO: return (HL) [Completion]
completions :: LedgerHandle -> ApplicationId -> [Party] -> IO (Stream LL.Completion)
completions LedgerHandle{port,lid} aid partys = wrapE "completions" $ do
    stream <- newStream
    let request = mkCompletionStreamRequest lid aid partys
    _ <- forkIO $
        LL.withGRPCClient (config port) $ \client -> do
            service <- LL.commandCompletionServiceClient client
            let CommandCompletionService {commandCompletionServiceCompletionStream=rpc} = service
            sendToStream request (map Right . Vector.toList . completionStreamResponseCompletions) stream rpc
    return stream

sendToStream :: a -> (b -> [Either Closed c]) -> Stream c -> (ClientRequest 'ServerStreaming a b -> IO (ClientResult 'ServerStreaming b)) -> IO ()
sendToStream request f stream rpc = do
    ClientReaderResponse _meta _code _details <- rpc $
        ClientReaderRequest request timeout mdm $ \ _mdm recv -> fix $
        \again -> do
            either <- recv
            case either of
                Left e -> do
                    writeStream stream (Left (Abnormal (show e)))
                    return ()
                Right Nothing -> do
                    writeStream stream (Left EOS)
                    return ()
                Right (Just x) ->
                    do
                        mapM_ (writeStream stream) (f x)
                        again
    return ()
        where timeout = 6000 -- TODO: come back and think about this!

config :: Port -> ClientConfig
config port =
    ClientConfig { clientServerHost = Host "localhost"
                 , clientServerPort = port
                 , clientArgs = []
                 , clientSSLConfig = Nothing
                 }


-- Low level data mapping for Request

transRequestUntil :: LedgerId -> LedgerOffset -> Party -> GetTransactionsRequest
transRequestUntil lid offset party =
    mkGetTransactionsRequest lid offsetBegin (Just offset) (filterEverthingForParty party)

transRequestFrom :: LedgerId -> LedgerOffset -> Party -> GetTransactionsRequest
transRequestFrom lid offset party =
    mkGetTransactionsRequest lid offset Nothing (filterEverthingForParty party)


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
