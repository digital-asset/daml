-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
-- | Interface for JSON-RPC.
module DA.Service.JsonRpc.Interface
(
  -- * Conduits for encoding/decoding
  decodeConduit
, encodeConduit

  -- * Internal data and functions
, SentRequests
, Session(..)
, initSession
, processIncoming
) where

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Trans.State
import Data.Aeson
import Data.Aeson.Types (parseMaybe)
import Data.Attoparsec.ByteString
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy.Char8 as L8
import Data.Either
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as M
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Maybe
import Data.Conduit.TMChan
import qualified Data.Vector as V
import DA.Service.JsonRpc.Data

import qualified DA.Service.Logger as Logger

type SentRequests = HashMap Id (TMVar (Maybe Response))

data Session = Session { inCh     :: TBMChan (Either Response Value)
                       , outCh    :: TBMChan Message
                       , reqCh    :: TBMChan BatchRequest
                       , lastId   :: TVar Id
                       , sentReqs :: TVar SentRequests
                       , rpcVer   :: Ver
                       , dead     :: TVar Bool
                       }

initSession :: Ver -> STM Session
initSession v =
    Session <$> newTBMChan 128
            <*> newTBMChan 128
            <*> newTBMChan 128
            <*> newTVar (IdInt 0)
            <*> newTVar M.empty
            <*> return v
            <*> newTVar False

-- Conduit to encode JSON to ByteString.
encodeConduit :: (ToJSON j) => ConduitT j ByteString IO ()
encodeConduit = CL.mapM $ \m -> return . L8.toStrict $ encode m

-- | Conduit to decode incoming messages.  Left Response indicates
-- a response to send back to sender if parsing JSON fails.
decodeConduit :: Ver -> ConduitT ByteString (Either Response Value) IO ()
decodeConduit ver = evalStateT loop Nothing where
    loop = lift await >>= maybe flush (process False)
    flush = get >>= maybe (return ()) (handle True . ($ B8.empty))
    process b = runParser >=> handle b
    runParser ck = maybe (parse json ck) ($ ck) <$> get <* put Nothing

    handle True (Fail "" _ _) = return ()
    handle b (Fail i _ _) = do
        lift . yield . Left $ OrphanError ver (errorParse i)
        unless b loop
    handle _ (Partial k) = put (Just k) >> loop
    handle b (Done rest v) = do
        lift $ yield $ Right v
        if B8.null rest
           then unless b loop
           else process b rest

-- | Process incoming messages. Do not use this directly unless you know
-- what you are doing. This is an internal function.
processIncoming :: Logger.Handle IO -> Session -> IO ()
processIncoming loggerH qs = join . liftIO . atomically $ do
    vEM <- readTBMChan $ inCh qs
    case vEM of
        Nothing -> flush
        Just vE ->
            case vE of
                Right v@Object{} -> do
                    single v
                    return $ do
                        --Logger.logDebug loggerH "received message"
                        processIncoming loggerH qs
                Right v@(Array a) -> do
                    if V.null a
                        then do
                            let e = OrphanError (rpcVer qs) (errorInvalid v)
                            writeTBMChan (outCh qs) $ MsgResponse e
                        else batch (V.toList a)
                    return $ do
                        --Logger.logDebug loggerH "received batch"
                        processIncoming loggerH qs
                Right v -> do
                    let e = OrphanError (rpcVer qs) (errorInvalid v)
                    writeTBMChan (outCh qs) $ MsgResponse e
                    return $ do
                        --Logger.logDebug loggerH "got invalid message"
                        processIncoming loggerH qs
                Left e -> do
                    writeTBMChan (outCh qs) $ MsgResponse e
                    return $ do
                        --Logger.logDebug loggerH "error parsing JSON"
                        processIncoming loggerH qs
  where
    flush = do
        m <- readTVar $ sentReqs qs
        closeTBMChan $ reqCh qs
        closeTBMChan $ outCh qs
        writeTVar (dead qs) True
        mapM_ ((`putTMVar` Nothing) . snd) $ M.toList m
        return $ do
            Logger.logDebug loggerH "session now dead"
            unless (M.null m) $ Logger.logError loggerH "requests remained unfulfilled"

    batch vs = do
        ts <- catMaybes <$> forM vs process
        unless (null ts) $
            if any isRight ts
                then writeTBMChan (reqCh qs) $ BatchRequest $ rights ts
                else writeTBMChan (outCh qs) $ MsgBatch $ lefts ts
    single v = do
        tM <- process v
        case tM of
            Nothing -> return ()
            Just (Right t) -> writeTBMChan (reqCh qs) $ SingleRequest t
            Just (Left e) -> writeTBMChan (outCh qs) e
    process v = do
        let qM = parseMaybe parseJSON v
        case qM of
            Just q -> request q
            Nothing -> do
                let rM = parseMaybe parseJSON v
                case rM of
                    Just r -> response r >> return Nothing
                    Nothing -> do
                        let e = OrphanError (rpcVer qs) (errorInvalid v)
                            m = MsgResponse e
                        return $ Just $ Left m
    request t = return $ Just $ Right t
    response r = do
        let hasid = case r of
                        Response{}      -> True
                        ResponseError{} -> True
                        OrphanError{}   -> False -- Ignore orphan errors
        when hasid $ do
            let x = getResId r
            m <- readTVar (sentReqs qs)
            case x `M.lookup` m of
                Nothing -> return () -- Ignore orphan responses
                Just p -> do
                    writeTVar (sentReqs qs) $ M.delete x m
                    putTMVar p $ Just r

