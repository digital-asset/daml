-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE ExistentialQuantification  #-}

-- WARNING: A copy of DA.Service.Daml.LanguageServer, try to keep them in sync
-- This version removes the daml: handling
module Development.IDE.LSP.LanguageServer
    ( runLanguageServer
    ) where

import           Development.IDE.LSP.Protocol
import           Development.IDE.LSP.Server hiding (runServer)
import qualified Language.Haskell.LSP.Control as LSP
import qualified Language.Haskell.LSP.Core as LSP
import Control.Concurrent.STM
import Control.Concurrent.Extra
import Control.Concurrent.Async
import Data.Default
import           GHC.IO.Handle                    (hDuplicate, hDuplicateTo)
import System.IO
import Control.Monad

import Development.IDE.LSP.Definition
import Development.IDE.LSP.Hover
import Development.IDE.LSP.Notifications
import Development.IDE.Core.Service
import Development.IDE.Core.FileStore
import Language.Haskell.LSP.Core (LspFuncs(..))
import Language.Haskell.LSP.Messages


runLanguageServer
    :: ((FromServerMessage -> IO ()) -> VFSHandle -> IO IdeState)
    -> IO ()
runLanguageServer getIdeState = do
    -- Move stdout to another file descriptor and duplicate stderr
    -- to stdout. This guards against stray prints from corrupting the JSON-RPC
    -- message stream.
    newStdout <- hDuplicate stdout
    stderr `hDuplicateTo` stdout

    -- Print out a single space to assert that the above redirection works.
    -- This is interleaved with the logger, hence we just print a space here in
    -- order not to mess up the output too much. Verified that this breaks
    -- the language server tests without the redirection.
    putStr " " >> hFlush stdout

    clientMsgChan :: TChan AddItem <- newTChanIO
    -- These barriers are signaled when the threads reading from these chans exit.
    -- This should not happen but if it does, we will make sure that the whole server
    -- dies and can be restarted instead of losing threads silently.
    clientMsgBarrier <- newBarrier

    let withResponse wrap f = Just $ \r -> atomically $ writeTChan clientMsgChan $ AddResponse r wrap f
    let withNotification f = Just $ \r -> atomically $ writeTChan clientMsgChan $ AddNotification r f
    let runHandler = WithMessage{withResponse, withNotification}
    handlers <- mergeHandlers [setHandlersDefinition, setHandlersHover, setHandlersNotifications, setHandlersIgnore] runHandler def

    void $ waitAnyCancel =<< traverse async
        [ void $ LSP.runWithHandles
            stdin
            newStdout
            ( const $ Right ()
            , handleInit (signalBarrier clientMsgBarrier ()) clientMsgChan
            )
            handlers
            options
            Nothing
        , void $ waitBarrier clientMsgBarrier
        ]
    where
        handleInit :: IO () -> TChan AddItem -> LSP.LspFuncs () -> IO (Maybe err)
        handleInit exitClientMsg clientMsgChan lspFuncs@LSP.LspFuncs{..} = do
            ide <- getIdeState sendFunc (makeLSPVFSHandle lspFuncs)
            _ <- flip forkFinally (const exitClientMsg) $ forever $ do
                msg <- atomically $ readTChan clientMsgChan
                case msg of
                    AddNotification NotificationMessage{_params} act -> act ide _params
                    AddResponse RequestMessage{_id, _params} wrap act -> do
                        res <- act ide _params
                        sendFunc $ wrap $ ResponseMessage "2.0" (responseId _id) (Just res) Nothing
            pure Nothing


-- | Things that get sent to us, but we don't deal with.
--   Set them to avoid a warning in VS Code output.
setHandlersIgnore :: WithMessage -> LSP.Handlers -> IO LSP.Handlers
setHandlersIgnore _ x = return x
    {LSP.cancelNotificationHandler = none
    ,LSP.initializedHandler = none
    ,LSP.codeLensHandler = none -- FIXME: Stop saying we support it in 'options'
    }
    where none = Just $ const $ return ()


mergeHandlers :: [WithMessage -> LSP.Handlers -> IO LSP.Handlers] -> WithMessage -> LSP.Handlers -> IO LSP.Handlers
mergeHandlers = foldl f (\_ a -> return a)
    where f x1 x2 r a = x1 r a >>= x2 r


data AddItem
    = forall m req resp . AddResponse (RequestMessage m req resp) (ResponseMessage resp -> FromServerMessage) (IdeState -> req -> IO resp)
    | forall m req . AddNotification (NotificationMessage m req) (IdeState -> req -> IO ())


options :: LSP.Options
options = def
    { LSP.textDocumentSync = Just TextDocumentSyncOptions
          { _openClose = Just True
          , _change = Just TdSyncIncremental
          , _willSave = Nothing
          , _willSaveWaitUntil = Nothing
          , _save = Just $ SaveOptions $ Just False
          }
    , LSP.codeLensProvider = Just $ CodeLensOptions $ Just False
    }
