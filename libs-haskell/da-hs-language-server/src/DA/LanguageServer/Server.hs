-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DA.LanguageServer.Server
  ( runServer
  ) where


import           Control.Concurrent.STM.TChan     (TChan)

import           DA.LanguageServer.Protocol
import           DA.LanguageServer.Conduits
import qualified DA.Service.JsonRpc               as JsonRpc
import qualified DA.Service.Logger                as Logger

import qualified Data.Aeson                       as Aeson
import           Data.Conduit                     ((.|))
import           Data.Conduit.Binary              (sourceHandle, sinkHandle)

import           System.IO
import           GHC.IO.Handle                    (hDuplicate, hDuplicateTo)

------------------------------------------------------------------------
-- Server execution
------------------------------------------------------------------------

runServer
    :: Logger.Handle IO
    -> (ServerRequest -> IO (Either ServerError Aeson.Value))
    -- ^ Request handler for language server requests
    -> (ServerNotification -> IO ())
    -- ^ Notification handler for language server notifications
    -> TChan ClientNotification
    -- ^ Channel for notifications towards the client
    -> IO ()
runServer loggerH reqHandler notifHandler notifChan = do
    -- DEL-6257: Move stdout to another file descriptor and duplicate stderr
    -- to stdout. This guards against stray prints from corrupting the JSON-RPC
    -- message stream.
    newStdout <- hDuplicate stdout
    stderr `hDuplicateTo` stdout

    -- Print out a single space to assert that the above redirection works.
    -- This is interleaved with the logger, hence we just print a space here in
    -- order not to mess up the output too much. Verified that this breaks
    -- the language server tests without the redirection.
    putStr " " >> hFlush stdout

    -- Disable the default line buffering on the handles as the JSON-RPC messages
    -- are not newline delimited.
    hSetBuffering stdin NoBuffering
    hSetBuffering newStdout NoBuffering
    -- We don’t want any newline conversion or encoding on those handles.
    hSetBinaryMode newStdout True
    hSetBinaryMode stdin True
    JsonRpc.runServer
      loggerH
      (sink newStdout) source
      notifChan reqHandler' notifHandler
  where
    reqHandler' req = do
      result <- reqHandler req
      pure $ case result of
        Left err   -> Left $ serverErrorToErrorObj err
        Right resp -> Right resp

    source    = sourceHandle stdin .| contentLengthParserConduit
    sink handle =
          contentLengthEmitterConduit
      .|  sinkHandle handle
