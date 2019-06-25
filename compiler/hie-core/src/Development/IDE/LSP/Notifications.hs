-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes #-}

module Development.IDE.LSP.Notifications
    ( setHandlersNotifications
    ) where

import           Development.IDE.LSP.Protocol
import           Development.IDE.LSP.Server hiding (runServer)
import qualified Language.Haskell.LSP.Core as LSP
import qualified Language.Haskell.LSP.Types as LSP

import Development.IDE.Types.Logger
import Development.IDE.Core.Service
import Development.IDE.Types.Location

import qualified Data.Set                                  as S
import qualified Data.Text as T

import Development.IDE.Core.FileStore
import Development.IDE.Core.OfInterest


textShow :: Show a => a -> T.Text
textShow = T.pack . show


whenUriFile :: IdeState -> Uri -> (NormalizedFilePath -> IO ()) -> IO ()
whenUriFile ide uri act = case LSP.uriToFilePath uri of
    Just file -> act $ toNormalizedFilePath file
    Nothing -> logWarning (ideLogger ide) $ "Unknown scheme in URI: " <> textShow uri

setHandlersNotifications :: PartialHandlers
setHandlersNotifications = PartialHandlers $ \WithMessage{..} x -> return x
    {LSP.didOpenTextDocumentNotificationHandler = withNotification $
        \ide (DidOpenTextDocumentParams TextDocumentItem{_uri}) -> do
            setSomethingModified ide
            whenUriFile ide _uri $ \file ->
                modifyFilesOfInterest ide (S.insert file)
            logInfo (ideLogger ide) $ "Opened text document: " <> textShow _uri

    ,LSP.didChangeTextDocumentNotificationHandler = withNotification $
        \ide (DidChangeTextDocumentParams VersionedTextDocumentIdentifier{_uri} _) -> do
            setSomethingModified ide
            logInfo (ideLogger ide) $ "Modified text document: " <> textShow _uri

    ,LSP.didCloseTextDocumentNotificationHandler = withNotification $
        \ide (DidCloseTextDocumentParams TextDocumentIdentifier{_uri}) -> do
            setSomethingModified ide
            whenUriFile ide _uri $ \file ->
                modifyFilesOfInterest ide (S.delete file)
            logInfo (ideLogger ide) $ "Closed text document: " <> textShow _uri
  }
