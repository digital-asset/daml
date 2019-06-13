-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Go to the definition of a variable.
module Development.IDE.LSP.Definition
    ( handle
    ) where

import           Development.IDE.LSP.Protocol
import Development.IDE.Types.Diagnostics
import Development.IDE.State.Rules as CompilerService
import qualified Development.IDE.State.Service as Compiler
import Development.IDE.State.Shake as S
import           Development.IDE.Types.Diagnostics as Base

import qualified Development.IDE.Logger as Logger

import qualified Data.Text as T
import Data.Text.Prettyprint.Doc
import Data.Text.Prettyprint.Doc.Render.Text

-- | Go to the definition of a variable.
handle
    :: Logger.Handle
    -> Compiler.IdeState
    -> TextDocumentPositionParams
    -> IO LocationResponseParams
handle loggerH compilerH (TextDocumentPositionParams (TextDocumentIdentifier uri) pos) = do


    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath -> filePath) -> do
          Logger.logInfo loggerH $
            "Definition request at position " <>
            renderStrict (layoutPretty defaultLayoutOptions $ prettyPosition pos) <>
            " in file: " <> T.pack (fromNormalizedFilePath filePath)
          gotoDefinition compilerH filePath pos
        Nothing       -> pure Nothing

    case mbResult of
        Nothing ->
            pure $ MultiLoc []

        Just loc ->
            pure $ SingleLoc loc

gotoDefinition
    :: IdeState
    -> NormalizedFilePath
    -> Base.Position
    -> IO (Maybe Base.Location)
gotoDefinition service afp pos = do
    S.logDebug service $ "Goto definition: " <> T.pack (show afp)
    CompilerService.runAction service (CompilerService.getDefinition afp pos)
