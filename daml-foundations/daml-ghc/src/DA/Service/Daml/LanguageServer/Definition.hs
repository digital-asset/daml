-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Go to the definition of a variable.
module DA.Service.Daml.LanguageServer.Definition
    ( handle
    ) where

import           DA.LanguageServer.Protocol

import           DA.Prelude
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import           DA.Service.Daml.LanguageServer.Common
import qualified DA.Service.Logger                     as Logger

import qualified Data.Aeson                            as Aeson
import qualified Data.Text.Extended                    as T

import qualified Development.IDE.Types.Location as Base

-- | Go to the definition of a variable.
handle
    :: Logger.Handle IO
    -> Compiler.IdeState
    -> TextDocumentPositionParams
    -> IO (Either a Aeson.Value)
handle loggerH compilerH (TextDocumentPositionParams docId pos) = do


    mbResult <- case documentUriToFilePath $ tdidUri docId of
        Just filePath -> do
          Logger.logInfo loggerH $
            "Definition request at position " <> renderPretty pos
            <> " in file: " <> T.pack filePath
          Compiler.gotoDefinition compilerH filePath (toAstPosition pos)
        Nothing       -> pure Nothing

    case mbResult of
        Nothing ->
            pure $ Right $ Aeson.toJSON ([] :: [Base.Location])

        Just loc ->
            pure $ Right $ Aeson.toJSON $ fromAstLocation loc
