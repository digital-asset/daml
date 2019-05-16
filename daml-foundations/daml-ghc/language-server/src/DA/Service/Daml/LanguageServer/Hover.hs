-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Display information on hover.
module DA.Service.Daml.LanguageServer.Hover
    ( handle
    ) where

import           DA.LanguageServer.Protocol

import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import           DA.Service.Daml.LanguageServer.Common
import qualified DA.Service.Logger                     as Logger

import qualified Data.Aeson                            as Aeson
import qualified Data.Text.Extended                    as T

import           Development.IDE.Types.LSP as Compiler

-- | Display information on hover.
handle
    :: Logger.Handle IO
    -> Compiler.IdeState
    -> TextDocumentPositionParams
    -> IO (Either a Aeson.Value)
handle loggerH compilerH (TextDocumentPositionParams docId pos) = do
    mbResult <- case documentUriToFilePath $ tdidUri docId of
        Just filePath -> do
          Logger.logInfo loggerH $
              "Hover request at position " <> renderPretty pos
              <> " in file: " <> T.pack filePath
          Compiler.atPoint compilerH filePath (toAstPosition pos)
        Nothing       -> pure Nothing

    case mbResult of
        Just (mbRange, contents) ->
            pure $ Right $ Aeson.toJSON
                  $ HoverResult
                        (map showHoverInformation contents)
                        (fromAstRange <$> mbRange)

        Nothing -> pure $ Right Aeson.Null
  where
    showHoverInformation :: Compiler.HoverText -> MarkedString
    showHoverInformation = \case
        Compiler.HoverHeading h -> MarkedString ("***" <> h <> "***:")
        Compiler.HoverDamlCode damlCode -> MarkedStringWithLanguage
            { mswlLanguage = damlLanguageIdentifier
            , mswlValue = damlCode
            }
        Compiler.HoverMarkdown md -> MarkedString md
