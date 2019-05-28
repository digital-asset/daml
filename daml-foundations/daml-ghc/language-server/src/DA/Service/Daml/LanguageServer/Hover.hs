-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO MarkedString is deprecated in LSP protocol so we should move to MarkupContent at some point.
{-# OPTIONS_GHC -Wno-deprecations #-}

{-# LANGUAGE OverloadedStrings #-}

-- | Display information on hover.
module DA.Service.Daml.LanguageServer.Hover
    ( handle
    ) where

import DA.Pretty
import DA.LanguageServer.Protocol hiding (Hover)
import Language.Haskell.LSP.Types (Hover(..))

import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import           DA.Service.Daml.LanguageServer.Common
import qualified DA.Service.Logger                     as Logger

import qualified Data.Aeson                            as Aeson
import qualified Data.Text.Extended                    as T

import           Development.IDE.Types.LSP as Compiler
import Development.IDE.Types.Diagnostics

-- | Display information on hover.
handle
    :: Logger.Handle IO
    -> Compiler.IdeState
    -> TextDocumentPositionParams
    -> IO (Either a Aeson.Value)
handle loggerH compilerH (TextDocumentPositionParams (TextDocumentIdentifier uri) pos) = do
    mbResult <- case uriToFilePath' uri of
        Just filePath -> do
          Logger.logInfo loggerH $
              "Hover request at position " <> renderPlain (prettyPosition pos)
              <> " in file: " <> T.pack filePath
          Compiler.atPoint compilerH filePath pos
        Nothing       -> pure Nothing

    case mbResult of
        Just (mbRange, contents) ->
            pure $ Right $ Aeson.toJSON
                  $ Hover
                        (HoverContentsMS $ List $ map showHoverInformation contents)
                        mbRange

        Nothing -> pure $ Right Aeson.Null
  where
    showHoverInformation :: Compiler.HoverText -> MarkedString
    showHoverInformation = \case
        Compiler.HoverHeading h -> PlainString ("***" <> h <> "***:")
        Compiler.HoverDamlCode damlCode -> CodeString $ LanguageString
            { _language = damlLanguageIdentifier
            , _value = damlCode
            }
        Compiler.HoverMarkdown md -> PlainString md
