-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

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

import qualified Data.Text.Extended                    as T

import           Development.IDE.Types.LSP as Compiler
import Development.IDE.Types.Diagnostics

-- | Display information on hover.
handle
    :: Logger.Handle IO
    -> Compiler.IdeState
    -> TextDocumentPositionParams
    -> IO (Maybe Hover)
handle loggerH compilerH (TextDocumentPositionParams (TextDocumentIdentifier uri) pos) = do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath -> filePath) -> do
          Logger.logInfo loggerH $
              "Hover request at position " <> renderPlain (prettyPosition pos)
              <> " in file: " <> T.pack (fromNormalizedFilePath filePath)
          Compiler.atPoint compilerH filePath pos
        Nothing       -> pure Nothing

    case mbResult of
        Just (mbRange, contents) ->
            pure $ Just $ Hover
                        (HoverContents $ MarkupContent MkMarkdown $ T.intercalate sectionSeparator $ map showHoverInformation contents)
                        mbRange

        Nothing -> pure Nothing
  where
    showHoverInformation :: Compiler.HoverText -> T.Text
    showHoverInformation = \case
        Compiler.HoverDamlCode damlCode -> T.unlines
            [ "```" <> damlLanguageIdentifier
            , damlCode
            , "```"
            ]
        Compiler.HoverMarkdown md -> md
