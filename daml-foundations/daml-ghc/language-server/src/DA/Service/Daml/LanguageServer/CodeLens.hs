-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Gather code lenses like scenario execution for a DAML file.
module DA.Service.Daml.LanguageServer.CodeLens
    ( handle
    ) where

import           DA.LanguageServer.Protocol
import Language.Haskell.LSP.Types

import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import           DA.Service.Daml.LanguageServer.Common
import qualified Development.IDE.Logger as Logger
import Development.IDE.Types.Diagnostics

-- | Gather code lenses like scenario execution for a DAML file.
handle
    :: Logger.Handle
    -> Compiler.IdeState
    -> CodeLensParams
    -> IO (List CodeLens)
handle loggerH compilerH (CodeLensParams (TextDocumentIdentifier uri)) = do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath -> filePath) -> do
          Logger.logInfo loggerH $ "CodeLens request for file: " <> T.pack (fromNormalizedFilePath filePath)
          vrs <- Compiler.getAssociatedVirtualResources compilerH filePath
          pure $ mapMaybe virtualResourceToCodeLens vrs
        Nothing       -> pure []

    pure $ List $ toList mbResult
