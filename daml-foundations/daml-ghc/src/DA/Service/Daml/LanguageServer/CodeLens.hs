-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Gather code lenses like scenario execution for a DAML file.
module DA.Service.Daml.LanguageServer.CodeLens
    ( handle
    ) where

import           DA.LanguageServer.Protocol

import           DA.Prelude
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import           DA.Service.Daml.LanguageServer.Common
import qualified DA.Service.Logger                     as Logger

import qualified Data.Aeson                            as Aeson
import qualified Data.Text.Extended                    as T

-- | Gather code lenses like scenario execution for a DAML file.
handle
    :: Logger.Handle IO
    -> Compiler.IdeState
    -> CodeLensParams
    -> IO (Either a Aeson.Value)
handle loggerH compilerH (CodeLensParams (TextDocumentIdentifier uri)) = do
    mbResult <- case documentUriToFilePath uri of
        Just filePath -> do
          Logger.logInfo loggerH $ "CodeLens request for file: " <> T.pack filePath
          vrs <- Compiler.getAssociatedVirtualResources compilerH filePath
          pure $ mapMaybe virtualResourceToCodeLens vrs
        Nothing       -> pure []

    pure $ Right $ Aeson.toJSON mbResult
