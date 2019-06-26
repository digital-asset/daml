-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Gather code lenses like scenario execution for a DAML file.
module DA.Service.Daml.LanguageServer.CodeLens
    ( setHandlersCodeLens
    ) where

import Language.Haskell.LSP.Types
import qualified Data.Aeson as Aeson
import Development.IDE.Core.Service.Daml
import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import Development.IDE.LSP.Server
import qualified Language.Haskell.LSP.Core as LSP
import Language.Haskell.LSP.Messages
import Development.IDE.Types.Logger
import Development.IDE.Types.Location

-- | Gather code lenses like scenario execution for a DAML file.
handle
    :: Compiler.IdeState
    -> CodeLensParams
    -> IO (List CodeLens)
handle ide (CodeLensParams (TextDocumentIdentifier uri)) = do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath -> filePath) -> do
          logInfo (ideLogger ide) $ "CodeLens request for file: " <> T.pack (fromNormalizedFilePath filePath)
          vrs <- Compiler.getAssociatedVirtualResources ide filePath
          pure $ mapMaybe virtualResourceToCodeLens vrs
        Nothing       -> pure []

    pure $ List $ toList mbResult

-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Range, T.Text, VirtualResource)
    -> Maybe CodeLens
virtualResourceToCodeLens (range, title, vr) =
 Just CodeLens
    { _range = range
    , _command = Just $ Command
        "Scenario results"
        "daml.showResource"
        (Just $ List
              [ Aeson.String title
              , Aeson.String $ Compiler.virtualResourceToUri vr])
    , _xdata = Nothing
    }

setHandlersCodeLens :: PartialHandlers
setHandlersCodeLens = PartialHandlers $ \WithMessage{..} x -> return x{
    LSP.codeLensHandler = withResponse RspCodeLens handle
    }
