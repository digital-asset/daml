-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Gather code lenses like scenario execution for a DAML file.
module DA.Daml.LanguageServer.CodeLens
    ( setHandlersCodeLens
    ) where

import DA.Daml.LFConversion.UtilLF (sourceLocToRange)
import qualified DA.Daml.LF.Ast as LF
import Language.Haskell.LSP.Types
import qualified Data.Aeson as Aeson
import Development.IDE.Core.Service.Daml
import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import Development.IDE.LSP.Server
import qualified Language.Haskell.LSP.Core as LSP
import Language.Haskell.LSP.Messages
import Development.IDE.Core.Rules.Daml
import Development.IDE.Types.Logger
import Development.IDE.Types.Location

-- | Gather code lenses like scenario execution for a DAML file.
handle
    :: IdeState
    -> CodeLensParams
    -> IO (List CodeLens)
handle ide (CodeLensParams (TextDocumentIdentifier uri)) = do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath -> filePath) -> do
          logInfo (ideLogger ide) $ "CodeLens request for file: " <> T.pack (fromNormalizedFilePath filePath)
          mbMod <- runAction ide (getDalfModule filePath)
          pure $ mapMaybe virtualResourceToCodeLens
              [ (sourceLocToRange loc, "Scenario: " <> name, vr)
              | (valRef, Just loc) <- maybe [] scenariosInModule mbMod
              , let name = LF.unExprValName (LF.qualObject valRef)
              , let vr = VRScenario filePath name
              ]
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
              , Aeson.String $ virtualResourceToUri vr])
    , _xdata = Nothing
    }

setHandlersCodeLens :: PartialHandlers
setHandlersCodeLens = PartialHandlers $ \WithMessage{..} x -> return x{
    LSP.codeLensHandler = withResponse RspCodeLens $ const handle
    }
