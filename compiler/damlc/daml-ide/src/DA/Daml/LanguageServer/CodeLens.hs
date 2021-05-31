-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


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
import qualified Data.Text as T
import Development.IDE.LSP.Server
import qualified Language.Haskell.LSP.Core as LSP
import Language.Haskell.LSP.Messages
import Development.IDE.Core.PositionMapping
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Logger
import Development.IDE.Types.Location

-- | Gather code lenses like scenario execution for a DAML file.
handle
    :: IdeState
    -> CodeLensParams
    -> IO (Either e (List CodeLens))
handle ide (CodeLensParams (TextDocumentIdentifier uri) _) = Right <$> do
    mbResult <- case uriToFilePath' uri of
        Just (toNormalizedFilePath' -> filePath) -> do
          logInfo (ideLogger ide) $ "CodeLens request for file: " <> T.pack (fromNormalizedFilePath filePath)
          (mbModMapping, DamlEnv{..}) <- runAction ide $
              (,) <$> useWithStale GenerateRawDalf filePath
                  <*> getDamlServiceEnv
          case mbModMapping of
              Nothing -> pure []
              Just (mod, mapping) ->
                  pure
                      [ virtualResourceToCodeLens (range, kind, name, vr)
                      | (kind, (valRef, Just loc)) <- map (Scenario,) (scenariosInModule mod) ++ map (Script,) (scriptsInModule envEnableScripts mod)
                      , let name = LF.unExprValName (LF.qualObject valRef)
                      , let vr = VRScenario filePath name
                      , Just range <- [toCurrentRange mapping $ sourceLocToRange loc]
                      ]
        Nothing -> pure []

    pure $ List $ toList mbResult

data Kind = Scenario | Script
  deriving Show

-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Range, Kind, T.Text, VirtualResource)
    -> CodeLens
virtualResourceToCodeLens (range, kind, title, vr) =
 CodeLens
    { _range = range
    , _command = Just $ Command
        (T.pack (show kind) <> " results")
        "daml.showResource"
        (Just $ List
              [ Aeson.String $ T.pack (show kind) <> ": " <> title
              , Aeson.String $ virtualResourceToUri vr])
    , _xdata = Nothing
    }

setHandlersCodeLens :: PartialHandlers a
setHandlersCodeLens = PartialHandlers $ \WithMessage{..} x -> return x{
    LSP.codeLensHandler = withResponse RspCodeLens $ const handle
    }
