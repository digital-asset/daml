-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE PatternSynonyms #-}
module Development.IDE.Types.LSP
    ( VirtualResource(..)
    , pattern EventFileDiagnostics
    ) where

import Control.DeepSeq
import qualified Data.Text as T
import Development.IDE.Types.Diagnostics
import GHC.Generics
import Language.Haskell.LSP.Messages
import Language.Haskell.LSP.Types

-- | Virtual resources
data VirtualResource = VRScenario
    { vrScenarioFile :: !NormalizedFilePath
    , vrScenarioName :: !T.Text
    } deriving (Eq, Ord, Show, Generic)
    -- ^ VRScenario identifies a scenario in a given file.
    -- This virtual resource is associated with the HTML result of
    -- interpreting the corresponding scenario.

instance NFData VirtualResource

-- | Pattern synonym to make it a bit more convenient to match on diagnostics
-- in things like damlc test.
pattern EventFileDiagnostics :: FilePath -> [Diagnostic] -> FromServerMessage
pattern EventFileDiagnostics fp diags <-
    NotPublishDiagnostics
        (NotificationMessage _ _ (PublishDiagnosticsParams (uriToFilePath' -> Just fp) (List diags)))
