-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings  #-}

-- | Defaults and conversions between Ast types and protocol types
module DA.Service.Daml.LanguageServer.Common
    ( -- Defaults
      damlLanguageIdentifier

    , virtualResourceToCodeLens
    ) where

import DA.LanguageServer.Protocol hiding (CodeLens)
import Language.Haskell.LSP.Types (CodeLens(..))

import qualified Data.Aeson                       as Aeson
import qualified Data.Text as T

import qualified DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified Development.IDE.Types.Diagnostics as Base
import qualified Development.IDE.Types.LSP as Compiler

-- | DAML-Language identifier. Used to tell VS Code to highlight
--  certain things as DAML Code.
damlLanguageIdentifier :: T.Text
damlLanguageIdentifier = "daml"


-- | Convert a compiler virtual resource into a code lens.
virtualResourceToCodeLens
    :: (Base.Range, T.Text, Compiler.VirtualResource)
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
