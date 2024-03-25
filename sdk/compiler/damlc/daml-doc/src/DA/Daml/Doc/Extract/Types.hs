-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Extract.Types
    ( ExtractOptions (..)
    , defaultExtractOptions
    , QualifyTypes (..)
    , DocCtx (..)
    , DeclData (..)
    , ExportSet (..)
    , ExportedItem (..)
    ) where

import DA.Daml.Doc.Types

import qualified Data.Map.Strict as MS
import qualified Data.Set as Set
import "ghc-lib" GHC

-- | Options that affect doc extraction.
data ExtractOptions = ExtractOptions
    { eo_qualifyTypes :: QualifyTypes
        -- ^ qualify non-local types
    , eo_simplifyQualifiedTypes :: Bool
        -- ^ drop common module prefix when qualifying types
    } deriving (Eq, Show, Read)

data QualifyTypes
    = QualifyTypesAlways
    | QualifyTypesInPackage
    | QualifyTypesNever
    deriving (Eq, Show, Read)

-- | Default options for doc extraction.
defaultExtractOptions :: ExtractOptions
defaultExtractOptions = ExtractOptions
    { eo_qualifyTypes = QualifyTypesNever
    , eo_simplifyQualifiedTypes = False
    }

-- | Context in which to extract a module's docs. This is created from
-- 'TypecheckedModule' by 'buildDocCtx'.
data DocCtx = DocCtx
    { dc_ghcMod :: GHC.Module
        -- ^ ghc name for current module
    , dc_modname :: Modulename
        -- ^ name of the current module
    , dc_decls :: [DeclData]
        -- ^ module declarations
    , dc_insts :: [ClsInst]
        -- ^ typeclass instances
    , dc_tycons :: MS.Map Typename TyCon
        -- ^ types defined in this module
    , dc_datacons :: MS.Map Typename DataCon
        -- ^ constructors defined in this module
    , dc_ids :: MS.Map Fieldname Id
        -- ^ values defined in this module
    , dc_templates :: Set.Set Typename
        -- ^ Daml templates defined in this module
    , dc_choices :: MS.Map Typename (Set.Set Typename)
        -- ^ choices per Daml template/interface defined in this module
    , dc_extractOptions :: ExtractOptions
        -- ^ command line options that affect the doc extractor
    , dc_exports :: ExportSet
        -- ^ set of export, unless everything is exported
    , dc_interfaces :: Set.Set Typename
        -- ^ interfaces defined in this module
    }

-- | Parsed declaration with associated docs.
data DeclData = DeclData
    { _dd_decl :: LHsDecl GhcPs
    , _dd_docs :: Maybe DocText
    }


-- | Set of module exports.
--
-- Unlike Haddock, we don't ask the export list to dictate the order
-- of docs; damldocs imposes its own order. So we can treat the export
-- list as a set instead.
data ExportSet
    = ExportEverything
    | ExportOnly !(Set.Set ExportedItem)

-- | Particular exported item. We don't particularly care
-- about re-exported modules for now, but we want to know
-- if a module re-exports itself so we have a way to track
-- it here.
data ExportedItem
    = ExportedType !Typename
        -- ^ type is exported
    | ExportedTypeAll !Typename
        -- ^ all constructors and fields for a type are exported
    | ExportedConstr !Typename
        -- ^ constructor is exported
    | ExportedFunction !Fieldname
        -- ^ function or field is exported
    | ExportedModule !GHC.ModuleName
        -- ^ module is reexported
    deriving (Eq, Ord)

