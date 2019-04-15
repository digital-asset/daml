-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.GHC.Compiler.Preprocessor
  ( damlPreprocessor
  , damlPreprocessorImports
  ) where

import           DA.Daml.GHC.Compiler.Records

import Development.IDE.UtilGHC

import qualified "ghc-lib" GHC

import           Control.Monad.Extra
import           Data.List


isInternal :: GHC.ModuleName -> Bool
isInternal (GHC.moduleNameString -> x)
  = "DA.Internal." `isPrefixOf` x ||
    "GHC." `isPrefixOf` x ||
    x `elem` ["Control.Exception.Base", "Data.String", "LibraryModules"]

mayImportInternal :: [GHC.ModuleName]
mayImportInternal = map GHC.mkModuleName ["Prelude", "DA.Time", "DA.Date", "DA.Record", "DA.Private.TextMap"]

-- | Apply all necessary preprocessors
damlPreprocessor :: GHC.ParsedSource -> ([(GHC.SrcSpan, String)], GHC.ParsedSource)
damlPreprocessor x
    | maybe False (isInternal ||^ (`elem` mayImportInternal)) name = ([], x)
    | otherwise = (checkImports x ++ checkDataTypes x, recordDotPreprocessor $ importDamlPreprocessor x)
    where name = fmap GHC.unLoc $ GHC.hsmodName $ GHC.unLoc x


-- | Imports that will be added by the preprocessor
damlPreprocessorImports :: GHC.ModuleName -> [GHC.ModuleName]
-- FIXME: This function should be deleted, since people should calculate imports from the parse tree
damlPreprocessorImports name
    | isInternal name = []
    | otherwise =
      map GHC.mkModuleName [
         "DA.Internal.Record"
       , "DA.Internal.RebindableSyntax"
       , "DA.Internal.Desugar"]

-- With RebindableSyntax any missing DAML import results in pretty much nothing
-- working (literals, if-then-else) so we inject an implicit import DAML for
-- peoples convenience
importDamlPreprocessor :: GHC.ParsedSource -> GHC.ParsedSource
importDamlPreprocessor = fmap onModule
    where
        onModule y = y {
          GHC.hsmodImports =
            newImport True "DA.Internal.Desugar" :
            newImport False "DA.Internal.RebindableSyntax" : GHC.hsmodImports y
          }
        newImport :: Bool -> String -> GHC.Located (GHC.ImportDecl GHC.GhcPs)
        newImport qual = GHC.noLoc . importGenerated qual . mkImport . GHC.noLoc . GHC.mkModuleName

-- | We ban people from importing modules such
checkImports :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkImports x =
    [ (ss, "Import of internal module " ++ GHC.moduleNameString m ++ " is not allowed.")
    | GHC.L ss GHC.ImportDecl{ideclName=GHC.L _ m} <- GHC.hsmodImports $ GHC.unLoc x, isInternal m]


checkDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkDataTypes m = checkAmbiguousDataTypes m ++ checkUnlabelledConArgs m


checkAmbiguousDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkAmbiguousDataTypes m =
    [ (ss, "Ambiguous data type. Please disambiguate, e.g. data Foo = Foo {} for a record type or data Foo = Foo () for a variant type.")
    | GHC.L ss decl <- GHC.hsmodDecls (GHC.unLoc m), isBad decl ]
    where
        isBad :: GHC.HsDecl GHC.GhcPs -> Bool
        -- Is the declaration a data type with one constructor and zero arguments?
        isBad decl
          | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons=[con]}} <- decl -- single con data type
          , GHC.PrefixCon [] <- GHC.con_args (GHC.unLoc con) -- zero arguments
          = True
        isBad _ = False


checkUnlabelledConArgs :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkUnlabelledConArgs m =
    [ (ss, "Constructors with multiple fields must give explicit field names, e.g. Foo with bar : Int; baz : Int")
    | GHC.L ss con <- universeConDecl m, isBad $ GHC.con_args con]
    where
        isBad :: GHC.HsConDeclDetails GHC.GhcPs -> Bool
        isBad (GHC.PrefixCon xs) = length xs > 1
        isBad GHC.InfixCon{} = True
        isBad GHC.RecCon{} = False


-- Extract all data constructors with their locations
universeConDecl :: GHC.ParsedSource -> [GHC.LConDecl GHC.GhcPs]
-- equivalent to universeBi, but specialised to be faster
universeConDecl m = concat
    [ dd_cons
    | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons}} <- map GHC.unLoc $ GHC.hsmodDecls $ GHC.unLoc m
    ]
