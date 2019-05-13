-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.GHC.Compiler.Preprocessor
  ( damlPreprocessor
  , damlPreprocessorImports
  ) where

import           DA.Daml.GHC.Compiler.Records

import DA.Daml.GHC.Compiler.UtilGHC

import qualified "ghc-lib" GHC
import Outputable

import           Control.Monad.Extra
import           Data.List
import           Data.Maybe


isInternal :: GHC.ModuleName -> Bool
isInternal (GHC.moduleNameString -> x)
  = "DA.Internal." `isPrefixOf` x ||
    "GHC." `isPrefixOf` x ||
    x `elem` ["Control.Exception.Base", "Data.String", "LibraryModules"]

mayImportInternal :: [GHC.ModuleName]
mayImportInternal = map GHC.mkModuleName ["Prelude", "DA.Time", "DA.Date", "DA.Record", "DA.TextMap"]

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
            newImport GHC.QualifiedPost "DA.Internal.Desugar" :
            newImport GHC.NotQualified "DA.Internal.RebindableSyntax" : GHC.hsmodImports y
          }
        newImport :: GHC.ImportDeclQualifiedStyle -> String -> GHC.Located (GHC.ImportDecl GHC.GhcPs)
        newImport qual = GHC.noLoc . importGenerated qual . mkImport . GHC.noLoc . GHC.mkModuleName

-- | We ban people from importing modules such
checkImports :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkImports x =
    [ (ss, "Import of internal module " ++ GHC.moduleNameString m ++ " is not allowed.")
    | GHC.L ss GHC.ImportDecl{ideclName=GHC.L _ m} <- GHC.hsmodImports $ GHC.unLoc x, isInternal m]


checkDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkDataTypes m = checkAmbiguousDataTypes m ++ checkUnlabelledConArgs m ++ checkThetas m


checkAmbiguousDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkAmbiguousDataTypes (GHC.L _ m) =
    mapMaybe getAmbiguousError (GHC.hsmodDecls m)
  where
    getAmbiguousError :: GHC.LHsDecl GHC.GhcPs -> Maybe (GHC.SrcSpan, String)
    -- Generate an error if the declaration is a data type with one constructor and zero arguments
    getAmbiguousError (GHC.L ss decl)
      | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons=[con]}} <- decl -- single con data type
      , GHC.PrefixCon [] <- GHC.con_args (GHC.unLoc con) -- zero arguments
      = Just (ss, message)
      | otherwise
      = Nothing
      where
        message =
          "Ambiguous data type declaration. Write " ++
          baseDeclStr ++ " {} for a record or " ++
          baseDeclStr ++ " () for a variant."
        baseDeclStr = showSDocUnsafe (ppr decl)


checkUnlabelledConArgs :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkUnlabelledConArgs m =
    [ (ss, "Constructors with multiple fields must give explicit field names, e.g. Foo with bar : Int; baz : Int")
    | GHC.L ss con <- universeConDecl m, isBad $ GHC.con_args con]
    where
        isBad :: GHC.HsConDeclDetails GHC.GhcPs -> Bool
        isBad (GHC.PrefixCon xs) = length xs > 1
        isBad GHC.InfixCon{} = True
        isBad GHC.RecCon{} = False

-- | We only allow non-empty thetas (i.e. constraints) in data types with named fields.
checkThetas :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkThetas m =
    [ ( ss
      , "Constructors with type constraints must give explicit field names, e.g. data Foo = Show a => Foo {bar : a}")
    | GHC.L ss con <- universeConDecl m
    , isJust $ GHC.con_mb_cxt con
    , isBad $ GHC.con_args con
    ]
  where
    isBad :: GHC.HsConDeclDetails GHC.GhcPs -> Bool
    isBad (GHC.PrefixCon _) = True
    isBad GHC.InfixCon {} = True
    isBad GHC.RecCon {} = False

-- Extract all data constructors with their locations
universeConDecl :: GHC.ParsedSource -> [GHC.LConDecl GHC.GhcPs]
-- equivalent to universeBi, but specialised to be faster
universeConDecl m = concat
    [ dd_cons
    | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons}} <- map GHC.unLoc $ GHC.hsmodDecls $ GHC.unLoc m
    ]
