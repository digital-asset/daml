-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Preprocessor
  ( damlPreprocessor
  , generatedPreprocessor
  , noPreprocessor
  ) where

import           DA.Daml.Preprocessor.Records
import           DA.Daml.Preprocessor.Generics
import           DA.Daml.Preprocessor.EnumType

import Development.IDE.Types.Options
import qualified "ghc-lib" GHC
import qualified "ghc-lib-parser" SrcLoc as GHC
import qualified "ghc-lib-parser" Module as GHC
import qualified "ghc-lib-parser" RdrName as GHC
import qualified "ghc-lib-parser" OccName as GHC
import qualified "ghc-lib-parser" FastString as GHC
import Outputable

import           Control.Monad.Extra
import           Data.List
import           Data.Maybe
import           System.FilePath (splitDirectories)


isInternal :: GHC.ModuleName -> Bool
isInternal (GHC.moduleNameString -> x)
  = "DA.Internal." `isPrefixOf` x ||
    "GHC." `isPrefixOf` x ||
    x `elem`
      [ "Control.Exception.Base"
      , "Data.String"
      , "LibraryModules"
      , "DA.Types"
      , "DA.Time.Types"
      ]

mayImportInternal :: [GHC.ModuleName]
mayImportInternal =
    map GHC.mkModuleName
        [ "Prelude"
        , "DA.Time"
        , "DA.Date"
        , "DA.Record"
        , "DA.TextMap"
        , "DA.Map"
        , "DA.Generics"
        , "DA.Text"
        , "DA.Numeric"

        -- These modules are just listed to disable the record preprocessor.
        , "DA.NonEmpty.Types"
        , "DA.Monoid.Types"
        ]

-- | Apply all necessary preprocessors
damlPreprocessor :: Maybe GHC.UnitId -> GHC.ParsedSource -> IdePreprocessedSource
damlPreprocessor mbUnitId x
    | maybe False (isInternal ||^ (`elem` mayImportInternal)) name = noPreprocessor x
    | otherwise = IdePreprocessedSource
        { preprocWarnings = []
        , preprocErrors = checkImports x ++ checkDataTypes x ++ checkModuleDefinition x ++ checkRecordConstructor x ++ checkModuleName x
        , preprocSource = recordDotPreprocessor $ importDamlPreprocessor $ genericsPreprocessor mbUnitId $ enumTypePreprocessor "GHC.Types" x
        }
    where
      name = fmap GHC.unLoc $ GHC.hsmodName $ GHC.unLoc x

-- | Preprocessor for generated code.
generatedPreprocessor :: GHC.ParsedSource -> IdePreprocessedSource
generatedPreprocessor x =
    IdePreprocessedSource
      { preprocWarnings = []
      , preprocErrors = []
      , preprocSource = enumTypePreprocessor "CurrentSdk.GHC.Types" x
      }

-- | No preprocessing.
noPreprocessor :: GHC.ParsedSource -> IdePreprocessedSource
noPreprocessor x =
    IdePreprocessedSource
      { preprocWarnings = []
      , preprocErrors = []
      , preprocSource = x
      }

-- With RebindableSyntax any missing DAML import results in pretty much nothing
-- working (literals, if-then-else) so we inject an implicit import DAML for
-- peoples convenience
importDamlPreprocessor :: GHC.ParsedSource -> GHC.ParsedSource
importDamlPreprocessor = fmap onModule
    where
        onModule y = y {
          GHC.hsmodImports =
            newImport True "GHC.Types" :
            newImport True "DA.Internal.Desugar" :
            newImport False "DA.Internal.RebindableSyntax" : GHC.hsmodImports y
          }
        newImport :: Bool -> String -> GHC.Located (GHC.ImportDecl GHC.GhcPs)
        newImport qual = GHC.noLoc . importGenerated qual . mkImport . GHC.noLoc . GHC.mkModuleName

checkModuleName :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkModuleName (GHC.L _ m)
    | Just (GHC.L nameLoc modName) <- GHC.hsmodName m
    , expected <- GHC.moduleNameSlashes modName ++ ".daml"
    , Just actual <- GHC.unpackFS <$> GHC.srcSpanFileName_maybe nameLoc
    , not (splitDirectories expected `isSuffixOf` splitDirectories actual)
    = [(nameLoc, "Module names should always match file names, as per [documentation|https://docs.daml.com/daml/reference/file-structure.html]. Please change the filename to " ++ expected ++ ".")]

    | otherwise
    = []

-- | We ban people from importing modules such
checkImports :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkImports x =
    [ (ss, "Import of internal module " ++ GHC.moduleNameString m ++ " is not allowed.")
    | GHC.L ss GHC.ImportDecl{ideclName=GHC.L _ m} <- GHC.hsmodImports $ GHC.unLoc x, isInternal m]

-- | Emit a warning if a record constructor name does not match the record type name.
-- See issue #4718. This ought to be moved into 'checkDataTypes' before too long.
checkRecordConstructor :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkRecordConstructor (GHC.L _ m) = mapMaybe getRecordError (GHC.hsmodDecls m)
  where
    getRecordError :: GHC.LHsDecl GHC.GhcPs -> Maybe (GHC.SrcSpan, String)
    getRecordError (GHC.L ss decl)
        | GHC.TyClD _ GHC.DataDecl{tcdLName=ltyName, tcdDataDefn=dataDefn} <- decl
        , GHC.HsDataDefn{dd_cons=[con]} <- dataDefn
        , GHC.RecCon{} <- GHC.con_args (GHC.unLoc con)
        , GHC.L _ tyName <- ltyName
        , [GHC.L _ conName] <- GHC.getConNames (GHC.unLoc con)
        , let tyNameStr = GHC.occNameString (GHC.rdrNameOcc tyName)
        , let conNameStr = GHC.occNameString (GHC.rdrNameOcc conName)
        , tyNameStr /= conNameStr
        = Just (ss, message tyNameStr conNameStr)

        | otherwise
        = Nothing

    message tyNameStr conNameStr = unwords
        [ "Record type", tyNameStr, "has constructor", conNameStr
        , "with different name."
        , "Possible solution: Change the constructor name to", tyNameStr ]

checkDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkDataTypes m = checkAmbiguousDataTypes m ++ checkUnlabelledConArgs m ++ checkThetas m

checkAmbiguousDataTypes :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkAmbiguousDataTypes (GHC.L _ m) =
    mapMaybe getAmbiguousError (GHC.hsmodDecls m)
  where
    getAmbiguousError :: GHC.LHsDecl GHC.GhcPs -> Maybe (GHC.SrcSpan, String)
    -- Generate an error if the declaration is a data type with one constructor and zero arguments
    getAmbiguousError (GHC.L ss decl)
      | GHC.TyClD _ GHC.DataDecl{tcdTyVars = dtyvars, tcdDataDefn=GHC.HsDataDefn{dd_cons=[con]}} <- decl -- single con data type
      , GHC.HsQTvs _ (_:_) <- dtyvars -- with at least one type-level arguments
      , GHC.PrefixCon [] <- GHC.con_args (GHC.unLoc con) -- but zero value arguments
      = Just (ss, message)
      | otherwise
      = Nothing
      where
        message =
          "Ambiguous data type declaration. Enums cannot have type arguments. Write " ++
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

-- | Check for the presence of the 'module ... where' clause.
checkModuleDefinition :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkModuleDefinition x
    | Nothing <- GHC.hsmodName $ GHC.unLoc x =
        [ ( GHC.noSrcSpan
          , "Missing module name, e.g. 'module ... where'.")
        ]
    | otherwise = []

-- Extract all data constructors with their locations
universeConDecl :: GHC.ParsedSource -> [GHC.LConDecl GHC.GhcPs]
-- equivalent to universeBi, but specialised to be faster
universeConDecl m = concat
    [ dd_cons
    | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons}} <- map GHC.unLoc $ GHC.hsmodDecls $ GHC.unLoc m
    ]
