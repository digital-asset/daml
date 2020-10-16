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
import qualified "ghc-lib-parser" EnumSet as ES
import qualified "ghc-lib-parser" SrcLoc as GHC
import qualified "ghc-lib-parser" Module as GHC
import qualified "ghc-lib-parser" RdrName as GHC
import qualified "ghc-lib-parser" OccName as GHC
import qualified "ghc-lib-parser" FastString as GHC
import qualified "ghc-lib-parser" GHC.LanguageExtensions.Type as GHC
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
        , "DA.Stack"

        -- These modules are just listed to disable the record preprocessor.
        , "DA.NonEmpty.Types"
        , "DA.Monoid.Types"
        ]

-- | Apply all necessary preprocessors
damlPreprocessor :: ES.EnumSet GHC.Extension -> Maybe GHC.UnitId -> GHC.DynFlags -> GHC.ParsedSource -> IdePreprocessedSource
damlPreprocessor dataDependableExtensions mbUnitId dflags x
    | maybe False (isInternal ||^ (`elem` mayImportInternal)) name = noPreprocessor dflags x
    | otherwise = IdePreprocessedSource
        { preprocWarnings = concat
            [ checkDamlHeader x
            , checkVariantUnitConstructors x
            , checkLanguageExtensions dataDependableExtensions dflags x
            , checkImportsWrtDataDependencies x
            ]
        , preprocErrors = checkImports x ++ checkDataTypes x ++ checkModuleDefinition x ++ checkRecordConstructor x ++ checkModuleName x
        , preprocSource = recordDotPreprocessor $ importDamlPreprocessor $ genericsPreprocessor mbUnitId $ enumTypePreprocessor "GHC.Types" x
        }
    where
      name = fmap GHC.unLoc $ GHC.hsmodName $ GHC.unLoc x

-- | Preprocessor for generated code.
generatedPreprocessor :: GHC.DynFlags -> GHC.ParsedSource -> IdePreprocessedSource
generatedPreprocessor _dflags x =
    IdePreprocessedSource
      { preprocWarnings = []
      , preprocErrors = []
      , preprocSource = enumTypePreprocessor "CurrentSdk.GHC.Types" x
      }

-- | No preprocessing.
noPreprocessor :: GHC.DynFlags -> GHC.ParsedSource -> IdePreprocessedSource
noPreprocessor _dflags x =
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

-- | Emit a warning if the "daml 1.2" version header is present.
checkDamlHeader :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkDamlHeader (GHC.L _ m)
    | Just (GHC.L ss doc) <- GHC.hsmodHaddockModHeader m
    , "HAS_DAML_VERSION_HEADER" `isPrefixOf` GHC.unpackHDS doc
    = [(ss, "The \"daml 1.2\" version header is deprecated, please remove it.")]

    | otherwise
    = []

-- | Emit a warning if a variant constructor has a single argument of unit type '()'.
-- See issue #7207.
checkVariantUnitConstructors :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkVariantUnitConstructors (GHC.L _ m) =
    [ let tyNameStr = GHC.occNameString (GHC.rdrNameOcc (GHC.unLoc ltyName))
          conNameStr = GHC.occNameString (GHC.rdrNameOcc conName)
      in (ss, message tyNameStr conNameStr)
    | GHC.L ss (GHC.TyClD _ GHC.DataDecl{tcdLName=ltyName, tcdDataDefn=dataDefn}) <- GHC.hsmodDecls m
    , GHC.HsDataDefn{dd_cons=cons} <- [dataDefn]
    , length cons > 1
    , GHC.L _ con <- cons
    , GHC.PrefixCon [GHC.L _ (GHC.HsTupleTy _ _ [])] <- [GHC.con_args con]
    , GHC.L _ conName <- GHC.getConNames con
    ]
  where
    message tyNameStr conNameStr = unwords
      [ "Variant type", tyNameStr, "constructor", conNameStr
      , "has a single argument of type (). The argument will not be"
      , "preserved when importing this package via data-dependencies."
      , "Possible solution: Remove the constructor's argument." ]

-- | Emit an error if a record constructor name does not match the record type name.
-- See issue #4718.
checkRecordConstructor :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkRecordConstructor (GHC.L _ m) = mapMaybe getRecordError (GHC.hsmodDecls m)
  where
    getRecordError :: GHC.LHsDecl GHC.GhcPs -> Maybe (GHC.SrcSpan, String)
    getRecordError (GHC.L ss decl)
        | GHC.TyClD _ GHC.DataDecl{tcdLName=ltyName, tcdDataDefn=dataDefn} <- decl
        , GHC.HsDataDefn{dd_ND=nd, dd_cons=[con]} <- dataDefn
        , isNewType nd || isRecCon (GHC.con_args (GHC.unLoc con))
        , GHC.L _ tyName <- ltyName
        , [GHC.L _ conName] <- GHC.getConNames (GHC.unLoc con)
        , let tyNameStr = GHC.occNameString (GHC.rdrNameOcc tyName)
        , let conNameStr = GHC.occNameString (GHC.rdrNameOcc conName)
        , tyNameStr /= conNameStr
        = Just (ss, message nd tyNameStr conNameStr)

        | otherwise
        = Nothing

    isNewType :: GHC.NewOrData -> Bool
    isNewType = (GHC.NewType ==)

    isRecCon :: GHC.HsConDeclDetails GHC.GhcPs -> Bool
    isRecCon = \case
        GHC.RecCon{} -> True
        _ -> False

    message :: GHC.NewOrData -> String -> String -> String
    message nd tyNameStr conNameStr = unwords
        [ if isNewType nd then "Newtype" else "Record type"
        , tyNameStr, "has constructor", conNameStr
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

checkLanguageExtensions :: ES.EnumSet GHC.Extension -> GHC.DynFlags -> GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkLanguageExtensions dataDependableExtensions dflags x =
    let exts = ES.toList (GHC.extensionFlags dflags)
        badExts = filter (\ext -> not (ext `ES.member` dataDependableExtensions)) exts
    in
    [ (modNameLoc, warning ext) | ext <- badExts ]
  where
    warning ext = unlines
        [ "Modules compiled with the " ++ show ext ++ " language extension"
        , "might not work properly with data-dependencies. This might stop the"
        , "whole package from being extensible or upgradable using other versions"
        , "of the SDK. Use this language extension at your own risk."
        ]
    -- NOTE(MH): Neither the `DynFlags` nor the `ParsedSource` contain
    -- information about where a `{-# LANGUAGE ... #-}` pragma has been used.
    -- In fact, there might not even be such a pragma if a `-X...` flag has been
    -- used on the command line. Thus, we always put the warning at the location
    -- of the module name.
    modNameLoc = maybe GHC.noSrcSpan GHC.getLoc (GHC.hsmodName (GHC.unLoc x))

checkImportsWrtDataDependencies :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkImportsWrtDataDependencies x =
    [ (loc, warning)
    | GHC.L loc GHC.ImportDecl{ideclName = GHC.L _ m} <- GHC.hsmodImports $ GHC.unLoc x
    , GHC.moduleNameString m == "DA.Generics"
    ]
  where
    warning = unlines
        [ "Modules importing DA.Generics do not work with data-dependencies."
        , "This will prevent the whole package from being extensible or upgradable"
        , "using other versions of the SDK. Use DA.Generics at your own risk."
        ]

-- Extract all data constructors with their locations
universeConDecl :: GHC.ParsedSource -> [GHC.LConDecl GHC.GhcPs]
-- equivalent to universeBi, but specialised to be faster
universeConDecl m = concat
    [ dd_cons
    | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons}} <- map GHC.unLoc $ GHC.hsmodDecls $ GHC.unLoc m
    ]
