-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Preprocessor
  ( damlPreprocessor
  , generatedPreprocessor
  , noPreprocessor
  , isInternal
  ) where

import qualified DA.Daml.LF.Ast as LF (PackageName (..))
import           DA.Daml.Preprocessor.Records
import           DA.Daml.Preprocessor.EnumType
import           DA.Daml.StablePackages (stablePackageByModuleName)
import           DA.Daml.UtilGHC (convertModuleName)

import Development.IDE.Types.Options
import qualified "ghc-lib" GHC
import qualified "ghc-lib-parser" Bag as GHC
import qualified "ghc-lib-parser" EnumSet as ES
import qualified "ghc-lib-parser" SrcLoc as GHC
import qualified "ghc-lib-parser" Module as GHC
import qualified "ghc-lib-parser" RdrName as GHC
import qualified "ghc-lib-parser" OccName as GHC
import qualified "ghc-lib-parser" FastString as GHC
import qualified "ghc-lib-parser" GHC.LanguageExtensions.Type as GHC
import Outputable

import           Data.List.NonEmpty (NonEmpty ((:|)))
import qualified Data.List.NonEmpty as NE
import           Data.List.Extra
import           Data.Maybe
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import           System.FilePath (splitDirectories)

import qualified Data.Generics.Uniplate.Data as Uniplate


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

isUnstableInternal :: GHC.ModuleName -> Bool
isUnstableInternal moduleName =
  isInternal moduleName &&
    convertModuleName moduleName `Map.notMember` stablePackageByModuleName

preprocessorExceptions :: Set.Set GHC.ModuleName
preprocessorExceptions = Set.fromList $ map GHC.mkModuleName
    -- These modules need to import internal modules.
    [ "Prelude"
    , "DA.Time"
    , "DA.Date"
    , "DA.Record"
    , "DA.TextMap"
    , "DA.Map"
    , "DA.Text"
    , "DA.Numeric"
    , "DA.Stack"
    , "DA.BigNumeric"
    , "DA.Exception"
    , "DA.Exception.GeneralError"
    , "DA.Exception.ArithmeticError"
    , "DA.Exception.AssertionFailed"
    , "DA.Exception.PreconditionFailed"

    -- These modules need to have the record preprocessor disabled.
    , "DA.NonEmpty.Types"
    , "DA.Monoid.Types"
    , "DA.Set.Types"

    -- This module needs to use the PatternSynonyms extension.
    , "DA.Maybe"
    ]

isExperimental :: GHC.ModuleName -> Bool
isExperimental (GHC.moduleNameString -> x)
  -- Experimental modules need to import internal modules.
  = "DA.Experimental." `isPrefixOf` x

shouldSkipPreprocessor :: (LF.PackageName, GHC.ModuleName) -> Bool
shouldSkipPreprocessor (pkgName, modName) =
    pkgName `elem` fmap LF.PackageName ["daml-prim", "daml-stdlib"]
      &&
        (isInternal modName
        || Set.member modName preprocessorExceptions
        || isExperimental modName)

-- | Apply all necessary preprocessors
damlPreprocessor :: ES.EnumSet GHC.Extension -> Maybe LF.PackageName -> GHC.DynFlags -> GHC.ParsedSource -> IdePreprocessedSource
damlPreprocessor dataDependableExtensions mPkgName dflags x
    | maybe False shouldSkipPreprocessor mod = noPreprocessor dflags x
    | otherwise = IdePreprocessedSource
        { preprocWarnings = concat
            [ checkDamlHeader x
            , checkVariantUnitConstructors x
            , checkLanguageExtensions dataDependableExtensions dflags x
            , checkKinds x
            ]
        , preprocErrors = concat
            [ checkImports x
            , checkDataTypes x
            , checkModuleDefinition x
            , checkRecordConstructor x
            , checkModuleName x
            ]
        , preprocSource =
            rewriteLets
            . recordDotPreprocessor
            . importDamlPreprocessor
            $ enumTypePreprocessor "GHC.Types" x
        }
    where
      mod =
        (,)
          <$> mPkgName
          <*> fmap GHC.unLoc (GHC.hsmodName $ GHC.unLoc x)

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

-- With RebindableSyntax any missing Daml import results in pretty much nothing
-- working (literals, if-then-else) so we inject an implicit import Daml for
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
    | GHC.L ss GHC.ImportDecl{ideclName=GHC.L _ m} <- GHC.hsmodImports $ GHC.unLoc x, isUnstableInternal m]

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
checkDataTypes m = checkAmbiguousDataTypes m ++ checkUnlabelledConArgs m ++ checkThetas m ++ checkDuplicateRecordFields m

checkDuplicateRecordFields :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkDuplicateRecordFields m =
    [ err
    | GHC.L _ con <- universeConDecl m
    , err <- conErrs (GHC.getConArgs con)
    ]
  where
      conErrs :: GHC.HsConDeclDetails GHC.GhcPs -> [(GHC.SrcSpan, String)]
      conErrs (GHC.RecCon (GHC.L _ fields)) =
          let names = map fieldName fields
              grouped = Map.fromListWith (++) [(GHC.unLoc n, [n]) | n <- names]
          in concatMap errors (Map.elems grouped)
      conErrs _ = []

      -- The first field name is the one the user wrote which we want here. Later (there should only ever be 2)
      -- field names are autogenerated and don’t matter here.
      fieldName :: GHC.LConDeclField GHC.GhcPs -> GHC.Located GHC.RdrName
      fieldName (GHC.L _ GHC.ConDeclField { cd_fld_names = (n : _) }) = GHC.rdrNameFieldOcc (GHC.unLoc n)
      fieldName (GHC.L _ GHC.ConDeclField { cd_fld_names = [] }) =
          error "Internal error: cd_fld_names should contain exactly one name but got an empty list"
      fieldName (GHC.L _ (GHC.XConDeclField GHC.NoExt)) = error "Internal error: unexpected XConDeclField"
      fieldError :: GHC.SrcSpan -> GHC.Located GHC.RdrName -> (GHC.SrcSpan, String)
      fieldError def (GHC.L l n) =
          ( l
          , unwords
            [ "Duplicate field name " ++ showSDocUnsafe (ppr n) ++ "."
            , "Original definition at " ++ showSDocUnsafe (ppr def) ++ "."
            ]
          )
      -- the list should always be non-empty but easy enough to handle this case without crashing
      errors xs = case sortOn GHC.getLoc xs of
        [] -> []
        (hd : tl) -> map (fieldError (GHC.getLoc hd)) tl


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

-- Extract all data constructors with their locations
universeConDecl :: GHC.ParsedSource -> [GHC.LConDecl GHC.GhcPs]
-- equivalent to universeBi, but specialised to be faster
universeConDecl m = concat
    [ dd_cons
    | GHC.TyClD _ GHC.DataDecl{tcdDataDefn=GHC.HsDataDefn{dd_cons}} <- map GHC.unLoc $ GHC.hsmodDecls $ GHC.unLoc m
    ]

-- | Emit a warning if GHC.Types.Symbol was used in a top-level kind signature.
checkKinds :: GHC.ParsedSource -> [(GHC.SrcSpan, String)]
checkKinds (GHC.L _ m) = do
    GHC.L _ decl <- GHC.hsmodDecls m
    checkDeclKinds decl
  where
    checkDeclKinds :: GHC.HsDecl GHC.GhcPs -> [(GHC.SrcSpan, String)]
    checkDeclKinds = \case
        GHC.TyClD _ GHC.SynDecl{..} -> checkQTyVars tcdTyVars
        GHC.TyClD _ GHC.DataDecl{..} -> checkQTyVars tcdTyVars
        GHC.TyClD _ GHC.ClassDecl{..} -> checkQTyVars tcdTyVars
        _ -> []

    checkQTyVars :: GHC.LHsQTyVars GHC.GhcPs -> [(GHC.SrcSpan, String)]
    checkQTyVars = \case
        GHC.HsQTvs{..} -> concatMap checkTyVarBndr hsq_explicit
        _ -> []

    checkTyVarBndr :: GHC.LHsTyVarBndr GHC.GhcPs -> [(GHC.SrcSpan, String)]
    checkTyVarBndr (GHC.L _ var) = case var of
        GHC.KindedTyVar _ _ kind -> checkKind kind
        _ -> []

    checkKind :: GHC.LHsKind GHC.GhcPs -> [(GHC.SrcSpan, String)]
    checkKind (GHC.L _ kind) = case kind of
        GHC.HsTyVar _ _ v -> checkRdrName v
        GHC.HsFunTy _ k1 k2 -> checkKind k1 ++ checkKind k2
        GHC.HsParTy _ k -> checkKind k
        _ -> []

    checkRdrName :: GHC.Located GHC.RdrName -> [(GHC.SrcSpan, String)]
    checkRdrName (GHC.L loc name) = case name of
        GHC.Unqual (GHC.occNameString -> "Symbol") ->
            [(loc, symbolKindMsg)]
        GHC.Qual (GHC.moduleNameString -> "GHC.Types") (GHC.occNameString -> "Symbol") ->
            [(loc, ghcTypesSymbolMsg)]
        _ -> []

    symbolKindMsg :: String
    symbolKindMsg = unlines
        [ "Reference to Symbol kind will not be preserved during Daml compilation."
        , "This will cause problems when importing this module via data-dependencies."
        ]

    ghcTypesSymbolMsg :: String
    ghcTypesSymbolMsg = unlines
        [ "Reference to GHC.Types.Symbol will not be preserved during Daml compilation."
        , "This will cause problems when importing this module via data-dependencies."
        ]

-- | Issue #6788: Rewrite multi-binding let expressions,
--
--      let x1 = e1
--          x2 = e2
--          ...
--          xn = en
--      in b
--
-- Into a chain of single-binding let expressions,
--
--      let x1 = e1 in
--        let x2 = e2 in
--          ...
--            let xn = en in
--              b
--
-- Likewise, rewrite multi-binding let statements in a "do" block,
--
--      do
--          ...
--          let x1 = e1
--              x2 = e2
--              ...
--              xn = en
--          ...
--
-- Into single-binding let statements,
--
--      do
--          ...
--          let x1 = e1
--          let x2 = e2
--          ...
--          let xn = en
--          ...
--
rewriteLets :: GHC.ParsedSource -> GHC.ParsedSource
rewriteLets = fmap onModule
  where
    onModule :: GHC.HsModule GHC.GhcPs -> GHC.HsModule GHC.GhcPs
    onModule x = x { GHC.hsmodDecls = map onDecl $ GHC.hsmodDecls x }

    onDecl :: GHC.LHsDecl GHC.GhcPs -> GHC.LHsDecl GHC.GhcPs
    onDecl = fmap (Uniplate.descendBi onExpr)

    onExpr :: GHC.LHsExpr GHC.GhcPs -> GHC.LHsExpr GHC.GhcPs
    onExpr = Uniplate.transform $ \case
        GHC.L loc (GHC.HsLet GHC.NoExt letBinds letBody) ->
            foldr (\bind body -> GHC.L loc (GHC.HsLet GHC.NoExt bind body))
                letBody
                (sequenceLocalBinds letBinds)

        GHC.L loc1 (GHC.HsDo GHC.NoExt doContext (GHC.L loc2 doStmts)) ->
            GHC.L loc1 (GHC.HsDo GHC.NoExt doContext
                (GHC.L loc2 (concatMap onStmt doStmts)))

        x -> x

    onStmt :: GHC.ExprLStmt GHC.GhcPs -> [GHC.ExprLStmt GHC.GhcPs]
    onStmt = \case
        GHC.L loc (GHC.LetStmt GHC.NoExt letBinds) ->
            map (GHC.L loc . GHC.LetStmt GHC.NoExt) (sequenceLocalBinds letBinds)
        x -> [x]

    sequenceLocalBinds :: GHC.LHsLocalBinds GHC.GhcPs -> [GHC.LHsLocalBinds GHC.GhcPs]
    sequenceLocalBinds = \case
        GHC.L loc (GHC.HsValBinds GHC.NoExt valBinds)
            | GHC.ValBinds GHC.NoExt bindBag sigs <- valBinds
            , bind : binds <- GHC.bagToList bindBag
            , notNull binds
            -> makeLocalValBinds loc (bind :| binds) sigs
        binds -> [binds]

    makeLocalValBinds :: GHC.SrcSpan
        -> NonEmpty (GHC.LHsBindLR GHC.GhcPs GHC.GhcPs)
        -> [GHC.LSig GHC.GhcPs]
        -> [GHC.LHsLocalBinds GHC.GhcPs]
    makeLocalValBinds loc binds sigs =
        let (bind1, bindRestM) = NE.uncons binds
        in case bindRestM of
            Nothing -> [ makeLocalValBind loc bind1 sigs ]
            Just bindRest ->
                let (sig1, sigRest) = peelRelevantSigs bind1 sigs
                in makeLocalValBind loc bind1 sig1 : makeLocalValBinds loc bindRest sigRest

    makeLocalValBind :: GHC.SrcSpan
        -> GHC.LHsBindLR GHC.GhcPs GHC.GhcPs
        -> [GHC.LSig GHC.GhcPs]
        -> GHC.LHsLocalBinds GHC.GhcPs
    makeLocalValBind loc bind sigs =
        GHC.L loc (GHC.HsValBinds GHC.NoExt
            (GHC.ValBinds GHC.NoExt (GHC.unitBag bind) sigs))

    -- | List of names bound in HsBindLR
    bindNames :: GHC.HsBindLR GHC.GhcPs GHC.GhcPs -> [GHC.RdrName]
    bindNames = \case
        GHC.FunBind { fun_id } -> [ GHC.unLoc fun_id ]
        GHC.PatBind { pat_lhs } -> Uniplate.universeBi pat_lhs -- safe overapproximation
        GHC.VarBind { var_id } -> [ var_id ]
        GHC.PatSynBind _ GHC.PSB { psb_id } -> [ GHC.unLoc psb_id ]
        GHC.PatSynBind _ GHC.XPatSynBind { } -> unexpected "XPatSynBind"
        GHC.AbsBinds {} -> unexpected "AbsBinds"
        GHC.XHsBindsLR {} -> unexpected "XHsBindsLR"

    -- Given a binding and a set of signatures, split it into the signatures relevant to this binding and other signatures.
    peelRelevantSigs :: GHC.LHsBindLR GHC.GhcPs GHC.GhcPs
        -> [GHC.LSig GHC.GhcPs]
        -> ([GHC.LSig GHC.GhcPs], [GHC.LSig GHC.GhcPs])
    peelRelevantSigs bind sigs =
        let names = Set.fromList (bindNames (GHC.unLoc bind))
            (sigs1, sigs2) = unzip (map (peelRelevantSig names) sigs)
        in (concat sigs1, concat sigs2)

    peelRelevantSig :: Set.Set GHC.RdrName -> GHC.LSig GHC.GhcPs -> ([GHC.LSig GHC.GhcPs], [GHC.LSig GHC.GhcPs])
    peelRelevantSig relevantNames = \case
        GHC.L loc (GHC.TypeSig GHC.NoExt sigNames sigType) ->
            partitionByRelevance relevantNames sigNames
                (\ns -> GHC.L loc (GHC.TypeSig GHC.NoExt ns sigType))
        GHC.L loc (GHC.PatSynSig GHC.NoExt sigNames sigType) ->
            partitionByRelevance relevantNames sigNames
                (\ns -> GHC.L loc (GHC.PatSynSig GHC.NoExt ns sigType))
        _ -> unexpected "local signature"

    partitionByRelevance :: Set.Set GHC.RdrName
        -> [GHC.Located GHC.RdrName]
        -> ([GHC.Located GHC.RdrName] -> t)
        -> ([t], [t])
    partitionByRelevance relevantNames sigNames f =
        let (names1, names2) = partition ((`Set.member` relevantNames) . GHC.unLoc) sigNames
            sigs1 = [ f names1 | notNull names1 ]
            sigs2 = [ f names2 | notNull names2 ]
        in (sigs1, sigs2)

    unexpected :: String -> t
    unexpected x = error (unwords ["Unexpected", x, "in daml-preprocessor"])
