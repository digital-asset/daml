-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

{-# LANGUAGE RecordWildCards #-}

module DA.Daml.GHC.Compiler.Generics
    ( genericsPreprocessor
    ) where

import "ghc-lib-parser" Bag
import "ghc-lib-parser" BasicTypes
import Data.Maybe
import "ghc-lib-parser" FastString
import "ghc-lib" GHC
import "ghc-lib-parser" Name
import "ghc-lib-parser" Outputable
import "ghc-lib-parser" PrelNames
import "ghc-lib-parser" RdrName



data GenericKind = Gen0 | Gen1

-- | Generate a generic instance for data definitions with a `deriving Generic` or `deriving
-- Generic1` derivation.
genericsPreprocessor :: Maybe String -> ParsedSource -> ParsedSource
genericsPreprocessor mbPkgName (L l src) =
    L l
        src
            { hsmodDecls =
                  map dropGenericDeriv (hsmodDecls src) ++
                  [ generateGenericInstanceFor
                      t
                      n
                      (fromMaybe "Main" mbPkgName)
                      (fromMaybe (error "Missing module name") $ hsmodName src)
                      tys
                      d
                  | (t, n, tys, d) <- dataDecls
                  ]
            }
    -- the data declarations deriving generic instances
  where
    dataDecls =
        [ (name, tcdLName, tcdTyVars, tcdDataDefn)
        | L _ (TyClD _x (DataDecl {..})) <- hsmodDecls src
        , d <- unLoc $ dd_derivs tcdDataDefn
        , (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name))))) <-
              unLoc $ deriv_clause_tys $ unLoc d
        , name `elem` [nameOccName n | n <- genericClassNames]
        ]
    dropGenericDeriv :: LHsDecl GhcPs -> LHsDecl GhcPs
    dropGenericDeriv decl =
        case decl of
            L loc (TyClD x (DataDecl tcdDExt tcdLName tcdTyVars tcdFixity (HsDataDefn dd_ext dd_ND dd_ctxt dd_cType dd_kindSig dd_cons (L loc2 derivs)))) ->
                L
                    loc
                    (TyClD
                         x
                         (DataDecl
                              tcdDExt
                              tcdLName
                              tcdTyVars
                              tcdFixity
                              (HsDataDefn
                                   dd_ext
                                   dd_ND
                                   dd_ctxt
                                   dd_cType
                                   dd_kindSig
                                   dd_cons
                                   (L loc2 $ map dropGeneric derivs))))
            _other -> decl
      where
        dropGeneric :: LHsDerivingClause GhcPs -> LHsDerivingClause GhcPs
        dropGeneric clause =
            case clause of
                L l (HsDerivingClause deriv_clause_ext deriv_clause_strategy (L l2 derive_clause_tys)) ->
                    L
                        l
                        (HsDerivingClause
                             deriv_clause_ext
                             deriv_clause_strategy
                             (L l2 $ filter (not . isGeneric) derive_clause_tys))
                _other -> clause
        isGeneric :: LHsSigType GhcPs -> Bool
        isGeneric (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name)))))
            | name `elem` [nameOccName n | n <- genericClassNames] = True
        isGeneric _other = False


-- | Generate a generic instance for a data type.
generateGenericInstanceFor ::
       OccName
    -> Located (IdP GhcPs)
    -> String
    -> Located ModuleName
    -> LHsQTyVars GhcPs
    -> HsDataDefn GhcPs
    -> LHsDecl GhcPs
generateGenericInstanceFor genClass name@(L loc _n) pkgName modName tyVars dataDef@HsDataDefn {} =
    pprTrace "reptype" (ppr instDecl) $
    L loc $ InstD NoExt (ClsInstD {cid_d_ext = NoExt, cid_inst = instDecl})
  where
    instDecl =
        ClsInstDecl
            { cid_ext = NoExt
            , cid_poly_ty = HsIB NoExt typ
            , cid_binds = emptyBag -- unitBag from `unionBags` unitBag to
            , cid_sigs = []
            , cid_tyfam_insts = []
            , cid_datafam_insts = []
            , cid_overlap_mode = Just $ mkLoc $ NoOverlap NoSourceText
            }
    _genKind
        | genClass == nameOccName genClassName = Gen0
        | genClass == nameOccName gen1ClassName = Gen1
        | otherwise = error $ "Deriving generic instance for non-generic class"
    -- Tag something with the location of the data type we're creating a generic instance for.
    mkLoc = L loc
    typ =
        mkHsAppTys
            (mkLoc $ HsTyVar NoExt NotPromoted $ mkLoc (Unqual genClass))
            [tycon, repType]
    tycon =
        mkHsAppTys (mkLoc $ HsTyVar NoExt NotPromoted name) $
        hsLTyVarBndrsToTypes tyVars

    -- The generic type representation
    ----------------------------------
    repType :: LHsType GhcPs
    repType = mkD dataDef
    mkD :: HsDataDefn GhcPs -> LHsType GhcPs
    mkD a =
        mkHsAppTys
            d1
            [ metaDataTy
                  (occNameString $ rdrNameOcc $ unLoc name)
                  (moduleNameString $ unLoc modName)
                  pkgName
                  (dd_ND dataDef == NewType)
            , sumP $ dd_cons a
            ]
    mkS :: Maybe [LFieldOcc GhcPs] -> LHsType GhcPs -> LHsType GhcPs
    mkS mLbl a = mkHsAppTys s1 [metaSelTy mLbl a, a]
    sumP :: [LConDecl GhcPs] -> LHsType GhcPs
    sumP [] = v1
    sumP cs = foldr1 mkSum' $ map mkC cs
    mkSum' :: LHsType GhcPs -> LHsType GhcPs -> LHsType GhcPs
    mkSum' a b = mkHsAppTys plus [a, b]
    mkProd :: LHsType GhcPs -> LHsType GhcPs -> LHsType GhcPs
    mkProd a b = mkHsAppTys times [a, b]
    mkC :: LConDecl GhcPs -> LHsType GhcPs
    mkC c@(L _ ConDeclH98 {..}) = mkHsAppTys c1 [metaConsTy c, prod con_args]
    mkC (L _ ConDeclGADT {}) =
        error "Can't generate generic instances for data types with GADT's"
    mkC (L _ XConDecl {}) =
        error "Can't generate generic instance for extended AST"
    prod :: HsConDeclDetails GhcPs -> LHsType GhcPs
    prod (PrefixCon as)
        | null as = u1
        | otherwise = foldr1 mkProd $ [mkS Nothing $ mkRec0 a | a <- as]
    prod (RecCon (L _ fs)) =
        foldr1 mkProd $
        [ mkS (Just cd_fld_names) $ mkRec0 $ cd_fld_type
        | (L _ (ConDeclField {..})) <- fs
        ]
    prod (InfixCon a b) = mkProd (mkRec0 a) (mkRec0 b)
    -- | We don't have no unboxed types, so nothing to do here.
    mkRec0 :: LBangType GhcPs -> LHsType GhcPs
    mkRec0 t = mkHsAppTy rec0 t
    -- | Type variables defined in GHC.Generics
    mkGenTy :: Name -> LHsType GhcPs
    mkGenTy name =
        mkLoc $
        HsTyVar NoExt NotPromoted $
        mkLoc $ Qual (moduleName gHC_GENERICS) $ occName name

    mkGenCon = mkGenCon0 . occNameString . occName
    mkGenCon0 :: String -> LHsType GhcPs
    mkGenCon0 name =
        mkLoc $
        HsTyVar NoExt IsPromoted $
        mkLoc $ Qual (moduleName gHC_GENERICS) $ mkDataOcc name

    mkStrLit :: String -> LHsType GhcPs
    mkStrLit n = mkLoc $ HsTyLit NoExt $ HsStrTy (SourceText n) (mkFastString n)
    mkGenPromotedTy :: String -> LHsType GhcPs
    mkGenPromotedTy n =
        mkLoc $
        HsTyVar NoExt IsPromoted $
        mkLoc $ Qual (moduleName gHC_GENERICS) $ mkDataOcc n
    mkPromotedTy :: Module -> String -> LHsType GhcPs
    mkPromotedTy m n =
        mkLoc $
        HsTyVar NoExt IsPromoted $ mkLoc $ Qual (moduleName m) $ mkDataOcc n
    d1 = mkGenTy d1TyConName
    v1 = mkGenTy v1TyConName
    c1 = mkGenTy c1TyConName
    u1 = mkGenTy u1TyConName
    s1 = mkGenTy s1TyConName
    rec0 = mkGenTy rec0TyConName
    md = mkGenCon metaDataDataConName
    md0 = mkGenCon0 "MetaData0"
    mc = mkGenCon metaConsDataConName
    mc0 = mkGenCon0 "MetaCons0"
    ms = mkGenCon metaSelDataConName
    ms0 = mkGenCon0 "MetaSel0"
    plus = mkGenTy sumTyConName
    times = mkGenTy prodTyConName
    metaDataTy :: String -> String -> String -> Bool -> LHsType GhcPs
    metaDataTy name mod pkg isNewType =
        mkHsAppTy md (metaDataTy0 name mod pkg isNewType)
    metaConsTy :: LConDecl GhcPs -> LHsType GhcPs
    metaConsTy c = mkHsAppTy mc $ metaConsTy0 c
    metaSelTy mbLbs a = mkHsAppTy ms $ metaSelTy0 mbLbs a
    metaDataTy0 name mod pkg isNewType =
        mkHsAppTys
            md0
            [ mkStrLit name
            , mkStrLit mod
            , mkStrLit pkg
            , if isNewType
                  then mkPromotedTy gHC_TYPES "True"
                  else mkPromotedTy gHC_TYPES "False"
            ]
    metaConsTy0 :: LConDecl GhcPs -> LHsType GhcPs
    metaConsTy0 (L _ ConDeclH98 {..}) =
        mkHsAppTys mc0 [n, fixity, hasRecordSelectors]
      where
        n = mkStrLit $ occNameString $ rdrNameOcc $ unLoc con_name
        fixity
            | PrefixCon {} <- con_args = mkGenPromotedTy "PrefixI"
            | InfixCon {} <- con_args = mkGenPromotedTy "PrefixI" -- TODO (drsk) where is the fixity noted?
            | RecCon {} <- con_args = mkGenPromotedTy "PrefixI"
        hasRecordSelectors
            | PrefixCon {} <- con_args = mkPromotedTy gHC_TYPES "False"
            | InfixCon {} <- con_args = mkPromotedTy gHC_TYPES "False"
            | RecCon (L _ rec) <- con_args
            , not (null rec) = mkPromotedTy gHC_TYPES "True"
            | RecCon (L _ _rec) <- con_args = mkPromotedTy gHC_TYPES "False"
    metaConsTy0 (L _ ConDeclGADT {}) =
        error "Can't generate generic instances for data types with GADT's"
    metaConsTy0 (L _ XConDecl {}) =
        error "Can't generate generic instance for extended AST"
    metaSelTy0 :: Maybe [LFieldOcc GhcPs] -> LHsType GhcPs -> LHsType GhcPs
    metaSelTy0 mbLbs a =
        mkHsAppTys
            ms0
            [mbRecordName, srcUnpackedness, srcStrictness, decidedStrictness]
      where
        mbRecordName
            | Just [l] <- mbLbs =
                mkHsAppTy
                    (mkGenPromotedTy "Just")
                    (mkStrLit $
                     occNameString $
                     rdrNameOcc $ unLoc $ rdrNameFieldOcc $ unLoc l)
            | otherwise = mkGenPromotedTy "Nothing"
        HsSrcBang _st su ss = getBangStrictness a
        srcUnpackedness =
            case su of
                SrcUnpack -> mkGenCon sourceUnpackDataConName
                SrcNoUnpack -> mkGenCon sourceNoUnpackDataConName
                NoSrcUnpack -> mkGenCon noSourceUnpackednessDataConName
        srcStrictness =
            case ss of
                SrcLazy -> mkGenCon sourceLazyDataConName
                SrcStrict -> mkGenCon sourceStrictDataConName
                NoSrcStrict -> mkGenCon noSourceStrictnessDataConName
        decidedStrictness = mkGenCon decidedLazyDataConName -- TODO (drsk) we don't know decided strictness at parsing, we need to remove the field.

    -- Generic method generation
    ----------------------------
    from = undefined
    to = undefined

generateGenericInstanceFor _genClass _n _pkgName _mod _tyVars XHsDataDefn {} =
    error "Can't generate generic instance for extended AST"
