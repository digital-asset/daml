-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

module DA.Daml.Preprocessor.Generics
    ( genericsPreprocessor
    , generateGenericInstanceFor
    ) where

import "ghc-lib-parser" Bag
import "ghc-lib-parser" BasicTypes
import Data.Maybe
import "ghc-lib-parser" DataCon
import "ghc-lib-parser" FastString
import "ghc-lib" GHC
import "ghc-lib-parser" Module
import "ghc-lib-parser" Name
import "ghc-lib-parser" PrelNames hiding
    ( from_RDR
    , gHC_GENERICS
    , gHC_TYPES
    , k1DataCon_RDR
    , l1DataCon_RDR
    , m1DataCon_RDR
    , prodDataCon_RDR
    , r1DataCon_RDR
    , to_RDR
    , u1DataCon_RDR
    )
import "ghc-lib-parser" RdrName
import "ghc-lib" TcGenDeriv
import "ghc-lib-parser" Util
import SdkVersion


-- | Generate a generic instance for data definitions with a `deriving Generic` or `deriving
-- Generic1` derivation.
genericsPreprocessor :: Maybe UnitId -> ParsedSource -> ParsedSource
genericsPreprocessor mbPkgName (L l src) =
    L l
        src
            { hsmodDecls =
                  map dropGenericDeriv (hsmodDecls src) ++
                  [ generateGenericInstanceFor
                      genericsType
                      name
                      (fromMaybe (stringToUnitId "unknown-package") mbPkgName)
                      -- In `damlc build` and `damlc package` we always have a unit id, this is only for
                      -- `damlc compile`.
                      (fromMaybe (error "Missing module name") $ hsmodName src)
                      -- We check in the preprocessor that the module name is always present.
                      tys
                      dataDef
                  | (genericsType, name, tys, dataDef) <- dataDeclsWithGenericDeriv
                  ]
            }
  where
    -- the data declarations deriving generic instances
    dataDeclsWithGenericDeriv =
        [ (name, tcdLName, tcdTyVars, tcdDataDefn)
        | L _ (TyClD _x DataDecl {..}) <- hsmodDecls src
        , d <- unLoc $ dd_derivs tcdDataDefn
        , (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name))))) <-
              unLoc $ deriv_clause_tys $ unLoc d
        , name `elem` [nameOccName n | n <- genericClassNames]
        ]
    -- drop the generic deriving clauses from the parsed source otherwise GHC will try to process
    -- them.
    dropGenericDeriv :: LHsDecl GhcPs -> LHsDecl GhcPs
    dropGenericDeriv decl =
        case decl of
            L loc (TyClD x (DataDecl tcdDExt tcdLName tcdTyVars tcdFixity
                              (HsDataDefn dd_ext dd_ND dd_ctxt dd_cType dd_kindSig dd_cons
                                (L loc2 derivs)))) ->
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
        isGeneric (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name))))) =
            name `elem` [nameOccName n | n <- genericClassNames]
        isGeneric _other = False


-- | TODO (drsk) Add support for Gen1. For now we only support Gen0
gen1Error :: a
gen1Error = error "Generation of Generic1 not supported."

gADTError :: a
gADTError = error "Can't generate generic instances for data types with GADT's"

extError :: a
extError = error "Can't generate generic instances for extended AST"

-- NOTE (drsk) Our Generics is located in DA.Generics instead of GHC.Generics. Also some of the used
-- data types are in daml-stdlib and not in ghc-prim. Hence the need for the following RdrName
-- definitions coming from ghc-parser PrelNames.

stdlibUnitId :: UnitId
stdlibUnitId = fsToUnitId (fsLit $ unitIdString damlStdlib)

mkStdlibModule :: FastString -> Module
mkStdlibModule m = mkModule stdlibUnitId (mkModuleNameFS m)

gHC_GENERICS :: Module
gHC_GENERICS = mkStdlibModule (fsLit "DA.Generics")

gHC_TYPES :: Module
gHC_TYPES = mkStdlibModule (fsLit "Prelude")

u1DataCon_RDR, _par1DataCon_RDR, _rec1DataCon_RDR,
  k1DataCon_RDR, m1DataCon_RDR, l1DataCon_RDR, r1DataCon_RDR,
  prodDataCon_RDR, _comp1DataCon_RDR,
  _unPar1_RDR, _unRec1_RDR, _unK1_RDR, _unComp1_RDR,
  from_RDR, _from1_RDR, to_RDR, _to1_RDR :: RdrName
u1DataCon_RDR    = dataQual_RDR gHC_GENERICS (fsLit "U1")
_par1DataCon_RDR  = dataQual_RDR gHC_GENERICS (fsLit "Par1")
_rec1DataCon_RDR  = dataQual_RDR gHC_GENERICS (fsLit "Rec1")
k1DataCon_RDR    = dataQual_RDR gHC_GENERICS (fsLit "K1")
m1DataCon_RDR    = dataQual_RDR gHC_GENERICS (fsLit "M1")

l1DataCon_RDR     = dataQual_RDR gHC_GENERICS (fsLit "L1")
r1DataCon_RDR     = dataQual_RDR gHC_GENERICS (fsLit "R1")

prodDataCon_RDR   = dataQual_RDR gHC_GENERICS (fsLit "P1")
_comp1DataCon_RDR  = dataQual_RDR gHC_GENERICS (fsLit "Comp1")

_unPar1_RDR  = varQual_RDR gHC_GENERICS (fsLit "unPar1")
_unRec1_RDR  = varQual_RDR gHC_GENERICS (fsLit "unRec1")
_unK1_RDR    = varQual_RDR gHC_GENERICS (fsLit "unK1")
_unComp1_RDR = varQual_RDR gHC_GENERICS (fsLit "unComp1")

from_RDR  = mkRdrUnqual $ mkOccName varName "from" -- from/to appears as pattern and can not be qualified.
_from1_RDR = varQual_RDR gHC_GENERICS (fsLit "from1")
to_RDR    = mkRdrUnqual $ mkOccName varName "to" -- from/to appears as pattern and can not be qualified.
_to1_RDR   = varQual_RDR gHC_GENERICS (fsLit "to1")


-- | Generate a generic instance for a data type.
generateGenericInstanceFor ::
       OccName
    -> Located (IdP GhcPs)
    -> UnitId
    -> Located ModuleName
    -> LHsQTyVars GhcPs
    -> HsDataDefn GhcPs
    -> LHsDecl GhcPs
generateGenericInstanceFor genClass name@(L loc _n) pkgName modName tyVars dataDef@HsDataDefn{} =
    L loc $ InstD NoExt (ClsInstD {cid_d_ext = NoExt, cid_inst = instDecl})
  where
    instDecl =
        ClsInstDecl
            { cid_ext = NoExt
            , cid_poly_ty = HsIB NoExt typ
            , cid_binds = mkBindsRep genKind loc dataDef
            , cid_sigs = []
            , cid_tyfam_insts = []
            , cid_datafam_insts = []
            , cid_overlap_mode = Just $ mkLoc $ NoOverlap NoSourceText
            }
    genKind
        | genClass == nameOccName genClassName = Gen0
        | genClass == nameOccName gen1ClassName = Gen1
        | otherwise = error "Deriving generic instance for non-generic class"
        -- This can't happen since we only call "generateGenericInstanceFor" for generic
        -- derivations.

    -- Tag something with the location of the data type we're creating a generic instance for.
    mkLoc = L loc

    -- The typeclass type: data Foo a b ... deriving Generic => Generic (Foo a b) (FooRep a b)
    typ =
        mkHsAppTys
            (mkLoc $ HsTyVar NoExt NotPromoted $ mkLoc (Qual (moduleName gHC_GENERICS) genClass))
            [tycon, repType]
    -- The type for which we construct a generic instance
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
    sumP cs = foldBal mkSum' $ map mkC cs
    mkSum' :: LHsType GhcPs -> LHsType GhcPs -> LHsType GhcPs
    mkSum' a b = mkHsAppTys plus [a, b]
    mkProd :: LHsType GhcPs -> LHsType GhcPs -> LHsType GhcPs
    mkProd a b = mkHsAppTys times [a, b]
    mkC :: LConDecl GhcPs -> LHsType GhcPs
    mkC c@(L _ ConDeclH98 {..}) = mkHsAppTys c1 [metaConsTy c, prod con_args]
    mkC (L _ ConDeclGADT {}) = gADTError
    mkC (L _ XConDecl {}) = extError
    prod :: HsConDeclDetails GhcPs -> LHsType GhcPs
    prod (PrefixCon as)
        | null as = u1
        | otherwise = foldBal mkProd [mkS Nothing $ mkRec0 a | a <- as]
    prod (RecCon (L _ fs)) | null fs = u1
    prod (RecCon (L _ fs)) =
        foldBal mkProd
        [ mkS (Just cd_fld_names) $ mkRec0 cd_fld_type
        | L _ ConDeclField {..} <- fs
        ]
    prod (InfixCon a b) = mkProd (mkRec0 a) (mkRec0 b)
    -- | We don't have unboxed types, so nothing to do here.
    mkRec0 :: LBangType GhcPs -> LHsType GhcPs
    mkRec0 t = mkHsAppTy rec0 t
    -- | Type variables defined in DA.Generics
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
    mkPromotedTy :: Module -> String -> LHsType GhcPs
    mkPromotedTy m n =
        mkLoc $
        HsTyVar NoExt IsPromoted $ mkLoc $ Qual (moduleName m) $ mkDataOcc n
    mkGenPromotedTy :: String -> LHsType GhcPs
    mkGenPromotedTy n = mkPromotedTy gHC_GENERICS n
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
    metaDataTy :: String -> String -> UnitId -> Bool -> LHsType GhcPs
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
            , mkStrLit (unitIdString pkg)
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
            | InfixCon {} <- con_args = mkGenPromotedTy "PrefixI"
            -- TODO (drsk) where is the fixity noted? For now put it to PrefixI.
            | RecCon {} <- con_args = mkGenPromotedTy "PrefixI"
        hasRecordSelectors
            | PrefixCon {} <- con_args = mkPromotedTy gHC_TYPES "False"
            | InfixCon {} <- con_args = mkPromotedTy gHC_TYPES "False"
            | RecCon (L _ rec) <- con_args
            , not (null rec) = mkPromotedTy gHC_TYPES "True"
            | RecCon (L _ _rec) <- con_args = mkPromotedTy gHC_TYPES "False"
    metaConsTy0 (L _ ConDeclGADT {}) = gADTError
    metaConsTy0 (L _ XConDecl {}) = extError
    metaSelTy0 :: Maybe [LFieldOcc GhcPs] -> LHsType GhcPs -> LHsType GhcPs
    metaSelTy0 mbLbs a =
        mkHsAppTys
            ms0
            [mbRecordName, srcUnpackedness, srcStrictness]
      where
        mbRecordName
            | Just [l] <- mbLbs =
                mkHsAppTy
                    (mkPromotedTy gHC_TYPES "Some")
                    (mkStrLit $
                     occNameString $
                     rdrNameOcc $ unLoc $ rdrNameFieldOcc $ unLoc l)
            | otherwise = mkPromotedTy gHC_TYPES "None"
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
generateGenericInstanceFor _genClass _n _pkgName _mod _tyVars XHsDataDefn {} = extError


-- Bindings for the Generic instance.
--
-- NOTE (drsk) Most of the code below is copied from GHC with minor tweaks for our case
-- https://gitlab.haskell.org/ghc/ghc.git 4ba73e00c4887b58d85131601a15d00608acaa60
-- compiler/typecheck/TcGenGernerics.hs
---------------------------------------------------------------------------------------

type US = Int   -- Local unique supply, just a plain Int
type Alt = (LPat GhcPs, LHsExpr GhcPs)

-- GenericKind serves to mark if a datatype derives Generic (Gen0) or
-- Generic1 (Gen1).
data GenericKind = Gen0 | Gen1

-- as above, but with a payload of the TyCon's name for "the" parameter
data GenericKind_ = Gen0_ | Gen1_ TyVar

-- as above, but using a single datacon's name for "the" parameter
data GenericKind_DC = Gen0_DC | Gen1_DC TyVar

forgetArgVar :: GenericKind_DC -> GenericKind
forgetArgVar Gen0_DC   = Gen0
forgetArgVar Gen1_DC{} = Gen1

-- When working only within a single datacon, "the" parameter's name should
-- match that datacon's name for it.
gk2gkDC :: GenericKind_ -> ConDecl GhcPs -> GenericKind_DC
gk2gkDC Gen0_   _ = Gen0_DC
gk2gkDC Gen1_{} _d = gen1Error -- Gen1_DC $ last $ dataConUnivTyVars d

mkBindsRep :: GenericKind -> SrcSpan -> HsDataDefn GhcPs -> LHsBinds GhcPs
mkBindsRep gk loc dataDef =
    unitBag (mkRdrFunBind (L loc from01_RDR) [from_eqn])
  `unionBags`
    unitBag (mkRdrFunBind (L loc to01_RDR) [to_eqn])
      where
        -- The topmost M1 (the datatype metadata) has the exact same type
        -- across all cases of a from/to definition, and can be factored out
        -- to save some allocations during typechecking.
        -- See Note [Generics compilation speed tricks]
        from_eqn =
            mkSimpleMatch
                (FunRhs
                     { mc_fun = L loc from01_RDR
                     , mc_fixity = Prefix
                     , mc_strictness = NoSrcStrict
                     })
                [x_Pat] $
            mkM1_E $ nlHsPar $ nlHsCase x_Expr from_matches
        to_eqn =
            mkSimpleMatch
                (FunRhs
                     { mc_fun = L loc to01_RDR
                     , mc_fixity = Prefix
                     , mc_strictness = NoSrcStrict
                     })
                [mkM1_P x_Pat] $
            nlHsCase x_Expr to_matches

        from_matches  = [mkHsCaseAlt pat rhs | (pat,rhs) <- from_alts]
        to_matches    = [mkHsCaseAlt pat rhs | (pat,rhs) <- to_alts  ]
        datacons      = dd_cons dataDef

        (from01_RDR, to01_RDR) = case gk of
                                   Gen0 -> (from_RDR,  to_RDR)
                                   Gen1 -> gen1Error -- (from1_RDR, to1_RDR)

        -- Recurse over the sum first
        from_alts, to_alts :: [Alt]
        (from_alts, to_alts) = mkSum gk_ (1 :: US) datacons
          where gk_ = case gk of
                  Gen0 -> Gen0_
                  Gen1 -> gen1Error
                    --ASSERT(tyvars `lengthAtLeast` 1)
                          --Gen1_ (last tyvars)
                    --where tyvars = tyConTyVars tycon

--------------------------------------------------------------------------------
-- Dealing with sums
--------------------------------------------------------------------------------

mkSum :: GenericKind_ -- Generic or Generic1?
      -> US          -- Base for generating unique names
      -> [LConDecl GhcPs]   -- The data constructors
      -> ([Alt],     -- Alternatives for the T->Trep "from" function
          [Alt])     -- Alternatives for the Trep->T "to" function

-- Datatype without any constructors
mkSum _ _ [] = ([from_alt], [to_alt])
  where
    from_alt = (x_Pat, nlHsCase x_Expr [])
    to_alt   = (x_Pat, nlHsCase x_Expr [])
               -- These M1s are meta-information for the datatype

-- Datatype with at least one constructor
mkSum gk_ us datacons =
  -- switch the payload of gk_ to be datacon-centric instead of tycon-centric
 unzip [ mk1Sum (gk2gkDC gk_ d) us i (length datacons) d
           | (L _ d,i) <- zip datacons [1..] ]

-- Build the sum for a particular constructor
mk1Sum :: GenericKind_DC -- Generic or Generic1?
       -> US        -- Base for generating unique names
       -> Int       -- The index of this constructor
       -> Int       -- Total number of constructors
       -> ConDecl GhcPs -- The data constructor
       -> (Alt,     -- Alternative for the T->Trep "from" function
           Alt)     -- Alternative for the Trep->T "to" function
mk1Sum gk_ us i n datacon = (from_alt, to_alt)
  where
    gk = forgetArgVar gk_

    -- Existentials already excluded
    argTys = hsConDeclArgTys $ getConArgs datacon
    n_args = length argTys

    datacon_varTys = zip (map mkGenericLocal [us .. us+n_args-1]) argTys
    datacon_vars = map fst datacon_varTys
    us'          = us + n_args

    datacon_rdr | ConDeclH98{..} <- datacon = unLoc con_name
                | ConDeclGADT{} <- datacon = gADTError
                | XConDecl{} <- datacon = extError

    from_alt     = (nlConVarPat datacon_rdr datacon_vars, from_alt_rhs)
    from_alt_rhs = genLR_E i n (mkProd_E gk_ us' datacon_varTys)

    to_alt     = ( genLR_P i n (mkProd_P gk us' datacon_varTys)
                 , to_alt_rhs
                 ) -- These M1s are meta-information for the datatype
    to_alt_rhs = case gk_ of
      Gen0_DC        -> nlHsVarApps datacon_rdr datacon_vars
      Gen1_DC _argVar -> gen1Error -- nlHsApps datacon_rdr $ map argTo datacon_varTys
        --where
          --argTo (var, ty) = converter ty `nlHsApp` nlHsVar var where
            --converter = argTyFold argVar $ ArgTyAlg
              --{ata_rec0 = nlHsVar . unboxRepRDR,
               --ata_par1 = nlHsVar unPar1_RDR,
               --ata_rec1 = const $ nlHsVar unRec1_RDR,
               --ata_comp = \_ cnv -> (nlHsVar fmap_RDR `nlHsApp` cnv)
                                    --`nlHsCompose` nlHsVar unComp1_RDR}


-- Generates the L1/R1 sum pattern
genLR_P :: Int -> Int -> LPat GhcPs -> LPat GhcPs
genLR_P i n p
  | n == 0       = error "impossible"
  | n == 1       = p
  | i <= div n 2 = nlParPat $ nlConPat l1DataCon_RDR [genLR_P i     (div n 2) p]
  | otherwise    = nlParPat $ nlConPat r1DataCon_RDR [genLR_P (i-m) (n-m)     p]
                     where m = div n 2

-- Generates the L1/R1 sum expression
genLR_E :: Int -> Int -> LHsExpr GhcPs -> LHsExpr GhcPs
genLR_E i n e
  | n == 0       = error "impossible"
  | n == 1       = e
  | i <= div n 2 = nlHsVar l1DataCon_RDR `nlHsApp`
                                            nlHsPar (genLR_E i     (div n 2) e)
  | otherwise    = nlHsVar r1DataCon_RDR `nlHsApp`
                                            nlHsPar (genLR_E (i-m) (n-m)     e)
                     where m = div n 2

--------------------------------------------------------------------------------
-- Dealing with products
--------------------------------------------------------------------------------
-- Build a product expression
mkProd_E :: GenericKind_DC    -- Generic or Generic1?
         -> US                -- Base for unique names
         -> [(RdrName, LBangType GhcPs)]
                       -- List of variables matched on the lhs and their types
         -> LHsExpr GhcPs   -- Resulting product expression
mkProd_E _   _ []     = mkM1_E (nlHsVar u1DataCon_RDR)
mkProd_E gk_ _ varTys = mkM1_E (foldBal prod appVars)
                     -- These M1s are meta-information for the constructor
  where
    appVars = map (wrapArg_E gk_) varTys

    prod :: LHsExpr GhcPs -> LHsExpr GhcPs -> LHsExpr GhcPs
    prod a b = prodDataCon_RDR `nlHsApps` [a,b]

wrapArg_E :: GenericKind_DC -> (RdrName, LBangType GhcPs) -> LHsExpr GhcPs
wrapArg_E Gen0_DC          (var, _ty) = mkM1_E $
                            k1DataCon_RDR `nlHsVarApps` [var]
                         -- This M1 is meta-information for the selector
wrapArg_E (Gen1_DC _argVar) (_var, _ty) = gen1Error -- mkM1_E $
                            --converter ty `nlHsApp` nlHsVar var
                         ---- This M1 is meta-information for the selector
  --where converter = argTyFold argVar $ ArgTyAlg
          --{ata_rec0 = nlHsVar . boxRepRDR,
           --ata_par1 = nlHsVar par1DataCon_RDR,
           --ata_rec1 = const $ nlHsVar rec1DataCon_RDR,
           --ata_comp = \_ cnv -> nlHsVar comp1DataCon_RDR `nlHsCompose`
                                  --(nlHsVar fmap_RDR `nlHsApp` cnv)}

-- Build a product pattern
mkProd_P :: GenericKind       -- Gen0 or Gen1
         -> US                -- Base for unique names
         -> [(RdrName, LBangType GhcPs)] -- List of variables to match,
                              --   along with their types
         -> LPat GhcPs      -- Resulting product pattern
mkProd_P _  _ []     = mkM1_P (nlNullaryConPat u1DataCon_RDR)
mkProd_P gk _ varTys = mkM1_P (foldBal prod appVars)
                     -- These M1s are meta-information for the constructor
  where
    appVars = unzipWith (wrapArg_P gk) varTys
    prod a b = nlParPat $ prodDataCon_RDR `nlConPat` [a,b]

wrapArg_P :: GenericKind -> RdrName -> LBangType GhcPs -> LPat GhcPs
wrapArg_P Gen0 v _ty = mkM1_P (nlParPat $ k1DataCon_RDR `nlConVarPat` [v])
                   -- This M1 is meta-information for the selector
wrapArg_P Gen1 v _  = nlParPat $ m1DataCon_RDR `nlConVarPat` [v]

mkGenericLocal :: US -> RdrName
mkGenericLocal u = mkVarUnqual (mkFastString ("g" ++ show u))

x_RDR :: RdrName
x_RDR = mkVarUnqual (fsLit "x")

x_Expr :: LHsExpr GhcPs
x_Expr = nlHsVar x_RDR

x_Pat :: LPat GhcPs
x_Pat = nlVarPat x_RDR

mkM1_E :: LHsExpr GhcPs -> LHsExpr GhcPs
mkM1_E e = nlHsVar m1DataCon_RDR `nlHsApp` e

mkM1_P :: LPat GhcPs -> LPat GhcPs
mkM1_P p = nlParPat $ m1DataCon_RDR `nlConPat` [p]

_nlHsCompose :: LHsExpr GhcPs -> LHsExpr GhcPs -> LHsExpr GhcPs
_nlHsCompose x y = compose_RDR `nlHsApps` [x, y]

-- | Variant of foldr1 for producing balanced lists
foldBal :: (a -> a -> a) -> [a] -> a
foldBal op = foldBal' op (error "foldBal: empty list")

foldBal' :: (a -> a -> a) -> a -> [a] -> a
foldBal' _  x []  = x
foldBal' _  _ [y] = y
foldBal' op x l   = let (a,b) = splitAt (length l `div` 2) l
                    in foldBal' op x a `op` foldBal' op x b
