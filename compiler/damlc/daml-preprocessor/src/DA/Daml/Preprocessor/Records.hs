-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Code lifted from <https://github.com/ndmitchell/record-dot-preprocessor/blob/master/plugin/RecordDotPlugin.hs>.
module DA.Daml.Preprocessor.Records
    ( importGenerated
    , mkImport
    , recordDotPreprocessor
    , onExp
    , onImports
    ) where


import           Data.Generics.Uniplate.Data
import           Data.List.Extra
import           Data.Tuple.Extra

-- import DA.GHC.Util

import           "ghc-lib-parser" Bag
import qualified "ghc-lib" GHC
import qualified "ghc-lib" GhcPlugins as GHC
import           "ghc-lib-parser" HsSyn
import           "ghc-lib-parser" SrcLoc
import           "ghc-lib-parser" TcEvidence


noL :: e -> GenLocated SrcSpan e
noL = noLoc

noE :: GHC.NoExt
noE = GHC.NoExt

setL :: SrcSpan -> GenLocated SrcSpan e -> GenLocated SrcSpan e
setL l (L _ x) = L l x

mod_records :: GHC.ModuleName
mod_records = GHC.mkModuleName "DA.Internal.Record"

var_HasField, var_getField, var_setField, var_getFieldPrim, var_setFieldPrim, var_dot :: GHC.RdrName
var_HasField = GHC.mkRdrQual mod_records $ GHC.mkClsOcc "HasField"
var_getField = GHC.mkRdrQual mod_records $ GHC.mkVarOcc "getField"
var_setField = GHC.mkRdrQual mod_records $ GHC.mkVarOcc "setField"
var_getFieldPrim = GHC.mkRdrQual mod_records $ GHC.mkVarOcc "getFieldPrim"
var_setFieldPrim = GHC.mkRdrQual mod_records $ GHC.mkVarOcc "setFieldPrim"
var_dot = GHC.mkRdrUnqual $ GHC.mkVarOcc "."


recordDotPreprocessor :: GHC.ParsedSource -> GHC.ParsedSource
recordDotPreprocessor = fmap onModule

onModule :: HsModule GhcPs -> HsModule GhcPs
onModule x = x { hsmodImports = onImports $ hsmodImports x
               , hsmodDecls = concatMap onDecl $ hsmodDecls x
               }


onImports :: [LImportDecl GhcPs] -> [LImportDecl GhcPs]
onImports = (:) $ noL $ importGenerated True (mkImport $ noL mod_records)


{-
instance HasField "selector" Record Field where
    getField = selector
    setField v x = x{selector=v} -- EITHER
    setField v x = magicSetField @selector x v -- OR (if length selectors >= 3 AND not a sum type)

-}
instanceTemplate :: Bool -> FieldOcc GhcPs -> HsType GhcPs -> HsType GhcPs -> InstDecl GhcPs
instanceTemplate abstract selector record field = ClsInstD noE $ ClsInstDecl noE (HsIB noE typ) binds [] [] [] Nothing
    where
        typ = mkHsAppTys
            (noL (HsTyVar noE GHC.NotPromoted (noL var_HasField)))
            [noL (HsTyLit noE (HsStrTy GHC.NoSourceText (GHC.occNameFS $ GHC.occName $ unLoc $ rdrNameFieldOcc selector)))
            ,noL record
            ,noL field
            ]
        binds = listToBag $ if abstract then [getAbstract, setAbstract] else [get,set]

        funbind :: GHC.RdrName -> [LPat GhcPs] -> LHsExpr GhcPs -> LHsBind GhcPs
        funbind name args body = noL $ FunBind noE (noL $ unqual name) (MG noE (noL [eqn]) GHC.Generated) WpHole []
            where eqn = noL $ Match
                      { m_ext     = noE
                      , m_ctxt    = FunRhs (noL $ unqual name) GHC.Prefix NoSrcStrict
                      , m_pats    = args
                      , m_rhs_sig = Nothing
                      , m_grhss   = GRHSs noE [noL $ GRHS noE [] body] (noL $ EmptyLocalBinds noE)
                      }

        get = funbind var_getField [] $ noL $ HsVar noE $ rdrNameFieldOcc selector

        set = funbind var_setField
            [VarPat noE $ noL vA, VarPat noE $ noL vR] $
            noL $ RecordUpd noE (noL $ GHC.HsVar noE $ noL vR)
                (Left [noL $ HsRecField (noL (Unambiguous noE (rdrNameFieldOcc selector))) (noL $ GHC.HsVar noE $ noL vA) False])

        getAbstract = funbind var_getField [] $
            noL (HsVar noE $ noL var_getFieldPrim) `mkAppType` fldName `mkAppType` noL record `mkAppType` noL field
        setAbstract = funbind var_setField [] $
            noL (HsVar noE $ noL var_setFieldPrim) `mkAppType` fldName `mkAppType` noL record `mkAppType` noL field

        fldName = mkSelector $ GHC.rdrNameFieldOcc selector

        unqual (GHC.Qual _ x) = GHC.Unqual x
        unqual x = x

        vR = GHC.mkRdrUnqual $ GHC.mkVarOcc "r"
        vA = GHC.mkRdrUnqual $ GHC.mkVarOcc "a"


onDecl :: LHsDecl GhcPs -> [LHsDecl GhcPs]
onDecl o = descendBi onExp o : extraDecls o
    where
        extraDecls (L _ (GHC.TyClD _ x)) =
            [ noL $ InstD noE $ instanceTemplate (length ctors == 1) field (unLoc record) typ
            | let fields = nubOrdOn (\(_,_,x,_) -> GHC.occNameFS $ GHC.rdrNameOcc $ unLoc $ rdrNameFieldOcc x) $ getFields x
            , (record, _, field, typ) <- fields]
            where ctors = dd_cons $ tcdDataDefn x
        extraDecls _ = []

-- Process a Haskell data type definition. Walk its constructors. For
-- each, harvest a tuple of the parent type, the ctor name, the field
-- name and the field type.
getFields :: TyClDecl GhcPs ->
               [(LHsType GhcPs, IdP GhcPs, FieldOcc GhcPs, HsType GhcPs)]
getFields DataDecl{tcdDataDefn=HsDataDefn{..}, ..} = concatMap ctor dd_cons
    where
        ctor (L _ ConDeclH98{con_args=RecCon (L _ fields),con_name=L _ name}) = concatMap (field name) fields
        ctor _ = []

        field name (L _ ConDeclField{cd_fld_type=L _ ty, ..}) = [(result, name, fld, ty) | L _ fld <- cd_fld_names]
        field _ _ = error "unknown field declaration in getFields"

        -- A value of this data declaration will have this type.
        result = foldl' (\x y -> noL $ HsAppTy noE x $ hsLTyVarBndrToType y)
                   (noL $ HsTyVar noE GHC.NotPromoted tcdLName) $ hsq_explicit tcdTyVars
getFields _ = []


-- At this point infix expressions have not had associativity/fixity applied, so they are bracketed
-- a + b + c ==> (a + b) + c
-- Therefore we need to deal with, in general:
-- x.y, where
-- x := a | a b | a.b | a + b
-- y := a | a b | a{b=1}
onExp :: LHsExpr GhcPs -> LHsExpr GhcPs
onExp (L o (OpApp _ lhs mid@(isDot -> True) rhs))
    | adjacent lhs mid, adjacent mid rhs
    , (lhsOp, lhs) <- getOpRHS $ onExp lhs
    , (lhsApp, lhs) <- getAppRHS lhs
    , (rhsApp, rhs) <- getAppLHS rhs
    , (rhsRec, rhs) <- getRec rhs
    , Just sel <- getSelector rhs
    = onExp $ lhsOp $ rhsApp $ lhsApp $ rhsRec $ mkParen $ setL o $ mkVar var_getField `mkAppType` sel `mkApp` lhs

-- Turn (.foo.bar) into getField calls
onExp (L o (SectionR _ mid@(isDot -> True) rhs))
    | adjacent mid rhs
    , srcSpanStart o == srcSpanStart (getLoc mid)
    , srcSpanEnd o == srcSpanEnd (getLoc rhs)
    , Just sels <- getSelectors rhs
    -- Don't bracket here. The argument came in as a section so it's
    -- already enclosed in brackets.
    = setL o $ case sels of
        [] -> error "IMPOSSIBLE: getSelectors never returns an empty list"
        -- NOTE(MH): We don't want a lambda for a single projection since we
        -- don't need it when the record type is unknown. When the record type
        -- is known, the conversion to Daml-LF needs to add the lambda anyway.
        [sel] -> mkVar var_getField `mkAppType` sel
        _:_:_ -> mkLam var_record $ foldl' (\x sel -> mkVar var_getField `mkAppType` sel `mkApp` x) (mkVar var_record) sels
          where
            var_record = GHC.mkRdrUnqual $ GHC.mkVarOcc "record"

-- Turn a{b=c, ...} into setField calls
onExp (L o upd@RecordUpd{rupd_expr,rupd_flds=Left (L _ (HsRecField (fmap rdrNameAmbiguousFieldOcc -> lbl) arg pun):flds)})
    | let sel = mkSelector lbl
    , let arg2 = if pun then noL $ HsVar noE lbl else arg
    , let expr = mkParen $ mkVar var_setField `mkAppType` sel `mkApp` arg2 `mkApp` rupd_expr -- 'rupd_expr' never needs bracketing.
    = onExp $ if null flds then expr else L o upd{rupd_expr=expr,rupd_flds=Left flds}

onExp x = descend onExp x


mkSelector :: Located GHC.RdrName -> LHsType GhcPs
mkSelector (L o x) = L o $ HsTyLit noE $ HsStrTy GHC.NoSourceText $ GHC.occNameFS $ GHC.rdrNameOcc x

getSelector :: LHsExpr GhcPs -> Maybe (LHsType GhcPs)
getSelector (L _ (HsVar _ (L o sym)))
    | not $ GHC.isQual sym
    = Just $ mkSelector $ L o sym
getSelector _ = Nothing

-- | Turn a.b.c into Just [a,b,c]
getSelectors :: LHsExpr GhcPs -> Maybe [LHsType GhcPs]
getSelectors (L _ (OpApp _ lhs mid@(isDot -> True) rhs))
    | adjacent lhs mid, adjacent mid rhs
    , Just post <- getSelector rhs
    , Just pre <- getSelectors lhs
    = Just $ pre ++ [post]
getSelectors x = (:[]) <$> getSelector x

-- | Lens on: f [x]
getAppRHS :: LHsExpr GhcPs -> (LHsExpr GhcPs -> LHsExpr GhcPs, LHsExpr GhcPs)
getAppRHS (L l (HsApp e x y)) = (L l . HsApp e x, y)
getAppRHS x = (id, x)

-- | Lens on: [f] x y z
getAppLHS :: LHsExpr GhcPs -> (LHsExpr GhcPs -> LHsExpr GhcPs, LHsExpr GhcPs)
getAppLHS (L l (HsApp e x y)) = first (\c -> L l . (\x -> HsApp e x y) . c) $ getAppLHS x
getAppLHS x = (id, x)

-- | Lens on: a + [b]
getOpRHS :: LHsExpr GhcPs -> (LHsExpr GhcPs -> LHsExpr GhcPs, LHsExpr GhcPs)
getOpRHS (L l (OpApp x y p z)) = (L l . OpApp x y p, z)
getOpRHS x = (id, x)

-- | Lens on: [r]{f1=x1}{f2=x2}
getRec :: LHsExpr GhcPs -> (LHsExpr GhcPs -> LHsExpr GhcPs, LHsExpr GhcPs)
getRec (L l r@RecordUpd{}) = first (\c x -> L l r{rupd_expr=c x}) $ getRec $ rupd_expr r
getRec x = (id, x)

-- | Is it equal to: .
isDot :: LHsExpr GhcPs -> Bool
isDot (L _ (HsVar _ (L _ op))) = op == var_dot
isDot _ = False

mkVar :: GHC.RdrName -> LHsExpr GhcPs
mkVar = noL . HsVar noE . noL

mkParen :: LHsExpr GhcPs -> LHsExpr GhcPs
mkParen = noL . HsPar noE

mkApp :: LHsExpr GhcPs -> LHsExpr GhcPs -> LHsExpr GhcPs
mkApp x y = noL $ HsApp noE x y

mkAppType :: LHsExpr GhcPs -> LHsType GhcPs -> LHsExpr GhcPs
mkAppType expr typ = noL $ HsAppType noE expr (HsWC noE typ)

mkLam :: GHC.RdrName -> LHsExpr GhcPs -> LHsExpr GhcPs
mkLam v x = mkHsLam [VarPat noE $ noL v] x

-- | Are the end of a and the start of b next to each other, no white space
adjacent :: Located a -> Located b -> Bool
adjacent (L a _) (L b _) = isGoodSrcSpan a && srcSpanEnd a == srcSpanStart b

-- | This import was generated, not user written, so should not produce unused import warnings
importGenerated :: Bool -> ImportDecl phase -> ImportDecl phase
importGenerated qual i = i{ideclImplicit=True, ideclQualified=qual}

mkImport :: Located GHC.ModuleName -> ImportDecl GhcPs
mkImport mname = GHC.ImportDecl GHC.NoExt GHC.NoSourceText mname Nothing False False False False Nothing Nothing
