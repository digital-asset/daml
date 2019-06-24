-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV1
  ( encodeModuleWithLargePackageIds
  , encodePackage
  ) where

import           Control.Lens ((^.), (^..), matching)
import           Control.Lens.Ast (rightSpine)

import           Data.Word
import qualified Data.NameMap as NM
import qualified Data.Set as S
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V

import           DA.Pretty
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Ast.Optics (packageRefs)
import           DA.Daml.LF.Mangling
import qualified Da.DamlLf1 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- NOTE(MH): Type synonym for a `Maybe` that is always known to be a `Just`.
-- Some functions always return `Just x` instead of `x` since they would
-- otherwise always be wrapped in `Just` at their call sites.
type Just a = Maybe a

-- package-global state that encodePackageRef requires
type PackageRefCtx = PackageId -> Maybe Word64

data EncodeCtx = EncodeCtx {
   version :: Version
  ,interned :: PackageRefCtx
}

------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeList :: (a -> b) -> [a] -> V.Vector b
encodeList encodeElem = V.fromList . map encodeElem

encodeNameMap :: NM.Named a => (v -> a -> b) -> v -> NM.NameMap a -> V.Vector b
encodeNameMap encodeElem v = V.fromList . map (encodeElem v) . NM.toList

encodePackageId :: PackageId -> TL.Text
encodePackageId = TL.fromStrict . unPackageId

encodeName :: (a -> T.Text) -> a -> TL.Text
encodeName unwrapName (unwrapName -> unmangled) = case mangleIdentifier unmangled of
   Left err -> error $ "IMPOSSIBLE: could not mangle name " ++ show unmangled ++ ": " ++ err
   Right x -> TL.fromStrict x

-- | For now, value names are always encoded version using a single segment.
--
-- This is to keep backwards compat with older .dalf files, but also
-- because currently GenDALF generates weird names like `.` that we'd
-- have to handle separatedly. So for now, considering that we do not
-- use values in codegen, just mangle the entire thing.
encodeValueName :: ExprValName -> V.Vector TL.Text
encodeValueName = V.singleton . encodeName unExprValName

encodeDottedName :: (a -> [T.Text]) -> a -> Just P.DottedName
encodeDottedName unwrapDottedName = Just . P.DottedName . encodeList (encodeName id) . unwrapDottedName

encodeQualTypeConName :: PackageRefCtx -> Qualified TypeConName -> Just P.TypeConName
encodeQualTypeConName interned (Qualified pref mname con) = Just $ P.TypeConName (encodeModuleRef interned pref mname) (encodeDottedName unTypeConName con)

encodeSourceLoc :: PackageRefCtx -> SourceLoc -> P.Location
encodeSourceLoc interned SourceLoc{..} =
    P.Location
      (uncurry (encodeModuleRef interned) =<< slocModuleRef)
      (Just (P.Location_Range
        (fromIntegral slocStartLine)
        (fromIntegral slocStartCol)
        (fromIntegral slocEndLine)
        (fromIntegral slocEndCol)))

encodePackageRef :: PackageRefCtx -> PackageRef -> Just P.PackageRef
encodePackageRef interned = Just . \case
    PRSelf -> P.PackageRef $ Just $ P.PackageRefSumSelf P.Unit
    PRImport pkgid -> P.PackageRef $ Just $
      maybe (P.PackageRefSumPackageId $ encodePackageId pkgid)
            (P.PackageRefSumInternedId . fromIntegral)
            $ interned pkgid

internPackageRefIds :: Package -> (PackageRefCtx, [PackageId])
internPackageRefIds pkg
  | packageLfVersion pkg `supports` featureInternedPackageIds =
      let set = S.fromList $ pkg ^.. packageRefs._PRImport
          lookup pkgid = fromIntegral <$> pkgid `S.lookupIndex` set
      in (lookup, S.toAscList set)
  | otherwise = (const Nothing, [])

-- invariant: forall pkgid. pkgid `S.lookupIndex ` input = encodePackageId pkgid `V.elemIndex` output
encodeInternedPackageIds :: [PackageId] -> V.Vector TL.Text
encodeInternedPackageIds = encodeList encodePackageId

encodeModuleRef :: PackageRefCtx -> PackageRef -> ModuleName -> Just P.ModuleRef
encodeModuleRef ctx pkgRef modName =
  Just $ P.ModuleRef (encodePackageRef ctx pkgRef) (encodeDottedName unModuleName modName)

encodeFieldsWithTypes :: EncodeCtx -> (a -> T.Text) -> [(a, Type)] -> V.Vector P.FieldWithType
encodeFieldsWithTypes encctx unwrapName =
    encodeList $ \(name, typ) -> P.FieldWithType (encodeName unwrapName name) (encodeType encctx typ)

encodeFieldsWithExprs :: EncodeCtx -> (a -> T.Text) -> [(a, Expr)] -> V.Vector P.FieldWithExpr
encodeFieldsWithExprs encctx unwrapName =
    encodeList $ \(name, expr) -> P.FieldWithExpr (encodeName unwrapName name) (encodeExpr encctx expr)

encodeTypeVarsWithKinds :: Version -> [(TypeVarName, Kind)] -> V.Vector P.TypeVarWithKind
encodeTypeVarsWithKinds version =
    encodeList $ \(name, kind)  -> P.TypeVarWithKind (encodeName unTypeVarName name) (Just $ encodeKind version kind)

encodeExprVarWithType :: EncodeCtx -> (ExprVarName, Type) -> P.VarWithType
encodeExprVarWithType encctx (name, typ) = P.VarWithType (encodeName unExprVarName name) (encodeType encctx typ)

------------------------------------------------------------------------
-- Encoding of kinds
------------------------------------------------------------------------

encodeKind :: Version -> Kind -> P.Kind
encodeKind version = P.Kind . Just . \case
    KStar -> P.KindSumStar P.Unit
    k@KArrow{} ->
      let (params, result) = k ^. rightSpine _KArrow
      in P.KindSumArrow (P.Kind_Arrow (encodeList (encodeKind version) params) (Just $ encodeKind version result))

------------------------------------------------------------------------
-- Encoding of types
------------------------------------------------------------------------

encodeBuiltinType :: Version -> BuiltinType -> P.Enumerated P.PrimType
encodeBuiltinType _version = P.Enumerated . Right . \case
    BTInt64 -> P.PrimTypeINT64
    BTDecimal -> P.PrimTypeDECIMAL
    BTText -> P.PrimTypeTEXT
    BTTimestamp -> P.PrimTypeTIMESTAMP
    BTParty -> P.PrimTypePARTY
    BTUnit -> P.PrimTypeUNIT
    BTBool -> P.PrimTypeBOOL
    BTList -> P.PrimTypeLIST
    BTUpdate -> P.PrimTypeUPDATE
    BTScenario -> P.PrimTypeSCENARIO
    BTDate -> P.PrimTypeDATE
    BTContractId -> P.PrimTypeCONTRACT_ID
    BTOptional -> P.PrimTypeOPTIONAL
    BTMap -> P.PrimTypeMAP
    BTArrow -> P.PrimTypeARROW

encodeType' :: EncodeCtx -> Type -> P.Type
encodeType' encctx@EncodeCtx{..} typ = P.Type . Just $
    case typ ^. _TApps of
      (TVar var, args) ->
        P.TypeSumVar $ P.Type_Var (encodeName unTypeVarName var) (encodeTypes encctx args)
      (TCon con, args) ->
        P.TypeSumCon $ P.Type_Con (encodeQualTypeConName interned con) (encodeTypes encctx args)
      (TBuiltin bltn, args) ->
        P.TypeSumPrim $ P.Type_Prim (encodeBuiltinType version bltn) (encodeTypes encctx args)
      (t@TForall{}, []) ->
          let (binders, body) = t ^. _TForalls
          in P.TypeSumForall (P.Type_Forall (encodeTypeVarsWithKinds version binders) (encodeType encctx body))
      (TTuple flds, []) -> P.TypeSumTuple (P.Type_Tuple (encodeFieldsWithTypes encctx unFieldName flds))

      (TApp{}, _) -> error "TApp after unwinding TApp"
      -- NOTE(MH): The following case is ill-kinded.
      (TTuple{}, _:_) -> error "Application of TTuple"
      -- NOTE(MH): The following case requires impredicative polymorphism,
      -- which we don't support.
      (TForall{}, _:_) -> error "Application of TForall"

encodeType :: EncodeCtx -> Type -> Just P.Type
encodeType encctx = Just . encodeType' encctx

encodeTypes :: EncodeCtx -> [Type] -> V.Vector P.Type
encodeTypes = encodeList . encodeType'

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

encodeTypeConApp :: EncodeCtx -> TypeConApp -> Just P.Type_Con
encodeTypeConApp encctx@EncodeCtx{..} (TypeConApp tycon args) = Just $ P.Type_Con (encodeQualTypeConName interned tycon) (encodeTypes encctx args)

encodeBuiltinExpr :: BuiltinExpr -> P.ExprSum
encodeBuiltinExpr = \case
    BEInt64 x -> lit $ P.PrimLitSumInt64 x
    BEDecimal dec -> lit $ P.PrimLitSumDecimal (TL.pack (show dec))
    BEText x -> lit $ P.PrimLitSumText (TL.fromStrict x)
    BETimestamp x -> lit $ P.PrimLitSumTimestamp x
    BEParty x -> lit $ P.PrimLitSumParty $ TL.fromStrict $ unPartyLiteral x
    BEDate x -> lit $ P.PrimLitSumDate x

    BEUnit -> P.ExprSumPrimCon $ P.Enumerated $ Right P.PrimConCON_UNIT
    BEBool b -> P.ExprSumPrimCon $ P.Enumerated $ Right $ case b of
        False -> P.PrimConCON_FALSE
        True -> P.PrimConCON_TRUE

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTDecimal -> builtin P.BuiltinFunctionEQUAL_DECIMAL
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTBool -> builtin P.BuiltinFunctionEQUAL_BOOL
      other -> error $ "BEEqual unexpected type " <> show other

    BELessEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionLEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionLEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLEQ_DATE
      BTParty -> builtin P.BuiltinFunctionLEQ_PARTY
      other -> error $ "BELessEq unexpected type " <> show other

    BELess typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLESS_INT64
      BTDecimal -> builtin P.BuiltinFunctionLESS_DECIMAL
      BTText -> builtin P.BuiltinFunctionLESS_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLESS_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLESS_DATE
      BTParty -> builtin P.BuiltinFunctionLESS_PARTY
      other -> error $ "BELess unexpected type " <> show other

    BEGreaterEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionGEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionGEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGEQ_DATE
      BTParty -> builtin P.BuiltinFunctionGEQ_PARTY
      other -> error $ "BEGreaterEq unexpected type " <> show other

    BEGreater typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGREATER_INT64
      BTDecimal -> builtin P.BuiltinFunctionGREATER_DECIMAL
      BTText -> builtin P.BuiltinFunctionGREATER_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGREATER_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGREATER_DATE
      BTParty -> builtin P.BuiltinFunctionGREATER_PARTY
      other -> error $ "BEGreater unexpected type " <> show other

    BEToText typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionTO_TEXT_INT64
      BTDecimal -> builtin P.BuiltinFunctionTO_TEXT_DECIMAL
      BTText -> builtin P.BuiltinFunctionTO_TEXT_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionTO_TEXT_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionTO_TEXT_DATE
      BTParty -> builtin P.BuiltinFunctionTO_TEXT_PARTY
      other -> error $ "BEToText unexpected type " <> show other
    BETextFromCodePoints -> builtin P.BuiltinFunctionTEXT_FROM_CODE_POINTS
    BEPartyFromText -> builtin P.BuiltinFunctionFROM_TEXT_PARTY
    BEInt64FromText -> builtin P.BuiltinFunctionFROM_TEXT_INT64
    BEDecimalFromText-> builtin P.BuiltinFunctionFROM_TEXT_DECIMAL
    BETextToCodePoints -> builtin P.BuiltinFunctionTEXT_TO_CODE_POINTS
    BEPartyToQuotedText -> builtin P.BuiltinFunctionTO_QUOTED_TEXT_PARTY

    BEAddDecimal -> builtin P.BuiltinFunctionADD_DECIMAL
    BESubDecimal -> builtin P.BuiltinFunctionSUB_DECIMAL
    BEMulDecimal -> builtin P.BuiltinFunctionMUL_DECIMAL
    BEDivDecimal -> builtin P.BuiltinFunctionDIV_DECIMAL
    BERoundDecimal -> builtin P.BuiltinFunctionROUND_DECIMAL

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToDecimal -> builtin P.BuiltinFunctionINT64_TO_DECIMAL
    BEDecimalToInt64 -> builtin P.BuiltinFunctionDECIMAL_TO_INT64

    BEFoldl -> builtin P.BuiltinFunctionFOLDL
    BEFoldr -> builtin P.BuiltinFunctionFOLDR
    BEEqualList -> builtin P.BuiltinFunctionEQUAL_LIST
    BEExplodeText -> builtin P.BuiltinFunctionEXPLODE_TEXT
    BEAppendText -> builtin P.BuiltinFunctionAPPEND_TEXT
    BEImplodeText -> builtin P.BuiltinFunctionIMPLODE_TEXT
    BESha256Text -> builtin P.BuiltinFunctionSHA256_TEXT
    BEError -> builtin P.BuiltinFunctionERROR

    BEMapEmpty -> builtin P.BuiltinFunctionMAP_EMPTY
    BEMapInsert -> builtin P.BuiltinFunctionMAP_INSERT
    BEMapLookup -> builtin P.BuiltinFunctionMAP_LOOKUP
    BEMapDelete -> builtin P.BuiltinFunctionMAP_DELETE
    BEMapSize -> builtin P.BuiltinFunctionMAP_SIZE
    BEMapToList -> builtin P.BuiltinFunctionMAP_TO_LIST

    BETimestampToUnixMicroseconds -> builtin P.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS
    BEUnixMicrosecondsToTimestamp -> builtin P.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP

    BEDateToUnixDays -> builtin P.BuiltinFunctionDATE_TO_UNIX_DAYS
    BEUnixDaysToDate -> builtin P.BuiltinFunctionUNIX_DAYS_TO_DATE

    BETrace -> builtin P.BuiltinFunctionTRACE
    BEEqualContractId -> builtin P.BuiltinFunctionEQUAL_CONTRACT_ID
    BECoerceContractId -> builtin P.BuiltinFunctionCOERCE_CONTRACT_ID

    where
      builtin = P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumPrimLit . P.PrimLit . Just

encodeExpr' :: EncodeCtx -> Expr -> P.Expr
encodeExpr' encctx@EncodeCtx{..} = \case
  EVar v -> expr $ P.ExprSumVar (encodeName unExprVarName v)
  EVal (Qualified pkgRef modName val) -> expr $ P.ExprSumVal $ P.ValName (encodeModuleRef interned pkgRef modName) (encodeValueName val)
  EBuiltin bi -> expr $ encodeBuiltinExpr bi
  ERecCon{..} -> expr $ P.ExprSumRecCon $ P.Expr_RecCon (encodeTypeConApp encctx recTypeCon) (encodeFieldsWithExprs encctx unFieldName recFields)
  ERecProj{..} -> expr $ P.ExprSumRecProj $ P.Expr_RecProj (encodeTypeConApp encctx recTypeCon) (encodeName unFieldName recField) (encodeExpr encctx recExpr)
  ERecUpd{..} -> expr $ P.ExprSumRecUpd $ P.Expr_RecUpd (encodeTypeConApp encctx recTypeCon) (encodeName unFieldName recField) (encodeExpr encctx recExpr) (encodeExpr encctx recUpdate)
  EVariantCon{..} -> expr $ P.ExprSumVariantCon $ P.Expr_VariantCon (encodeTypeConApp encctx varTypeCon) (encodeName unVariantConName varVariant) (encodeExpr encctx varArg)
  ETupleCon{..} -> expr $ P.ExprSumTupleCon $ P.Expr_TupleCon (encodeFieldsWithExprs encctx unFieldName tupFields)
  ETupleProj{..} -> expr $ P.ExprSumTupleProj $ P.Expr_TupleProj (encodeName unFieldName tupField) (encodeExpr encctx tupExpr)
  ETupleUpd{..} -> expr $ P.ExprSumTupleUpd $ P.Expr_TupleUpd (encodeName unFieldName tupField) (encodeExpr encctx tupExpr) (encodeExpr encctx tupUpdate)
  e@ETmApp{} ->
      let (fun, args) = e ^. _ETmApps
      in expr $ P.ExprSumApp $ P.Expr_App (encodeExpr encctx fun) (encodeList (encodeExpr' encctx) args)
  e@ETyApp{} ->
      let (fun, args) = e ^. _ETyApps
      in expr $ P.ExprSumTyApp $ P.Expr_TyApp (encodeExpr encctx fun) (encodeTypes encctx args)
  e@ETmLam{} ->
      let (params, body) = e ^. _ETmLams
      in expr $ P.ExprSumAbs $ P.Expr_Abs (encodeList (encodeExprVarWithType encctx) params) (encodeExpr encctx body)
  e@ETyLam{} ->
      let (params, body) = e ^. _ETyLams
      in expr $ P.ExprSumTyAbs $ P.Expr_TyAbs (encodeTypeVarsWithKinds version params) (encodeExpr encctx body)
  ECase{..} -> expr $ P.ExprSumCase $ P.Case (encodeExpr encctx casScrutinee) (encodeList (encodeCaseAlternative encctx) casAlternatives)
  e@ELet{} ->
      let (lets, body) = e ^. _ELets
      in expr $ P.ExprSumLet $ encodeBlock encctx lets body
  ENil{..} -> expr $ P.ExprSumNil $ P.Expr_Nil (encodeType encctx nilType)
  ECons{..} ->
      let unwind e0 as = case matching _ECons e0 of
            Left e1 -> (e1, as)
            Right (typ, hd, tl)
              | typ /= consType -> error "internal error: unexpected mismatch in cons cell type"
              | otherwise -> unwind tl (hd:as)
          (ctail, cfront) = unwind consTail [consHead]
      in expr $ P.ExprSumCons $ P.Expr_Cons (encodeType encctx consType) (encodeList (encodeExpr' encctx) $ reverse cfront) (encodeExpr encctx ctail)
  EUpdate u -> expr $ P.ExprSumUpdate $ encodeUpdate encctx u
  EScenario s -> expr $ P.ExprSumScenario $ encodeScenario encctx s
  ELocation loc e ->
    let (P.Expr _ esum) = encodeExpr' encctx e
    in P.Expr (Just $ encodeSourceLoc interned loc) esum
  ENone typ -> expr (P.ExprSumNone (P.Expr_None (encodeType encctx typ)))
  ESome typ body -> expr (P.ExprSumSome (P.Expr_Some (encodeType encctx typ) (encodeExpr encctx body)))
  where
    expr = P.Expr Nothing . Just

encodeExpr :: EncodeCtx -> Expr -> Just P.Expr
encodeExpr encctx = Just . encodeExpr' encctx

encodeUpdate :: EncodeCtx -> Update -> P.Update
encodeUpdate encctx@EncodeCtx{..} = P.Update . Just . \case
    UPure{..} -> P.UpdateSumPure $ P.Pure (encodeType encctx pureType) (encodeExpr encctx pureExpr)
    e@UBind{} ->
      let (bindings, body) = EUpdate e ^. rightSpine (_EUpdate . _UBind)
      in P.UpdateSumBlock $ encodeBlock encctx bindings body
    UCreate{..} -> P.UpdateSumCreate $ P.Update_Create (encodeQualTypeConName interned creTemplate) (encodeExpr encctx creArg)
    UExercise{..} -> P.UpdateSumExercise $ P.Update_Exercise (encodeQualTypeConName interned exeTemplate) (encodeName unChoiceName exeChoice) (encodeExpr encctx exeContractId) (fmap (encodeExpr' encctx) exeActors) (encodeExpr encctx exeArg)
    UFetch{..} -> P.UpdateSumFetch $ P.Update_Fetch (encodeQualTypeConName interned fetTemplate) (encodeExpr encctx fetContractId)
    UGetTime -> P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> P.UpdateSumEmbedExpr $ P.Update_EmbedExpr (encodeType encctx typ) (encodeExpr encctx e)
    UFetchByKey rbk ->
       P.UpdateSumFetchByKey (encodeRetrieveByKey encctx rbk)
    ULookupByKey rbk ->
       P.UpdateSumLookupByKey (encodeRetrieveByKey encctx rbk)

encodeRetrieveByKey :: EncodeCtx -> RetrieveByKey -> P.Update_RetrieveByKey
encodeRetrieveByKey encctx@EncodeCtx{..} RetrieveByKey{..} = P.Update_RetrieveByKey
    (encodeQualTypeConName interned retrieveByKeyTemplate)
    (encodeExpr encctx retrieveByKeyKey)

encodeScenario :: EncodeCtx -> Scenario -> P.Scenario
encodeScenario encctx = P.Scenario . Just . \case
    SPure{..} -> P.ScenarioSumPure $ P.Pure (encodeType encctx spureType) (encodeExpr encctx spureExpr)
    e@SBind{} ->
      let (bindings, body) = EScenario e ^. rightSpine (_EScenario . _SBind)
      in P.ScenarioSumBlock $ encodeBlock encctx bindings body
    SCommit{..} ->
      P.ScenarioSumCommit $ P.Scenario_Commit
        (encodeExpr encctx scommitParty)
        (encodeExpr encctx scommitExpr)
        (encodeType encctx scommitType)
    SMustFailAt{..} ->
      P.ScenarioSumMustFailAt $ P.Scenario_Commit
        (encodeExpr encctx smustFailAtParty)
        (encodeExpr encctx smustFailAtExpr)
        (encodeType encctx smustFailAtType)
    SPass{..} ->
      P.ScenarioSumPass (encodeExpr' encctx spassDelta)
    SGetTime -> P.ScenarioSumGetTime P.Unit
    SGetParty{..} ->
      P.ScenarioSumGetParty (encodeExpr' encctx sgetPartyName)
    SEmbedExpr typ e -> P.ScenarioSumEmbedExpr $ P.Scenario_EmbedExpr (encodeType encctx typ) (encodeExpr encctx e)

encodeBinding :: EncodeCtx -> Binding -> P.Binding
encodeBinding encctx (Binding binder bound) =
    P.Binding (Just $ encodeExprVarWithType encctx binder) (encodeExpr encctx bound)

encodeBlock :: EncodeCtx -> [Binding] -> Expr -> P.Block
encodeBlock encctx bindings body =
    P.Block (encodeList (encodeBinding encctx) bindings) (encodeExpr encctx body)

encodeCaseAlternative :: EncodeCtx -> CaseAlternative -> P.CaseAlt
encodeCaseAlternative encctx@EncodeCtx{..} CaseAlternative{..} =
    let pat = case altPattern of
          CPDefault     -> P.CaseAltSumDefault P.Unit
          CPVariant{..} -> P.CaseAltSumVariant $ P.CaseAlt_Variant (encodeQualTypeConName interned patTypeCon) (encodeName unVariantConName patVariant) (encodeName unExprVarName patBinder)
          CPUnit -> P.CaseAltSumPrimCon $ P.Enumerated $ Right P.PrimConCON_UNIT
          CPBool b -> P.CaseAltSumPrimCon $ P.Enumerated $ Right $ case b of
            False -> P.PrimConCON_FALSE
            True -> P.PrimConCON_TRUE
          CPNil         -> P.CaseAltSumNil P.Unit
          CPCons{..}    -> P.CaseAltSumCons $ P.CaseAlt_Cons (encodeName unExprVarName patHeadBinder) (encodeName unExprVarName patTailBinder)
          CPNone        -> P.CaseAltSumNone P.Unit
          CPSome{..}    -> P.CaseAltSumSome $ P.CaseAlt_Some (encodeName unExprVarName patBodyBinder)
    in P.CaseAlt (Just pat) (encodeExpr encctx altExpr)

encodeDefDataType :: EncodeCtx -> DefDataType -> P.DefDataType
encodeDefDataType encctx@EncodeCtx{..} DefDataType{..} =
      P.DefDataType (encodeDottedName unTypeConName dataTypeCon) (encodeTypeVarsWithKinds version dataParams)
      (Just $ case dataCons of
        DataRecord fs -> P.DefDataTypeDataConsRecord $ P.DefDataType_Fields (encodeFieldsWithTypes encctx unFieldName fs)
        DataVariant fs -> P.DefDataTypeDataConsVariant $ P.DefDataType_Fields (encodeFieldsWithTypes encctx unVariantConName fs))
      (getIsSerializable dataSerializable)
      (encodeSourceLoc interned <$> dataLocation)

encodeDefValue :: EncodeCtx -> DefValue -> P.DefValue
encodeDefValue encctx@EncodeCtx{..} DefValue{..} =
    P.DefValue
      (Just (P.DefValue_NameWithType (encodeValueName (fst dvalBinder)) (encodeType encctx (snd dvalBinder))))
      (encodeExpr encctx dvalBody)
      (getHasNoPartyLiterals dvalNoPartyLiterals)
      (getIsTest dvalIsTest)
      (encodeSourceLoc interned <$> dvalLocation)

encodeTemplate :: EncodeCtx -> Template -> P.DefTemplate
encodeTemplate encctx@EncodeCtx{..} Template{..} =
    P.DefTemplate
    { P.defTemplateTycon = encodeDottedName unTypeConName tplTypeCon
    , P.defTemplateParam = encodeName unExprVarName tplParam
    , P.defTemplatePrecond = encodeExpr encctx tplPrecondition
    , P.defTemplateSignatories = encodeExpr encctx tplSignatories
    , P.defTemplateObservers = encodeExpr encctx tplObservers
    , P.defTemplateAgreement = encodeExpr encctx tplAgreement
    , P.defTemplateChoices = encodeNameMap encodeTemplateChoice encctx tplChoices
    , P.defTemplateLocation = encodeSourceLoc interned <$> tplLocation
    , P.defTemplateKey = fmap (encodeTemplateKey encctx) tplKey
    }

encodeTemplateKey :: EncodeCtx -> TemplateKey -> P.DefTemplate_DefKey
encodeTemplateKey encctx TemplateKey{..} = P.DefTemplate_DefKey
  { P.defTemplate_DefKeyType = encodeType encctx tplKeyType
  , P.defTemplate_DefKeyKeyExpr =
          Just $ P.DefTemplate_DefKeyKeyExprComplexKey $ encodeExpr' encctx tplKeyBody
  , P.defTemplate_DefKeyMaintainers = encodeExpr encctx tplKeyMaintainers
  }


encodeTemplateChoice :: EncodeCtx -> TemplateChoice -> P.TemplateChoice
encodeTemplateChoice encctx@EncodeCtx{..} TemplateChoice{..} =
    P.TemplateChoice
    { P.templateChoiceName = encodeName unChoiceName chcName
    , P.templateChoiceConsuming = chcConsuming
    , P.templateChoiceControllers = encodeExpr encctx chcControllers
    , P.templateChoiceSelfBinder = encodeName unExprVarName chcSelfBinder
    , P.templateChoiceArgBinder = Just $ encodeExprVarWithType encctx chcArgBinder
    , P.templateChoiceRetType = encodeType encctx chcReturnType
    , P.templateChoiceUpdate = encodeExpr encctx chcUpdate
    , P.templateChoiceLocation = encodeSourceLoc interned <$> chcLocation
    }

encodeFeatureFlags :: Version -> FeatureFlags -> Just P.FeatureFlags
encodeFeatureFlags _version FeatureFlags{..} = Just P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = forbidPartyLiterals
    -- We only support packages with these enabled -- see #157
    , P.featureFlagsDontDivulgeContractIdsInCreateArguments = True
    , P.featureFlagsDontDiscloseNonConsumingChoicesToObservers = True
    }

encodeModuleWithLargePackageIds :: Version -> Module -> P.Module
encodeModuleWithLargePackageIds = encodeModule . flip EncodeCtx (const Nothing)

encodeModule :: EncodeCtx -> Module -> P.Module
encodeModule encctx@EncodeCtx{..} Module{..} =
    P.Module
        (encodeDottedName unModuleName moduleName)
        (encodeFeatureFlags version moduleFeatureFlags)
        (encodeNameMap encodeDefDataType encctx moduleDataTypes)
        (encodeNameMap encodeDefValue encctx moduleValues)
        (encodeNameMap encodeTemplate encctx moduleTemplates)

-- | NOTE(MH): Assumes the DAML-LF version of the 'Package' is 'V1'.
encodePackage :: Package -> P.Package
encodePackage pkg@(Package version mods) =
    P.Package (encodeNameMap encodeModule (EncodeCtx version interned) mods)
              (encodeInternedPackageIds internedList)
  where (interned, internedList) = internPackageRefIds pkg


-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to DAML-LF.
_checkFeature :: Feature -> Version -> a -> a
_checkFeature feature version x
    | version `supports` feature = x
    | otherwise = error $ "DAML-LF " ++ renderPretty version ++ " cannot encode feature: " ++ T.unpack (featureName feature)
