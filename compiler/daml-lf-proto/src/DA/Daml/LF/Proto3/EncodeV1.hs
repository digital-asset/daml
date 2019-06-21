-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV1
  ( encodeModule
  , encodePackage
  ) where

import           Control.Lens ((^.), matching)
import           Control.Lens.Ast (rightSpine)

import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V

import           DA.Pretty
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import qualified Da.DamlLf1 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- NOTE(MH): Type synonym for a `Maybe` that is always known to be a `Just`.
-- Some functions always return `Just x` instead of `x` since they would
-- otherwise always be wrapped in `Just` at their call sites.
type Just a = Maybe a

------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeList :: (a -> b) -> [a] -> V.Vector b
encodeList encodeElem = V.fromList . map encodeElem

encodeNameMap :: NM.Named a => (Version -> a -> b) -> Version -> NM.NameMap a -> V.Vector b
encodeNameMap encodeElem version = V.fromList . map (encodeElem version) . NM.toList

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

encodeQualTypeConName :: Qualified TypeConName -> Just P.TypeConName
encodeQualTypeConName (Qualified pref mname con) = Just $ P.TypeConName (encodeModuleRef pref mname) (encodeDottedName unTypeConName con)

encodeSourceLoc :: SourceLoc -> P.Location
encodeSourceLoc SourceLoc{..} =
    P.Location
      (uncurry encodeModuleRef =<< slocModuleRef)
      (Just (P.Location_Range
        (fromIntegral slocStartLine)
        (fromIntegral slocStartCol)
        (fromIntegral slocEndLine)
        (fromIntegral slocEndCol)))

encodePackageRef :: PackageRef -> Just P.PackageRef
encodePackageRef = Just . \case
    PRSelf -> P.PackageRef $ Just $ P.PackageRefSumSelf P.Unit
    PRImport pkgid -> P.PackageRef $ Just $ P.PackageRefSumPackageId $ encodePackageId pkgid

encodeModuleRef :: PackageRef -> ModuleName -> Just P.ModuleRef
encodeModuleRef pkgRef modName = Just $ P.ModuleRef (encodePackageRef pkgRef) (encodeDottedName unModuleName modName)

encodeFieldsWithTypes :: Version -> (a -> T.Text) -> [(a, Type)] -> V.Vector P.FieldWithType
encodeFieldsWithTypes version unwrapName =
    encodeList $ \(name, typ) -> P.FieldWithType (encodeName unwrapName name) (encodeType version typ)

encodeFieldsWithExprs :: Version -> (a -> T.Text) -> [(a, Expr)] -> V.Vector P.FieldWithExpr
encodeFieldsWithExprs version unwrapName =
    encodeList $ \(name, expr) -> P.FieldWithExpr (encodeName unwrapName name) (encodeExpr version expr)

encodeTypeVarsWithKinds :: Version -> [(TypeVarName, Kind)] -> V.Vector P.TypeVarWithKind
encodeTypeVarsWithKinds version =
    encodeList $ \(name, kind)  -> P.TypeVarWithKind (encodeName unTypeVarName name) (Just $ encodeKind version kind)

encodeExprVarWithType :: Version -> (ExprVarName, Type) -> P.VarWithType
encodeExprVarWithType version (name, typ) = P.VarWithType (encodeName unExprVarName name) (encodeType version typ)

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
    BTEnum et -> case et of
      ETUnit -> P.PrimTypeUNIT
      ETBool -> P.PrimTypeBOOL
    BTList -> P.PrimTypeLIST
    BTUpdate -> P.PrimTypeUPDATE
    BTScenario -> P.PrimTypeSCENARIO
    BTDate -> P.PrimTypeDATE
    BTContractId -> P.PrimTypeCONTRACT_ID
    BTOptional -> P.PrimTypeOPTIONAL
    BTMap -> P.PrimTypeMAP
    BTArrow -> P.PrimTypeARROW

encodeType' :: Version -> Type -> P.Type
encodeType' version typ = P.Type . Just $
    case typ ^. _TApps of
      (TVar var, args) ->
        P.TypeSumVar $ P.Type_Var (encodeName unTypeVarName var) (encodeTypes version args)
      (TCon con, args) ->
        P.TypeSumCon $ P.Type_Con (encodeQualTypeConName con) (encodeTypes version args)
      (TBuiltin bltn, args) ->
        P.TypeSumPrim $ P.Type_Prim (encodeBuiltinType version bltn) (encodeTypes version args)
      (t@TForall{}, []) ->
          let (binders, body) = t ^. _TForalls
          in P.TypeSumForall (P.Type_Forall (encodeTypeVarsWithKinds version binders) (encodeType version body))
      (TTuple flds, []) -> P.TypeSumTuple (P.Type_Tuple (encodeFieldsWithTypes version unFieldName flds))

      (TApp{}, _) -> error "TApp after unwinding TApp"
      -- NOTE(MH): The following case is ill-kinded.
      (TTuple{}, _:_) -> error "Application of TTuple"
      -- NOTE(MH): The following case requires impredicative polymorphism,
      -- which we don't support.
      (TForall{}, _:_) -> error "Application of TForall"

encodeType :: Version -> Type -> Just P.Type
encodeType version = Just . encodeType' version

encodeTypes :: Version -> [Type] -> V.Vector P.Type
encodeTypes = encodeList . encodeType'

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

encodeEnumCon :: EnumCon -> P.Enumerated P.PrimCon
encodeEnumCon = P.Enumerated . Right . \case
    ECUnit -> P.PrimConCON_UNIT
    ECFalse -> P.PrimConCON_FALSE
    ECTrue -> P.PrimConCON_TRUE

encodeTypeConApp :: Version -> TypeConApp -> Just P.Type_Con
encodeTypeConApp version (TypeConApp tycon args) = Just $ P.Type_Con (encodeQualTypeConName tycon) (encodeTypes version args)

encodeBuiltinExpr :: BuiltinExpr -> P.ExprSum
encodeBuiltinExpr = \case
    BEInt64 x -> lit $ P.PrimLitSumInt64 x
    BEDecimal dec -> lit $ P.PrimLitSumDecimal (TL.pack (show dec))
    BEText x -> lit $ P.PrimLitSumText (TL.fromStrict x)
    BETimestamp x -> lit $ P.PrimLitSumTimestamp x
    BEParty x -> lit $ P.PrimLitSumParty $ TL.fromStrict $ unPartyLiteral x
    BEDate x -> lit $ P.PrimLitSumDate x

    BEEnumCon con -> P.ExprSumPrimCon (encodeEnumCon con)

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTDecimal -> builtin P.BuiltinFunctionEQUAL_DECIMAL
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTEnum ETBool -> builtin P.BuiltinFunctionEQUAL_BOOL
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

encodeExpr' :: Version -> Expr -> P.Expr
encodeExpr' version = \case
  EVar v -> expr $ P.ExprSumVar (encodeName unExprVarName v)
  EVal (Qualified pkgRef modName val) -> expr $ P.ExprSumVal $ P.ValName (encodeModuleRef pkgRef modName) (encodeValueName val)
  EBuiltin bi -> expr $ encodeBuiltinExpr bi
  ERecCon{..} -> expr $ P.ExprSumRecCon $ P.Expr_RecCon (encodeTypeConApp version recTypeCon) (encodeFieldsWithExprs version unFieldName recFields)
  ERecProj{..} -> expr $ P.ExprSumRecProj $ P.Expr_RecProj (encodeTypeConApp version recTypeCon) (encodeName unFieldName recField) (encodeExpr version recExpr)
  ERecUpd{..} -> expr $ P.ExprSumRecUpd $ P.Expr_RecUpd (encodeTypeConApp version recTypeCon) (encodeName unFieldName recField) (encodeExpr version recExpr) (encodeExpr version recUpdate)
  EVariantCon{..} -> expr $ P.ExprSumVariantCon $ P.Expr_VariantCon (encodeTypeConApp version varTypeCon) (encodeName unVariantConName varVariant) (encodeExpr version varArg)
  ETupleCon{..} -> expr $ P.ExprSumTupleCon $ P.Expr_TupleCon (encodeFieldsWithExprs version unFieldName tupFields)
  ETupleProj{..} -> expr $ P.ExprSumTupleProj $ P.Expr_TupleProj (encodeName unFieldName tupField) (encodeExpr version tupExpr)
  ETupleUpd{..} -> expr $ P.ExprSumTupleUpd $ P.Expr_TupleUpd (encodeName unFieldName tupField) (encodeExpr version tupExpr) (encodeExpr version tupUpdate)
  e@ETmApp{} ->
      let (fun, args) = e ^. _ETmApps
      in expr $ P.ExprSumApp $ P.Expr_App (encodeExpr version fun) (encodeList (encodeExpr' version) args)
  e@ETyApp{} ->
      let (fun, args) = e ^. _ETyApps
      in expr $ P.ExprSumTyApp $ P.Expr_TyApp (encodeExpr version fun) (encodeTypes version args)
  e@ETmLam{} ->
      let (params, body) = e ^. _ETmLams
      in expr $ P.ExprSumAbs $ P.Expr_Abs (encodeList (encodeExprVarWithType version) params) (encodeExpr version body)
  e@ETyLam{} ->
      let (params, body) = e ^. _ETyLams
      in expr $ P.ExprSumTyAbs $ P.Expr_TyAbs (encodeTypeVarsWithKinds version params) (encodeExpr version body)
  ECase{..} -> expr $ P.ExprSumCase $ P.Case (encodeExpr version casScrutinee) (encodeList (encodeCaseAlternative version) casAlternatives)
  e@ELet{} ->
      let (lets, body) = e ^. _ELets
      in expr $ P.ExprSumLet $ encodeBlock version lets body
  ENil{..} -> expr $ P.ExprSumNil $ P.Expr_Nil (encodeType version nilType)
  ECons{..} ->
      let unwind e0 as = case matching _ECons e0 of
            Left e1 -> (e1, as)
            Right (typ, hd, tl)
              | typ /= consType -> error "internal error: unexpected mismatch in cons cell type"
              | otherwise -> unwind tl (hd:as)
          (ctail, cfront) = unwind consTail [consHead]
      in expr $ P.ExprSumCons $ P.Expr_Cons (encodeType version consType) (encodeList (encodeExpr' version) $ reverse cfront) (encodeExpr version ctail)
  EUpdate u -> expr $ P.ExprSumUpdate $ encodeUpdate version u
  EScenario s -> expr $ P.ExprSumScenario $ encodeScenario version s
  ELocation loc e ->
    let (P.Expr _ esum) = encodeExpr' version e
    in P.Expr (Just $ encodeSourceLoc loc) esum
  ENone typ -> expr (P.ExprSumOptionalNone (P.Expr_OptionalNone (encodeType version typ)))
  ESome typ body -> expr (P.ExprSumOptionalSome (P.Expr_OptionalSome (encodeType version typ) (encodeExpr version body)))
  where
    expr = P.Expr Nothing . Just

encodeExpr :: Version -> Expr -> Just P.Expr
encodeExpr version = Just . encodeExpr' version

encodeUpdate :: Version -> Update -> P.Update
encodeUpdate version = P.Update . Just . \case
    UPure{..} -> P.UpdateSumPure $ P.Pure (encodeType version pureType) (encodeExpr version pureExpr)
    e@UBind{} ->
      let (bindings, body) = EUpdate e ^. rightSpine (_EUpdate . _UBind)
      in P.UpdateSumBlock $ encodeBlock version bindings body
    UCreate{..} -> P.UpdateSumCreate $ P.Update_Create (encodeQualTypeConName creTemplate) (encodeExpr version creArg)
    UExercise{..} -> P.UpdateSumExercise $ P.Update_Exercise (encodeQualTypeConName exeTemplate) (encodeName unChoiceName exeChoice) (encodeExpr version exeContractId) (fmap (encodeExpr' version) exeActors) (encodeExpr version exeArg)
    UFetch{..} -> P.UpdateSumFetch $ P.Update_Fetch (encodeQualTypeConName fetTemplate) (encodeExpr version fetContractId)
    UGetTime -> P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> P.UpdateSumEmbedExpr $ P.Update_EmbedExpr (encodeType version typ) (encodeExpr version e)
    UFetchByKey rbk ->
       P.UpdateSumFetchByKey (encodeRetrieveByKey version rbk)
    ULookupByKey rbk ->
       P.UpdateSumLookupByKey (encodeRetrieveByKey version rbk)

encodeRetrieveByKey :: Version -> RetrieveByKey -> P.Update_RetrieveByKey
encodeRetrieveByKey version RetrieveByKey{..} = P.Update_RetrieveByKey
    (encodeQualTypeConName retrieveByKeyTemplate)
    (encodeExpr version retrieveByKeyKey)

encodeScenario :: Version -> Scenario -> P.Scenario
encodeScenario version = P.Scenario . Just . \case
    SPure{..} -> P.ScenarioSumPure $ P.Pure (encodeType version spureType) (encodeExpr version spureExpr)
    e@SBind{} ->
      let (bindings, body) = EScenario e ^. rightSpine (_EScenario . _SBind)
      in P.ScenarioSumBlock $ encodeBlock version bindings body
    SCommit{..} ->
      P.ScenarioSumCommit $ P.Scenario_Commit
        (encodeExpr version scommitParty)
        (encodeExpr version scommitExpr)
        (encodeType version scommitType)
    SMustFailAt{..} ->
      P.ScenarioSumMustFailAt $ P.Scenario_Commit
        (encodeExpr version smustFailAtParty)
        (encodeExpr version smustFailAtExpr)
        (encodeType version smustFailAtType)
    SPass{..} ->
      P.ScenarioSumPass (encodeExpr' version spassDelta)
    SGetTime -> P.ScenarioSumGetTime P.Unit
    SGetParty{..} ->
      P.ScenarioSumGetParty (encodeExpr' version sgetPartyName)
    SEmbedExpr typ e -> P.ScenarioSumEmbedExpr $ P.Scenario_EmbedExpr (encodeType version typ) (encodeExpr version e)

encodeBinding :: Version -> Binding -> P.Binding
encodeBinding version (Binding binder bound) =
    P.Binding (Just $ encodeExprVarWithType version binder) (encodeExpr version bound)

encodeBlock :: Version -> [Binding] -> Expr -> P.Block
encodeBlock version bindings body =
    P.Block (encodeList (encodeBinding version) bindings) (encodeExpr version body)

encodeCaseAlternative :: Version -> CaseAlternative -> P.CaseAlt
encodeCaseAlternative version CaseAlternative{..} =
    let pat = case altPattern of
          CPDefault     -> P.CaseAltSumDefault P.Unit
          CPVariant{..} -> P.CaseAltSumVariant $ P.CaseAlt_Variant (encodeQualTypeConName patTypeCon) (encodeName unVariantConName patVariant) (encodeName unExprVarName patBinder)
          CPEnumCon con -> P.CaseAltSumPrimCon (encodeEnumCon con)
          CPNil         -> P.CaseAltSumNil P.Unit
          CPCons{..}    -> P.CaseAltSumCons $ P.CaseAlt_Cons (encodeName unExprVarName patHeadBinder) (encodeName unExprVarName patTailBinder)
          CPNone        -> P.CaseAltSumOptionalNone P.Unit
          CPSome{..}    -> P.CaseAltSumOptionalSome $ P.CaseAlt_OptionalSome (encodeName unExprVarName patBodyBinder)
    in P.CaseAlt (Just pat) (encodeExpr version altExpr)

encodeDefDataType :: Version -> DefDataType -> P.DefDataType
encodeDefDataType version DefDataType{..} =
      P.DefDataType (encodeDottedName unTypeConName dataTypeCon) (encodeTypeVarsWithKinds version dataParams)
      (Just $ case dataCons of
        DataRecord fs -> P.DefDataTypeDataConsRecord $ P.DefDataType_Fields (encodeFieldsWithTypes version unFieldName fs)
        DataVariant fs -> P.DefDataTypeDataConsVariant $ P.DefDataType_Fields (encodeFieldsWithTypes version unVariantConName fs))
      (getIsSerializable dataSerializable)
      (encodeSourceLoc <$> dataLocation)

encodeDefValue :: Version -> DefValue -> P.DefValue
encodeDefValue version DefValue{..} =
    P.DefValue
      (Just (P.DefValue_NameWithType (encodeValueName (fst dvalBinder)) (encodeType version (snd dvalBinder))))
      (encodeExpr version dvalBody)
      (getHasNoPartyLiterals dvalNoPartyLiterals)
      (getIsTest dvalIsTest)
      (encodeSourceLoc <$> dvalLocation)

encodeTemplate :: Version -> Template -> P.DefTemplate
encodeTemplate version Template{..} =
    P.DefTemplate
    { P.defTemplateTycon = encodeDottedName unTypeConName tplTypeCon
    , P.defTemplateParam = encodeName unExprVarName tplParam
    , P.defTemplatePrecond = encodeExpr version tplPrecondition
    , P.defTemplateSignatories = encodeExpr version tplSignatories
    , P.defTemplateObservers = encodeExpr version tplObservers
    , P.defTemplateAgreement = encodeExpr version tplAgreement
    , P.defTemplateChoices = encodeNameMap encodeTemplateChoice version tplChoices
    , P.defTemplateLocation = encodeSourceLoc <$> tplLocation
    , P.defTemplateKey = fmap (encodeTemplateKey version) tplKey
    }

encodeTemplateKey :: Version -> TemplateKey -> P.DefTemplate_DefKey
encodeTemplateKey version TemplateKey{..} = P.DefTemplate_DefKey
  { P.defTemplate_DefKeyType = encodeType version tplKeyType
  , P.defTemplate_DefKeyKeyExpr =
          Just $ P.DefTemplate_DefKeyKeyExprComplexKey $ encodeExpr' version tplKeyBody
  , P.defTemplate_DefKeyMaintainers = encodeExpr version tplKeyMaintainers
  }


encodeTemplateChoice :: Version -> TemplateChoice -> P.TemplateChoice
encodeTemplateChoice version TemplateChoice{..} =
    P.TemplateChoice
    { P.templateChoiceName = encodeName unChoiceName chcName
    , P.templateChoiceConsuming = chcConsuming
    , P.templateChoiceControllers = encodeExpr version chcControllers
    , P.templateChoiceSelfBinder = encodeName unExprVarName chcSelfBinder
    , P.templateChoiceArgBinder = Just $ encodeExprVarWithType version chcArgBinder
    , P.templateChoiceRetType = encodeType version chcReturnType
    , P.templateChoiceUpdate = encodeExpr version chcUpdate
    , P.templateChoiceLocation = encodeSourceLoc <$> chcLocation
    }

encodeFeatureFlags :: Version -> FeatureFlags -> Just P.FeatureFlags
encodeFeatureFlags _version FeatureFlags{..} = Just P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = forbidPartyLiterals
    -- We only support packages with these enabled -- see #157
    , P.featureFlagsDontDivulgeContractIdsInCreateArguments = True
    , P.featureFlagsDontDiscloseNonConsumingChoicesToObservers = True
    }


encodeModule :: Version -> Module -> P.Module
encodeModule version Module{..} =
    P.Module
        (encodeDottedName unModuleName moduleName)
        (encodeFeatureFlags version moduleFeatureFlags)
        (encodeNameMap encodeDefDataType version moduleDataTypes)
        (encodeNameMap encodeDefValue version moduleValues)
        (encodeNameMap encodeTemplate version moduleTemplates)

-- | NOTE(MH): Assumes the DAML-LF version of the 'Package' is 'V1'.
encodePackage :: Package -> P.Package
encodePackage (Package version mods) = P.Package (encodeNameMap encodeModule version mods)


-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to DAML-LF.
_checkFeature :: Feature -> Version -> a -> a
_checkFeature feature version x
    | version `supports` feature = x
    | otherwise = error $ "DAML-LF " ++ renderPretty version ++ " cannot encode feature: " ++ T.unpack (featureName feature)
