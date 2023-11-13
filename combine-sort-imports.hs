{-# LANGUAGE RecordWildCards #-}
module Main where

import Data.Char (isSpace)
import Data.List (intercalate, isPrefixOf, isInfixOf, sortOn)

main :: IO ()
main = interact f

f :: String -> String
f = unlines . renderFileSource . parseAFile . lines

splitWhile :: (a -> Bool) -> [a] -> ([a], [a])
splitWhile pred xs = (takeWhile pred xs, dropWhile pred xs)

globImportBlock :: [String] -> (Maybe ImportBlock, [String])
globImportBlock [] = (Nothing, [])
globImportBlock allLines =
  let (notImports, rest) = splitWhile (not . isPrefixOf "import ") allLines
  in
  case rest of
    [] -> (Nothing, notImports)
    (importHead:rest') ->
      let (importMultiLines, rest'') = splitWhile (isPrefixOf " ") rest'
      in
      ( Just $ ImportBlock
          (filter (not . all isSpace) notImports)
          importHead
          importMultiLines
      , rest''
      )

globHeader :: [String] -> ([String], [String])
globHeader lines =
  let (before, rest) = splitWhile (not . isPrefixOf "module ") lines
      (header, rest') = splitWhile (not . isInfixOf "where") rest
      (whereLine, rest'') = splitWhile (not . isPrefixOf "import ") rest'
  in
  (before ++ header ++ whereLine, rest'')

globAllImportBlocks :: [String] -> ([ImportBlock], [String])
globAllImportBlocks lines =
  let (mbBlock, rest) = globImportBlock lines
  in
  case mbBlock of
    Nothing -> ([], rest)
    Just block ->
      let (remainingBlocks, rest') = globAllImportBlocks rest
      in
      (block : remainingBlocks, rest')

data FileSource = FileSource
  { untilHeader :: [String]
  , importBlocks :: [ImportBlock]
  , afterAnyImports :: [String]
  }
  deriving (Show, Eq, Ord)

data ImportBlock = ImportBlock
  { importBefore :: [String]
  , importHead :: String
  , importTrailing :: [String]
  }
  deriving (Show, Eq, Ord)

parseAFile :: [String] -> FileSource
parseAFile lines =
  let (header, rest) = globHeader lines
      (blocks, rest') = globAllImportBlocks rest
  in
  FileSource header blocks rest'

renderImportBlock :: ImportBlock -> [String]
renderImportBlock ImportBlock{..} =
  importBefore ++ [importHead] ++ importTrailing

renderFileSource :: FileSource -> [String]
renderFileSource FileSource{..} =
  concat
    [ untilHeader
    , foldMap renderImportBlock (sortOn importHead importBlocks)
    , [""]
    , dropWhile (all isSpace) afterAnyImports
    ]
