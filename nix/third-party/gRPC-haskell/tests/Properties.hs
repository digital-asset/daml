import           Test.Tasty
import           GeneratedTests

main :: IO ()
main = defaultMain $ testGroup "GRPC Unit Tests" [ generatedTests ]
