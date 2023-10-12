// same script as demo.sc, but without the java-fx ivy imports. we can't use the same script as we can't
// guard the imports with an if statement in ammonite.
import $ivy.`org.scalafx::scalafx:17.0.1-R26`
val (testScript, loadJar, adjustPath) = sys.props.getOrElseUpdate("demo-test", "0") match {
  case "1" => (true, false, true)
  case "2" => (true, true, false)
  case "3" => (false, false, true)
  case _ => (false, true, false)
}

// load JAR into process using ammonites magic @ and interp.load.cp
import com.digitalasset.canton.version.ReleaseVersion
if(loadJar) {
  val version = ReleaseVersion.current.fullVersion
  val jarDir = os.Path(s"demo/lib/demo_2.13-${version}.jar", base=os.pwd)
  println(s"loading jar from ${jarDir}")
  interp.load.cp(jarDir)
}

@

import com.digitalasset.canton.demo.ReferenceDemoScript
ReferenceDemoScript.startup(adjustPath, testScript)
