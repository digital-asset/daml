

// load dependencies first, so that we fail early if there is a dependency problem
import $ivy.`org.openjfx:javafx-controls:17-ea+8`
type test = javafx.scene.control.TableCell[String, String]
import $ivy.`org.openjfx:javafx-controls:17-ea+8`
import $ivy.`org.openjfx:javafx-base:17-ea+8`
import $ivy.`org.openjfx:javafx-fxml:17-ea+8`
import $ivy.`org.openjfx:javafx-media:17-ea+8`
import $ivy.`org.openjfx:javafx-web:17-ea+8`
import $ivy.`org.openjfx:javafx-graphics:17-ea+8`
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
