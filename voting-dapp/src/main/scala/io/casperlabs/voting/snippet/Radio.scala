package io.casperlabs.voting.snippet

import net.liftweb.common._
import net.liftweb.util.Helpers._
import net.liftweb.http.SHtml
import net.liftweb.http.SHtml.ChoiceHolder

object SatoshiIdentity extends Enumeration {
  type SatoshiIdentity = Value

  val hal       = Value("Hal Finney")
  val nick      = Value("Nick Szabo")
  val dave      = Value("Dave Kleiman")
  val gov       = Value("PsyOP directed by the U.S. Government")
  val craig     = Value("Craig Wright")
  val none      = Value("None of the above")
  val neverKnow = Value("The world will never know")
}

object Radio {
  import SatoshiIdentity._
  val options: Seq[SatoshiIdentity] = SatoshiIdentity.values.toSeq
  val default: Box[SatoshiIdentity] = Empty
  val radio: ChoiceHolder[SatoshiIdentity] =
    SHtml.radioElem(options, default) { selected =>
      println("Choice: " + selected)
    }

  def render = ".options" #> radio.toForm
}
