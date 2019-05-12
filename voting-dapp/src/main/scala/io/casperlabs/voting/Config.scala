package io.casperlabs.voting

import java.io.File

case class Config(grpcHost: String, voteWasmLocation: String)

object Config {
  private val resourceName: String = "config.txt"

  def fromResource(): Config = {
    val stream = getClass().getClassLoader().getResourceAsStream(resourceName)
    val lines  = scala.io.Source.fromInputStream(stream).getLines().map(_.trim).toVector
    val grpcHost = lines.find(_.startsWith("host")).fold("127.0.0.1") { line =>
      line.split('=').last.trim
    }
    val voteWasmLocation = lines.find(_.startsWith("wasm")).fold("/tmp/vote.wasm") { line =>
      line.split('=').last.trim
    }

    Config(grpcHost, voteWasmLocation)
  }
}
