package com.plv.azbuka.tokenization

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Path

/*
 * Interface for tokenizers/parsers wishing to read files from an InputStream via a BufferedReader: this should allow
 * the reading of large files by not keeping everything in memory at once.
 */
interface StreamingTokenizer: Tokenizable {
  override fun toTokens(docPath: Path): Map<StringToken, List<Int>> {
    return toTokens(Files.newInputStream(docPath))
  }

  fun toTokens(docStream: InputStream): Map<StringToken, List<Int>> {
    val allTokens: MutableMap<StringToken, MutableList<Int>> = mutableMapOf()
    val reader = BufferedReader(InputStreamReader(docStream))

    reader.use { _ ->
      var offset = 0
      do {
        val line: String? = reader.readLine()
        line?.also {
          toTokens(line).forEach {
            val currOffset = offset++
            if (allTokens[it]?.add(currOffset) == null) {
              allTokens[it] = mutableListOf(currOffset)
            }
          }
        }
      } while (line != null)
    }

    return allTokens
  }
}