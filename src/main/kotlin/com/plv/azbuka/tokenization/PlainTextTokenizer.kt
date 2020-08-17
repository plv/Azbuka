package com.plv.azbuka.tokenization

object PlainTextTokenizer : StreamingTokenizer {
  override fun toTokens(str: String): List<StringToken> {
    return str.split(" ")
      .filter { !it.isStopWord() }
      .map {
        val processedString = it
          .filter { c -> c.isLetterOrDigit() }
          .toLowerCase()
        StringToken(processedString)
      }
  }

  private fun String.isStopWord(): Boolean {
    // 10 most common words from https://en.wikipedia.org/wiki/Most_common_words_in_English
    return this in listOf(
      "the",
      "be",
      "to",
      "of",
      "and",
      "a",
      "in",
      "that",
      "have",
      "I"
    )
  }
}