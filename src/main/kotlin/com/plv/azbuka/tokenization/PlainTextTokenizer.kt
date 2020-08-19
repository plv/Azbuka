package com.plv.azbuka.tokenization

object PlainTextTokenizer : StreamingTokenizer {
  override fun toTokens(str: String): List<StringToken> {
    return str
      .split(Regex("[^a-zA-Z\\d]"))
      .asSequence()
      .map { StringToken(it.toLowerCase()) }
      .filter { it.representation.isNotBlank() && !it.representation.isStopWord() }
      .toList()
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
      "i"
    )
  }
}