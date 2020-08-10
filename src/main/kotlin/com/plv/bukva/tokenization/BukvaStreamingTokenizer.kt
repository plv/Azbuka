package com.plv.bukva.tokenization

import com.plv.bukva.Token
import java.io.File
import java.io.FileInputStream

interface BukvaStreamingTokenizer: BukvaTokenizer {
  override fun toTokens(path: File): Map<Token, List<Int>> {
    return toTokens(path.inputStream())
  }

  fun toTokens(path: FileInputStream): Map<Token, List<Int>>
}