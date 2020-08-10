package com.plv.bukva.tokenization

import com.plv.bukva.BukvaFile
import com.plv.bukva.Token
import java.io.File

interface BukvaTokenizer {
    fun toTokens(path: File): Map<Token, List<Int>>
}