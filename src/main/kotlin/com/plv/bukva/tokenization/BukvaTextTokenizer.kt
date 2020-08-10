package com.plv.bukva.tokenization

import com.plv.bukva.BukvaFile
import com.plv.bukva.Token
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader

class BukvaTextTokenizer: BukvaStreamingTokenizer {
    override fun toTokens(path: FileInputStream): Map<Token, List<Int>> {
        val tokens : MutableMap<Token, MutableList<Int>> = mutableMapOf()
        val reader = BufferedReader(InputStreamReader(path))

        reader.use { path ->
            var offset = 0
            do {
                val line : String? = reader.readLine()
                line?.split(" ")?.forEach {
                    val tkn = Token(it)
                    if (tokens[tkn]?.add(offset) == null) {
                        tokens[tkn] = mutableListOf(offset)
                    }
                    offset++
                }
            } while(line != null)
        }
        return tokens
    }
}