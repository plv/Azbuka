package com.plv.bukva

import com.plv.bukva.tokenization.BukvaTextTokenizer
import java.nio.file.Path
import kotlin.streams.asStream

class BukvaIndex {
    // TODO change this data structure/abstract it out
    // This maps tokens -> files -> metadata (position, etc)
    private var index: MutableMap<
            Token,
            MutableMap<Path, List<Int>>
        > = mutableMapOf()


    // Adds and immediately indexes files
    fun addTokenization(file: Path, tokenization: Map<Token, List<Int>>) {
        // TODO select reader based on file
        tokenization.forEach {
            val (tkn, positions) = it
            if (!index.containsKey(tkn)) {
                index[tkn] = mutableMapOf(Pair(file, positions))
            } else {
                index[tkn]!![file] = positions
            }
        }
    }

    fun removeFile(file: Path) {
        index.forEach {
            val (tkn, usages) = it
            if (usages.containsKey(file)) {
                index[tkn]!!.remove(file)
            }
        }
    }

//    private fun addTokenUsageToFile(file: Path, tkn: Token): Boolean {
//        if (!index.containsKey(tkn)) {
//            // Let's only call this if we're indexing a file
//            // and we know that it exists already
//            return false
//        }
//
//        val tokenOccurrenceIndex = index[tkn]!!
//        if (tokenOccurrenceIndex[file]?.add(pos) == null) {
//            tokenOccurrenceIndex[file] = mutableListOf(pos)
//        }
//        return true
//    }

    fun searchKeyword(searchString: String): Set<Path> {
        // TODO (p0) take in some sort of abstract token
        // TODO (p0) refactor disgusting spaghetti code :D
        // TODO (p1) perhaps use parallel stream if num spaces > 2
        if (searchString.contains(' ')) {
            return searchString.split(' ').map { searchTerm ->
                // If any of our search terms aren't found then the entire query is false and we can return
                val idxResult = index[Token(searchTerm)] ?: return setOf()
                idxResult
            }.reduce { acc, mutableMap ->
                val iterator = acc.iterator()

                while (iterator.hasNext()) {
                    val entry = iterator.next()
                    val correspondingEntry = mutableMap[entry.key]
                    if (correspondingEntry == null) {
                        // We're not interested if the terms don't both show up in a file
                        iterator.remove()
                        continue
                    }

                    acc[entry.key] = entry.value.asSequence()
                        // If there are consecutively appearing entries in acc and correspondingEntry it means
                        // that the words appear consecutively
                        .filter { acc_position -> correspondingEntry!!.any {acc_position+1 == it} }
                        // We propagate this position until next time so that we can find the next token in the string the same way
                        .map { acc_position ->  acc_position+1}
                        .toMutableList()
                }
                acc
            }.keys
        } else {
            return index[Token(searchString)]?.keys ?: setOf()
        }
    }
}