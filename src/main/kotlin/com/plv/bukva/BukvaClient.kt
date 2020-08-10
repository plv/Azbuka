package com.plv.bukva

import java.nio.file.Path
import java.nio.file.Paths

class BukvaClient {

    val index: BukvaIndex = BukvaIndex()
    val store: BukvaFileStore = BukvaFileStore(index)

    fun addFilePath(newFile: String) {
        this.addFilePath(Paths.get(newFile))
    }

    fun addFilePath(newFile: Path) {
        store.add(newFile)
    }

    fun search(term: String): Set<Path> {
        return index.searchKeyword(term)
    }
}
