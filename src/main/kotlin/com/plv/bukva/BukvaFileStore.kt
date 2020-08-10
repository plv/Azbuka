package com.plv.bukva

import com.plv.bukva.tokenization.BukvaTextTokenizer
import kotlinx.coroutines.*
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

// TODO: what do we do if individual files are moved?
// For now, we're going to assume that we only get directory input. If we get a directory, then we
// naively index and insert ENTRY_CREATE/ENTRY_MODIFY and delete ENTRY_DELETEs.
//
// 1) Not actually delete on deletes and just mark them as "excluded."
// This would be really nice, but it wastes memory and tightly couples the indexer + the searcher.
//
// 2) Find a native solution like inotify. However, it's not portable, and not guaranteed to have
// feature parity.
//
// 3) Keeping a checksum of each file, requiring all polled events to complete before transacting them,
// and then just checking for dupe checksums to cancel each-other out would be ideal.

class BukvaFileStore(private val index: BukvaIndex) {

  private val fileStore: MutableMap<WatchKey, Path> = mutableMapOf()

  // Sometimes, we would like to track files instead of directories. In this case, we can keep track
  // of parent paths that we register with WatchService and the file paths that we actually care about.
  private val parentDirsToFiles: MutableMap<Path, MutableSet<Path>> = mutableMapOf()
  private val watchService: WatchService = FileSystems.getDefault().newWatchService()

  private fun tokenizeForPath(key: WatchKey, context: Path): Map<Token, List<Int>> {
    val watchedPath = fileStore[key]?.resolve(context)
      ?: throw Exception("Event received for non-tracked path")
    return BukvaTextTokenizer().toTokens(watchedPath.toFile())
  }

  init {
    suspend fun processCreate(key: WatchKey, event: WatchEvent<*>) = runBlocking {
      val tokenization = async(Dispatchers.IO) { tokenizeForPath(key, event.context() as Path) }
      index.addTokenization(fileStore[key]!!, tokenization.await())
    }

    fun processDelete(key: WatchKey, event: WatchEvent<*>) {
      if (parentDirsToFiles.containsKey(fileStore[key])) { // This is a file
        index.removeFile(fileStore[key]!!)
        // TODO If we tracked that file but not its parent
        // TODO we tracked that file AND its parent
      } else { // Not a file
        Files.walkFileTree(fileStore[key]!!, object: SimpleFileVisitor<Path>() {
          override fun preVisitDirectory(subPath: Path, attrs: BasicFileAttributes): FileVisitResult {
            return FileVisitResult.CONTINUE
          }
        })
      }
    }

    fun processModify(key: WatchKey, event: WatchEvent<*>) = runBlocking {
      if (!parentDirsToFiles.containsKey(fileStore[key])) {
        // We try to register directories recursively, so we can ignore any modify events to directories.
        return@runBlocking
      }

      // TODO: This is super inefficient. At the end of the day, I should have a smarter way of doing this,
      // but it involves completely re-working how my index works.
      val tokenization = async(Dispatchers.IO) { tokenizeForPath(key, event.context() as Path) }
      index.removeFile(fileStore[key]!!)
      index.addTokenization(fileStore[key]!!, tokenization.await())
    }

    GlobalScope.launch(Dispatchers.Default) {
      fun processEvents(key: WatchKey, event: WatchEvent<*>) = runBlocking {
        when (event.kind()) {
          StandardWatchEventKinds.ENTRY_CREATE -> {
            processCreate(key, event)
          }
          StandardWatchEventKinds.ENTRY_DELETE -> {
            processDelete(key, event)
          }
          StandardWatchEventKinds.ENTRY_MODIFY -> {
            processModify(key, event)
          }
          // TODO There may be others on Linux(?)
          else -> throw IOException("Unrecognized WatchEventKind received: ${event.kind()}")
        }
      }

      while (isActive) {
        val key = watchService.poll()
        key?.pollEvents()?.forEach {processEvents(key, it)}
        key?.reset()
      }
    }
  }

  fun add(path: Path) {
    // TODO does fileStore need to be atomic/locked too?
    val path = path.toRealPath()
    if (path.toFile().isDirectory) {
      addDirectory(path)
    } else {
      addFile(path)
    }
  }

  private fun addFile(path: Path) {
    // WatchService does not allow tracking of individual files. So we watch the parent instead.
    path.parent.let {
      val key = it.register(
        watchService,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_DELETE,
        StandardWatchEventKinds.ENTRY_MODIFY
      )
      fileStore[key] = it
      if (parentDirsToFiles[it]?.add(path) == null) {
        parentDirsToFiles[it] = mutableSetOf(path)
      }
    }
  }

  private fun addDirectory(path: Path) {
    // WatchService does not return CREATE/DELETE events for children. Example: If 'foo/' is registered, and a file
    // is created under 'foo/bar/', then we will be given an ENTRY_MODIFY 'foo/bar/' event and nothing else. So we
    // recursively add all of the directories by default.
    // TODO add an option to do non-recursive or complex file traversal
    Files.walkFileTree(path, object: SimpleFileVisitor<Path>() {
      override fun preVisitDirectory(subPath: Path, attrs: BasicFileAttributes): FileVisitResult {
        val key = subPath.register(
          watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY
        )
        fileStore[key] = subPath
        return FileVisitResult.CONTINUE
      }
    })
  }

  // TODO add remove
  // but make sure that remove handles case where file was deleted already
  fun remove(path: Path) {
    // When directory is deleted its already removed

  }
}
