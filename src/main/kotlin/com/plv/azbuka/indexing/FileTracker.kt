package com.plv.azbuka.indexing

import kotlinx.coroutines.*
import name.pachler.nio.file.*
import name.pachler.nio.file.FileSystems
import name.pachler.nio.file.WatchEvent
import name.pachler.nio.file.WatchKey
import name.pachler.nio.file.WatchService
import name.pachler.nio.file.ext.Bootstrapper
import name.pachler.nio.file.ext.ExtendedWatchEventKind
import java.nio.file.*
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes


/*
 *    File tracking service based on Java's WatchService. Has added functionality for filtering and tracking individual
 *    files. Has a simple event-based dispatching setting API.
 *
 *    TODO: Refactor FileTracker + integrate it with Inodes, so that it's not accessing raw paths.
 *    Loosely based on https://github.com/vishna/watchservice-ktx/
 */
class FileTracker(scope: CoroutineScope = GlobalScope, private val trackingDelayMs: Long = 500L) {
  private data class TrackedDirectory(val path: Path, val trackEntireDirectory: Boolean)

  /* From the purview of Java's WatchService, everything we're interested in is a directory. Here, we can keep track
   * of which directories we're tracking, and whether we should be tracking the entire directory or just some
   * of its children.
   */
  private val trackedDirectories: MutableMap<WatchKey, TrackedDirectory> = mutableMapOf()

  /*
   * In reality, sometimes we would like to track files instead of directories. In this case, we can keep track
   * of parent paths that we register with WatchService and the file paths that we actually care about.
   */
  private val parentDirsToFiles: MutableMap<Path, MutableSet<Path>> = mutableMapOf()

  private val watchService: WatchService = FileSystems.getDefault().newWatchService()
  private val eventDispatcher: MutableMap<WatchEvent.Kind<*>, eventFn> = mutableMapOf()

  /* Dispatcher setter methods */

  fun onCreate(fn: eventFn): FileTracker {
    eventDispatcher[StandardWatchEventKind.ENTRY_CREATE] = fn
    return this
  }

  fun onModify(fn: eventFn): FileTracker {
    eventDispatcher[StandardWatchEventKind.ENTRY_MODIFY] = fn
    return this
  }

  fun onDelete(fn: eventFn): FileTracker {
    eventDispatcher[StandardWatchEventKind.ENTRY_DELETE] = fn
    return this
  }

  /* Main event loop */
  init {
    scope.launch {
      fun processEvents(key: WatchKey?, event: WatchEvent<*>) {
        val directoryAssociatedWithEvent = trackedDirectories[key]!!
        val pathAffectedByEvent = if (event.context() == null) {
          // In cases where the directory itself is deleted, event.context()
          // will be null, and therefore the event's associated tracked directory
          // will be equal to its "affected" directory
          directoryAssociatedWithEvent.path
        } else {
          val relativeAffectedPath = Paths.get(event.context().toString())
          directoryAssociatedWithEvent.path.resolve(relativeAffectedPath)
        }

        if (pathAffectedByEvent.fileName.toString().startsWith(".")) {
          // We want to ignore anything starting with a ".", these are usually hidden or temporary files that aren't
          // meant to be indexed.
          return
        }

        when (event.kind()) {
          StandardWatchEventKind.ENTRY_CREATE -> {
            if (!directoryAssociatedWithEvent.trackEntireDirectory) {
              return
            } else if (Files.isDirectory(pathAffectedByEvent)) {
              trackDirectory(pathAffectedByEvent)
            }
            eventDispatcher[event.kind()]?.let { it(pathAffectedByEvent, directoryAssociatedWithEvent.path) }
          }

          StandardWatchEventKind.ENTRY_DELETE -> {
            if (parentDirsToFiles.containsKey(directoryAssociatedWithEvent.path)) {
              // If an individually tracked file is deleted, then we should un-track it.
              this@FileTracker.untrackFile(pathAffectedByEvent)
            }

            if (
              directoryAssociatedWithEvent.trackEntireDirectory
              && pathAffectedByEvent.equals(directoryAssociatedWithEvent.path)
            ) {
              // NOTE: this is not an `else if` because an individually tracked file can be tracked,
              // and later that file's parent dir can be tracked as well. In other words, a file belonging in
              // parentDirsToFiles does not mean that trackEntireDirectory will always be false.
              trackedDirectories.remove(key)
              key?.cancel()
            }
            eventDispatcher[event.kind()]?.let { it(pathAffectedByEvent, directoryAssociatedWithEvent.path) }
          }

          StandardWatchEventKind.ENTRY_MODIFY -> {
            if (Files.isDirectory(pathAffectedByEvent)) {
              // We're not interested in directory modify events, as we should be tracking the child
              // directory in question already.
              return
            }

            val parentIsTrackingIndividualFiles =
              parentDirsToFiles.containsKey(directoryAssociatedWithEvent.path)
            val parentIsTrackingAffectedFile =
              parentDirsToFiles[directoryAssociatedWithEvent.path]?.contains(pathAffectedByEvent)
            if (parentIsTrackingIndividualFiles && !(parentIsTrackingAffectedFile!!)) {
              // Filtering out events pertaining to siblings of individually tracked files
              return
            }

            eventDispatcher[event.kind()]?.let { it(pathAffectedByEvent, directoryAssociatedWithEvent.path) }
          }

          ExtendedWatchEventKind.ENTRY_RENAME_FROM,
          ExtendedWatchEventKind.ENTRY_RENAME_TO -> {
            // TODO: Implement ENTRY_RENAME events
          }
          ExtendedWatchEventKind.KEY_INVALID -> {
            // Key invalid = tracked directory delete
            trackedDirectories.remove(key)
            parentDirsToFiles.remove(pathAffectedByEvent)
          }
          else -> throw Exception("Unrecognized FileTracker event ${event.kind()}")
        }
      }

      while (isActive) {
        val key: WatchKey? = watchService.poll()
        key?.pollEvents()?.forEach {
          processEvents(key, it)
        }
        key?.reset()
        // We want to delay so that we have batches of events that we can transact together.
        delay(trackingDelayMs)
      }
    }
  }

  /* Public API + Helpers */

  fun track(path: Path) {
    val realPath = path.toRealPath()
    if (Files.isDirectory(realPath)) {
      trackDirectory(realPath)
    } else {
      trackFile(realPath)
    }
  }

  private fun trackFile(path: Path) {
    // WatchService does not allow tracking of individual files. So we watch the parent instead, and keep
    // a special mapping from parent to individually tracked child.
    path.parent.let {
      val key = Bootstrapper.newPath(it.toFile()).register(
        watchService,
        StandardWatchEventKind.ENTRY_CREATE,
        StandardWatchEventKind.ENTRY_DELETE,
        StandardWatchEventKind.ENTRY_MODIFY,
        ExtendedWatchEventKind.ENTRY_RENAME_FROM,
        ExtendedWatchEventKind.ENTRY_RENAME_TO
      )
      trackedDirectories[key] = TrackedDirectory(it, false)
      if (parentDirsToFiles[it]?.add(path) == null) {
        parentDirsToFiles[it] = mutableSetOf(path)
      }
    }
  }

  private fun trackDirectory(path: Path) {
    // WatchService does not return CREATE/DELETE events for children directories. Ex: If 'foo/' is registered, and a file
    // is created under 'foo/bar/', then we will be given an ENTRY_MODIFY 'foo/bar/' event and nothing else. So we
    // recursively add all of the directories by default.
    Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
      override fun preVisitDirectory(subPath: Path, attrs: BasicFileAttributes): FileVisitResult {
        val key = Bootstrapper.newPath(subPath.toFile()).register(
          watchService,
          StandardWatchEventKind.ENTRY_CREATE,
          StandardWatchEventKind.ENTRY_DELETE,
          StandardWatchEventKind.ENTRY_MODIFY,
          ExtendedWatchEventKind.ENTRY_RENAME_FROM,
          ExtendedWatchEventKind.ENTRY_RENAME_TO
        )
        trackedDirectories[key] = TrackedDirectory(subPath, true)
        return FileVisitResult.CONTINUE
      }
    })
  }

  fun untrack(path: Path) {
    val realPath = path.toRealPath()
    if (Files.isDirectory(realPath)) {
      untrackDirectory(realPath)
    } else {
      untrackFile(realPath)
    }
  }

  private fun untrackDirectory(path: Path) {
    if (!Files.exists(path)) {
      throw IllegalArgumentException("Directory at $path does not exist.")
    }

    parentDirsToFiles.remove(path)
  }

  private fun untrackFile(path: Path) {
    val filesTrackedByParent = parentDirsToFiles[path.parent]
    parentDirsToFiles[path.parent]?.remove(path.toAbsolutePath())

    if (
      filesTrackedByParent != null &&
      filesTrackedByParent.isEmpty() &&
      trackedDirectories.containsValue(TrackedDirectory(path.parent, false))
    ) {
      parentDirsToFiles.remove(path.parent)
    }
  }
}
typealias eventFn = (affectedPath: Path, associatedDir: Path) -> Unit
