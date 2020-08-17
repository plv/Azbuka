package com.plv.azbuka


import com.plv.azbuka.com.plv.bukva.Directory
import com.plv.azbuka.com.plv.bukva.Document
import com.plv.azbuka.com.plv.bukva.Inode
import com.plv.azbuka.indexing.DocId
import java.io.FileNotFoundException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write


/*
* InodeStore provides a thread safe, atomic storage of documents and directories paths that we want to index.
* Each path has at most one entry. All Inodes are stored in a StoredInode<T : Inode>, which provides a unique id
* to each stored inode.
*
* While members do have an id, these ids are generated on insertion and are not guaranteed to stay constant with
* respect to a given path (for example: an Inode's id will change if it is added, removed, and added to InodeStore).
* However, ids are guaranteed to not be reused.
*/
class InodeStore {
  private val inodes: MutableMap<StoredInode.Identifier, StoredInode<out Inode>> = mutableMapOf()

  private val directoriesToDocuments: MutableMap<DocId, MutableList<DocId>> = mutableMapOf()

  private val idCounter: AtomicInteger = AtomicInteger(0)
  private val lock: ReentrantReadWriteLock = ReentrantReadWriteLock()

  // For use by IndexManager
  val readLock = lock.readLock()

  /*
   * Logical size of store (i.e. how many **unique** Inodes are stored)
   */
  val size: Int
    get() = inodes.values.toSet().size

  /* Querying */

  /*
   * Returns null if Inode can't be found in store, throws if found Inode cannot be refined to T
   */
  fun <T : Inode> get(id: Int): StoredInode<T>? =
    lock.read { this.inodes[StoredInode.Identifier.IntId(id)]?.reify() }

  fun <T : Inode> get(path: Path): StoredInode<T>? =
    lock.read { this.inodes[StoredInode.Identifier.PathId(path)]?.reify() }

  /*
   * Bulk, coarse-grained read: store will not change while we look up every id.
   */
  fun getAll(ids: Collection<DocId>): Collection<StoredInode<Inode>?> = lock.read { ids.map { get<Inode>(it) } }

  /* Insertion */

  /*
   * Inserts a Document into the InodeStore, and returns a wrapped StoredInode<Document>.
   *
   * If the same path already exists in the store, even with a different tokenizer, the existing stored Document will
   * be returned instead. If a parent Directory also lives in InodeStore, the newly-stored Document's id will be added
   * to that Directory's list of children (kept in `directoriesToDocuments`).
   *
   * Throws `FileNotfoundException` if the given Document contains a path that does not actually exist on disk.
   */
  fun add(doc: Document): StoredInode<Document> {
    if (!Files.exists(doc.path)) {
      throw FileNotFoundException()
    }

    lock.write {
      inodes[StoredInode.Identifier.PathId(doc.path)]?.let { return@add it.reify() }

      val id = idCounter.getAndIncrement()
      val stored = StoredInode(id, doc)
      // We hash by both id and path for easy lookup -- both are guaranteed to be unique(*).
      // (*) Paths are only unique at a given point in time. In other words, a path can refer to an inode
      // with a different id or tokenizer if it is removed from InodeStore and later re-added. But, at any
      // given time, two inodes in InodeStore cannot have the same path.
      inodes[StoredInode.Identifier.IntId(id)] = stored
      inodes[StoredInode.Identifier.PathId(stored.inode.path)] = stored

      get<Directory>(doc.path.parent)?.let {
        if (directoriesToDocuments[it.id] == null) {
          directoriesToDocuments[it.id] = mutableListOf(id)
        } else {
          directoriesToDocuments[it.id]!!.add(id)
        }
      }

      return stored
    }
  }

  /*
   * Inserts a Directory into the InodeStore, as well as inserting any children Inode belonging to the directory on disk.
   * Returns a Pair containing the now inserted StoredDirectory and a Set containing immediate (non-recursive) children.
   *
   * Behavior notes:
   *  - An already-existing child Inode will not be re-inserted and will not inherit this parent's new Tokenizer.
   *  - The returned set of children does **not** only include children that were added during this traversal. It
   *    includes previously-inserted children as well.
   */
  fun add(dir: Directory): Pair<StoredInode<in Directory>, Set<StoredInode<in Inode>>> {
    // It's possible that there are duplicates. For example, a file can be added, and now its parent is being added too.
    // Or, children of parentDir could have been added to the store while we are traversing it. We can filter the
    // duplicates out once we acquire the lock. For now, let's just create the inner `Inode`s, so that we avoid
    // needlessly incrementing idCounter.
    val childInodes: MutableSet<Inode> = mutableSetOf()

    dir.walkContents {
      preVisitDirectory {
        if (it != dir.path) {
          val subDirStored = add(Directory(it, tokenizer))
          // The subdirectory and its children are now stored, but we also want to associate the parent dir with its
          // immediate subdir.
          childInodes.add(subDirStored.first.reify<Directory>().inode)
        }
        // Skip because we already
        FileVisitResult.SKIP_SUBTREE
      }

      visitFile {
        if (it.fileName.toString().startsWith(".")) {
          return@visitFile FileVisitResult.CONTINUE
        }
        childInodes.add(Document(it))
        FileVisitResult.CONTINUE
      }
    }

    lock.write {
      val parent =  get<Directory>(dir.path) ?: {
        val storedParent = StoredInode(idCounter.getAndIncrement(),
          Directory(dir.path, dir.tokenizer)
        )
        inodes[StoredInode.Identifier.PathId(storedParent.inode.path)] = storedParent
        inodes[StoredInode.Identifier.IntId(storedParent.id)] = storedParent
        directoriesToDocuments[storedParent.id] = mutableListOf()
        storedParent
      }()

      // Remove duplicates, as individual files may have been added to the store during traversal
      val inodesToStore = childInodes
        .map {
          get(it.path) ?: StoredInode(
            idCounter.getAndIncrement(),
            when (it) {
              is Document -> Document(
                it.path,
                it.tokenizer
              )
              is Directory -> Directory(
                it.path,
                it.tokenizer
              )
            }
          )
        }

      inodesToStore.forEach {
        inodes[StoredInode.Identifier.PathId(it.inode.path)] = it
        inodes[StoredInode.Identifier.IntId(it.id)] = it

        directoriesToDocuments.getValue(parent.id).add(it.id)
      }

      val children = directoriesToDocuments[parent.id]!!.mapTo(mutableSetOf()) { get<Inode>(it)!! }
      return Pair(parent, children)
    }
  }

  /*
   * Bulk add: provides course grained, atomic insertion of many documents.
   */
  fun addAll(docs: Set<Document>): List<StoredInode<Document>> = lock.write { docs.map { add(it) } }
  fun addAll(dirs: Set<Directory>): Map<StoredInode<in Directory>, Set<StoredInode<in Inode>>> =
    lock.write { dirs.map { add(it) }.associateBy({ it.first }, { it.second }) }

  /* Removal */

  /*
   * Removes a directory and its recursive children from InodeStore.
   */
  fun remove(dir: StoredInode<Directory>): Directory? = lock.write {
    val inode = inodes.remove(StoredInode.Identifier.IntId(dir.id))
    inode?.also {
      inodes.remove(StoredInode.Identifier.PathId(it.inode.path))
    }?.also {
      directoriesToDocuments.remove(it.id)!!.forEach { childId ->
        val storedChild = get<Inode>(childId)
        when (storedChild?.inode) {
          is Document -> {
            remove(storedChild.reify<Document>())
          }
          is Directory -> {
            remove(storedChild.reify<Directory>())
          }
        }
      }
    }

    inode?.reify<Directory>()?.inode
  }

  /*
   * Removes a Document from InodeStore. If the Document's parent Directory is registered as well, the Document's id
   * will be removed from the Directory's list of children.
   */
  fun remove(doc: StoredInode<Document>): Document? = lock.write {
    val inode = inodes.remove(StoredInode.Identifier.IntId(doc.id))
    inode?.also {
        inodes.remove(StoredInode.Identifier.PathId(it.inode.path))
      }?.also {
        val parent = get<Directory>(it.inode.path.parent)
        if (parent != null) {
          directoriesToDocuments[parent.id]?.remove(it.id)
        }
      }?.reify<Document>()?.inode
  }

  /*
   * StoredInode is a wrapper around Inode to represent a stored Inode in InodeStore. It adds a unique int ID, which
   * is generated upon insertion to the index.
   *
   * StoredInode acts as a sort of "receipt" type: other APIs use it as a guarantee that, at some point, the wrapped
   * Inode's path was stored and validated in InodeStore.
   */
  data class StoredInode<T : Inode> internal constructor(val id: Int, val inode: T) {
    fun <U : Inode> reify(): StoredInode<U> = this as StoredInode<U>

    sealed class Identifier {
      data class IntId(val id: DocId) : StoredInode.Identifier()
      data class PathId(val id: Path) : StoredInode.Identifier()
    }
  }
}