package com.plv.azbuka

import com.plv.azbuka.com.plv.bukva.Directory
import com.plv.azbuka.com.plv.bukva.Document
import com.plv.azbuka.IndexManager
import java.nio.file.Files
import java.nio.file.Paths

/*
 * Simple demo program, runnable from shell.
 * Arguments should be paths of directories and files you wish to issue queries for.
 */
fun main(args: Array<String>) {
  val manager = IndexManager()
  args.forEach {
    val path = Paths.get(it).toAbsolutePath()
    if (Files.isDirectory(path)) {
      manager.add(Directory(path), shouldTrack = true)
    } else {
      manager.add(Document(path), shouldTrack = true)
    }
  }

  do {
    print("query> ")
    val input = readLine()!!

    println(manager.search(input))
  } while(input != "exit")
}