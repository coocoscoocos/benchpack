package ru.coocos.benchpack.indexer

import java.io.File
import java.util.concurrent.LinkedBlockingQueue

class FileReaderThread(val queue : LinkedBlockingQueue<ByteArray>, val filename : String) : Thread() {

    override fun run() {
        File(filename).readLines()
    }
}