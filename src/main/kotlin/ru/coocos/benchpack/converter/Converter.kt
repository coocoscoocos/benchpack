package ru.coocos.benchpack.converter

import ru.coocos.benchpack.indexer.Page
import ru.coocos.benchpack.indexer.WikiReaderThread
import java.util.concurrent.LinkedBlockingQueue

object Converter {

    @JvmStatic
    fun main(args: Array<String>) {

        val queue = LinkedBlockingQueue<Page>(100_000)
        val reader = WikiReaderThread(queue)
        val startMs = System.currentTimeMillis()
        reader.start()
        val writer = CSVWriterThread(queue, "/opt/wiki_csv")
        writer.start()
        reader.join()
        writer.isFinished.set(true)
        writer.join()
        val endMs = System.currentTimeMillis()
        println(endMs - startMs)
    }
}