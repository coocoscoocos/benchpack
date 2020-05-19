package ru.coocos.benchpack.indexer

import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriter
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class IndexerThread (val queue: LinkedBlockingQueue<Document>, val writer : IndexWriter) : Thread() {

    var isFinished = AtomicBoolean(false)

    override fun run() {
        var docList = arrayListOf<Document>()
        while (true) {
            val document = queue.poll(10, TimeUnit.MILLISECONDS)
            if (null == document) {
                if (isFinished.get()) {
                    writer.addDocuments(docList)
                    return
                }
                continue;
            }
            docList.add(document)
            if (docList.size >= 500) {
                writer.addDocuments(docList)
                docList = arrayListOf()
            }
        }
    }
}