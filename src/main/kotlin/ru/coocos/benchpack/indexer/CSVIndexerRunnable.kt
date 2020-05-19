package ru.coocos.benchpack.indexer

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import java.io.BufferedReader
import java.io.FileReader
import java.util.concurrent.Semaphore

class CSVIndexerRunnable(val writer : IndexWriter, val filename : String, val diskSemaphore : Semaphore) : Runnable {

    override fun run() {
        println("start $filename")
        diskSemaphore.acquire()
        val reader = BufferedReader(FileReader(filename), 64 * 1024 * 1024)
        val parser = CSVParser(reader, CSVFormat.RFC4180)
        val docList = arrayListOf<Document>()
        parser.forEach {
            val document = Document()
            document.add(StringField("title", it.get(0), Field.Store.YES))
            document.add(TextField("text", it.get(1), Field.Store.YES))
            docList.add(document)
        }
        parser.close()
        reader.close()
        diskSemaphore.release()
        writer.addDocuments(docList)
        println("done $filename")
    }
}