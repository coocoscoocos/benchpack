package ru.coocos.benchpack.indexer

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.index.IndexWriter
import java.lang.StringBuilder
import kotlin.random.Random

class RandomIndexerThread (val writer : IndexWriter) : Thread() {

    override fun run() {
        var docList = arrayListOf<Document>()
        val document = Document()
        document.add(StringField("title", genRandomString(), Field.Store.YES))
        document.add(StringField("text", genRandomText(), Field.Store.YES))
        (1..1_000_000).forEach {
            docList.add(document)
            if (docList.size >= 500) {
                writer.addDocuments(docList)
                docList = arrayListOf()
            }
        }
        writer.addDocuments(docList)
        writer.flush()
    }

    fun genRandomString(): String {
        val chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        var passWord = ""
        for (i in 0..Random.nextInt(3, 35)) {
            passWord += chars[Math.floor(Math.random() * chars.length).toInt()]
        }
        return passWord
    }

    fun genRandomText() : String {
        val stringBuilder = StringBuilder()
        (10..Random.nextInt(1000)).forEach {
            stringBuilder.append(genRandomString()).append(" ")
        }
        return stringBuilder.toString()
    }
}