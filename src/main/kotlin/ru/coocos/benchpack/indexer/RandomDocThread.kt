package ru.coocos.benchpack.indexer

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.StringField
import java.lang.StringBuilder
import java.util.concurrent.LinkedBlockingQueue
import kotlin.random.Random

class RandomDocThread (val queue : LinkedBlockingQueue<Document>) : Thread() {

    override fun run() {
        var counter = 0L
        val document = Document()
        document.add(StringField("title", genRandomString(), Field.Store.YES))
        document.add(StringField("text", genRandomText(), Field.Store.YES))
        document.add(StringField("thread", name, Field.Store.YES) )
        document.add(NumericDocValuesField("pos", counter++))
        (1..100_000_000).forEach {
            queue.put(document)
        }
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