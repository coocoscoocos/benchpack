package ru.coocos.benchpack.indexer

import java.io.BufferedInputStream
import java.io.FileInputStream
import java.util.concurrent.LinkedBlockingQueue
import javax.xml.stream.XMLInputFactory

class WikiReaderThread (val queue : LinkedBlockingQueue<Page>) : Thread() {

    override fun run() {
        System.setProperty("jdk.xml.totalEntitySizeLimit", Int.MAX_VALUE.toString());
        name = "WikiReader"
        val xmlInputFactory = XMLInputFactory.newInstance()
        val reader = xmlInputFactory.createXMLEventReader(BufferedInputStream(FileInputStream("/opt/enwiki-latest-pages-articles.xml"), 64 * 1024 * 1024))
        var title : String? = null
        var text : String? = null
        var currentElement : String? = null
        var pageCount = 0
        while (reader.hasNext()) {
            val nextEvent = reader.nextEvent()
            if (nextEvent.isStartElement) {
                val startElement = nextEvent.asStartElement()
                currentElement = startElement.name.localPart
                if (currentElement == "page") {
                    text = ""
                }
            } else if (nextEvent.isEndElement) {
                when (nextEvent.asEndElement().name.localPart) {
                    "page" -> {
                        queue.put(Page(title!!, text!!))
//                        if (pageCount++ > 200) {
//                            return
//                        }
                    }
                    else -> currentElement = null
                }
            } else if (nextEvent.isCharacters) {
                when (currentElement) {
                    "title" -> title = nextEvent.asCharacters().data
                    "text" -> text += nextEvent.asCharacters().data
                }
            }
        }
    }

//    private fun makeDoc(title : String, text : String) : Document {
//        val document = Document()
//        document.add(StringField("title", title, Field.Store.YES))
//        document.add(TextField("text", text, Field.Store.YES))
//        return document
//    }

}