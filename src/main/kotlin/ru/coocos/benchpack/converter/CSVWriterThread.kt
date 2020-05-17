package ru.coocos.benchpack.converter

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import ru.coocos.benchpack.indexer.Page
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class CSVWriterThread(val queue : LinkedBlockingQueue<Page>, val dir : String) : Thread() {

    var csvPrinter : CSVPrinter? = null
    var fileIndex = 0
    var pages = 0
    var isFinished = AtomicBoolean(false)

    override fun run() {
        nextFile()
        while (true) {
            val page = queue.poll(1, TimeUnit.SECONDS)
            if (null == page) {
                if (isFinished.get()) {
                    csvPrinter?.close()
                }
                continue;
            }
            if (++pages > 2_000) {
                pages = 0
                nextFile()
            }
            csvPrinter!!.printRecord(page.title, page.text)
        }
    }

    private fun getNextFilename() : String {
        fileIndex++
        return "$dir/part$fileIndex.csv"
    }

    private fun nextFile() {
        csvPrinter?.close(true)
        csvPrinter = CSVPrinter(BufferedWriter(FileWriter(getNextFilename()), 16 * 1024 * 1024), CSVFormat.RFC4180)
    }
}