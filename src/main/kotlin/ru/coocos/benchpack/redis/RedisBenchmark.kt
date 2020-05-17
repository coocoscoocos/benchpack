package ru.coocos.benchpack.redis

import java.io.File

object RedisBenchmark {

    @JvmStatic
    fun main(args: Array<String>) {
        val threads = Runtime.getRuntime().availableProcessors()
//        val mkdirCode = ProcessBuilder("mkdir", "benchmark").start().waitFor()
//        val wgetRedis = ProcessBuilder("wget", "http://download.redis.io/releases/redis-6.0.3.tar.gz").directory(File("benchmark")).start().waitFor()
//        val unpackRedis = ProcessBuilder("tar", "-xf", "redis-6.0.3.tar.gz").directory(File("benchmark")).start().waitFor()
//        val makeRedis = ProcessBuilder("make", "-j", threads.toString()).directory(File("benchmark/redis-6.0.3")).start().waitFor()

        //todo NUMA
        val serverProcessList = (1..(threads / 2)).map {
            val port = 8000 + it
            ProcessBuilder("./redis-server", "--port", port.toString()).directory(File("benchmark/redis-6.0.3/src")).start()
        }
        Thread.sleep(5000)
        val benchmarkProcessList = (1..(threads / 2)).map {
            val port = 8000 + it
            ProcessBuilder("./redis-benchmark", "-p", port.toString()).directory(File("benchmark/redis-6.0.3/src")).start()
        }
        var counter = 1
        benchmarkProcessList.forEach {
            it.waitFor()
            val file = File("benchmark", "result_" + counter++)
            it.inputStream.copyTo(file.outputStream())
        }

        serverProcessList.forEach { it.waitFor() }
    }

}