package ru.coocos.benchpack.redis

import java.io.File

object RedisBenchmark {

    val dir = "benchmark"
    val version = "6.0.3"
    val filename = "redis-$version.tar.gz"
    val cpuCount = Runtime.getRuntime().availableProcessors()
    val startPort = 8000

    @JvmStatic
    fun main(args: Array<String>) {
        val threads = System.getProperty("threads")?.toInt() ?: cpuCount / 2
        val isNuma = System.getProperty("numa")?.equals("true") ?: false
        println("Using $threads threads")
        if (isNuma) {
            println("NUMA enabled")
        } else {
            println("NUMA disabled")
        }
        prepare()
        println("Run")
        val serverProcessList = (1..threads).map {
            val port = startPort + it
            val core = it * 2 - 2
            val redisServerList = listOf("./redis-server", "--port", port.toString(), "--maxmemory", "2048mb")
            val tasksetList = listOf("taskset", "-c", core.toString())
            val cmd = if (isNuma) tasksetList + redisServerList else redisServerList
            ProcessBuilder(cmd).directory(File("benchmark/redis/src")).start()
        }
        Thread.sleep(5000)
        val benchmarkProcessList = (1..threads).map {
            val port = startPort + it
            val core = it * 2 - 1
            val redisBenchmarkList = arrayListOf("./redis-benchmark", "-p", port.toString(), "--csv", "-r", "10000000", "-n", "1000000", "-d", "100")
            val tasksetList = listOf("taskset", "-c", core.toString())
            val cmd = if (isNuma) tasksetList + redisBenchmarkList else redisBenchmarkList
            ProcessBuilder(cmd).directory(File("benchmark/redis/src")).start()
        }
        var counter = 1
        benchmarkProcessList.forEach {
            it.waitFor()
            val file = File("benchmark", "result_" + counter++)
            it.inputStream.copyTo(file.outputStream())
        }
        println("Done")

        serverProcessList.forEach { it.waitFor() }
    }

    private fun prepare() {
        print("Prepare... ")
        val directory = File(dir)
        if (!directory.exists()) {
            directory.mkdir()
        }
        if (!File("$dir/$filename").exists()) {
            val result = ProcessBuilder("wget", "http://download.redis.io/releases/$filename").directory(File(dir)).start().waitFor()
            if (0 != result) {
                throw Exception("Error download file $filename")
            }
        }
        val unpackRedis = ProcessBuilder("tar", "-xf", filename).directory(File(dir)).start().waitFor()
        File("$dir/redis-$version").renameTo(File("$dir/redis"))
        val makeRedis = ProcessBuilder("make", "-j", cpuCount.toString()).directory(File("$dir/redis")).start().waitFor()
        println("done")
    }

}