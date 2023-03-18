package com.nominanuda.emboot.examples.pipedfork

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@Disabled
class ChatWithBootIT {

    @Test
    fun chatWithBootTest() {
        assertTrue(File("target/piped-fork-0.0.1-SNAPSHOT.jar").exists())
        val javaHome = System.getProperty("java.home")
        val pBuilder = ProcessBuilder()
            .command("$javaHome/bin/java", "-jar", "target/piped-fork-0.0.1-SNAPSHOT.jar")
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        val process = pBuilder.start()
        val pIn = process.outputWriter(StandardCharsets.UTF_8)
        pIn.write("HELLO !!\n")
        pIn.close()
        if (! process.waitFor(60, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            while (process.isAlive) {
                println("ARRRRG")
                Thread.sleep(1000)
            }
        }
    }
}