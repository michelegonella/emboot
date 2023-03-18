package com.nominanuda.emboot.examples.pipedfork

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

class CheckBootJarExistsIT {

    @Test
    fun checkBootJarExistsTest() {
        assertTrue(File("target/piped-fork-0.0.1-SNAPSHOT.jar").exists())
    }
}