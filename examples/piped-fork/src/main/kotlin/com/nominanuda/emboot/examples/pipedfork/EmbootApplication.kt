package com.nominanuda.emboot.examples.pipedfork

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.BufferedReader
import java.io.InputStreamReader

@SpringBootApplication
class EmbootApplication

fun main(args: Array<String>) {
    val pIn = System.`in`

    val inLines = BufferedReader(InputStreamReader(pIn)).lineSequence()
    Thread {
        inLines.forEach {
            println("<$it>")
        }
    }.start()

    //pIn.transferTo(System.out)
    Thread.sleep(10_000)
    runApplication<EmbootApplication>(*args)
}
