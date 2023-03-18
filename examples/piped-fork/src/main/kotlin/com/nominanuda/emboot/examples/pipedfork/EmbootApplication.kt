package com.nominanuda.emboot.examples.pipedfork

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class EmbootApplication

fun main(args: Array<String>) {
    val pIn = System.`in`
    pIn.transferTo(System.out)
    Thread.sleep(10_000)
    runApplication<EmbootApplication>(*args)
}
