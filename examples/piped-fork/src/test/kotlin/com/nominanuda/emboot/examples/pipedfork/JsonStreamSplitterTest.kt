package com.nominanuda.emboot.examples.pipedfork

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.TokenBuffer
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.util.StreamUtils
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.io.StringWriter
import java.util.concurrent.TimeUnit

class JsonStreamSplitterTest {
    @Test
    fun testSplit() {
        val fac = JsonFactory()
        val p = fac.createNonBlockingByteArrayParser() as NonBlockingJsonParser
        val pis = PipedInputStream()
        val pos = PipedOutputStream(pis)
        val chunks = StreamUtils.copyToByteArray(javaClass.classLoader.getResourceAsStream("json-chunks.txt"))
        val sendSingleBytes = true
        Thread {
            Thread.sleep(5000)

            if (sendSingleBytes)
                chunks.asList().forEach {
                        pos.write(it.toInt())
                        Thread.sleep(5)
                    }
            else
                pos.write(chunks)


            pos.close()
            println("ENDOFOUTPUT")
        }.start()
        val log = LoggerFactory.getLogger(JsonSplitter::class.java)
        val c: (ByteArray) -> Unit = {
            log.info("{}", String(it))
        }
        val js = JsonSplitter(pis, c)
        val fut = js.loop()
        fut.get(100, TimeUnit.SECONDS)
println()
    }
}
