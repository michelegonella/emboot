package com.nominanuda.emboot.examples.pipedfork

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ser.std.TokenBufferSerializer
import com.fasterxml.jackson.databind.util.TokenBuffer
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.util.StreamUtils
import java.io.IOException
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class JsonStreamSplitterTest {
    @Test
    fun testSplit() {
        val fac = JsonFactory()
        val p = fac.createNonBlockingByteArrayParser() as NonBlockingJsonParser
        val pis = PipedInputStream()
        val pos = PipedOutputStream(pis)
        val chunks = StreamUtils.copyToByteArray(javaClass.classLoader.getResourceAsStream("json-chunks.txt"))
        Thread {
            chunks.asList().forEach {
                pos.write(it.toInt())
                Thread.sleep(1)
            }
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

    }
//    @Test
    fun testSplit2() {
        val fac = JsonFactory()
        val p = fac.createNonBlockingByteArrayParser() as NonBlockingJsonParser
        val pis = PipedInputStream()
        val pos = PipedOutputStream(pis)
        val chunks = StreamUtils.copyToByteArray(javaClass.classLoader.getResourceAsStream("json-chunks.txt"))
        Thread {
            chunks.asList().forEach {
                pos.write(it.toInt())
                Thread.sleep(1)
            }
            pos.close()
            println("ENDOFOUTPUT")
        }.start()
        Thread {
            var barr = ByteArray(128)
            while (true) {
                val nRead = pis.read(barr)
                if (nRead < 0) {
                    p.endOfInput()
                    break;
                } else if (nRead > 0) {
                    while (! p.needMoreInput()) {
                        Thread.sleep(100)
                    }
                    p.feedInput(barr, 0, nRead)
                } else {
                    println("WOW")
                    Thread.sleep(1000)
                }
                barr = ByteArray(128)
            }
            println("ENDOFINPUT")
        }.start()
        val packs = ArrayList<ArrayList<JsonToken>>()
        var pack = ArrayList<JsonToken>()
        var level  = -1
        var waitIngFirst = true
        var tokBuf = TokenBuffer(ObjectMapper(), false)

        var sw = StringWriter()
        var gen = fac.createGenerator(sw)
        while (!p.isClosed()) {
            val tok: JsonToken?  = p.nextToken()
            if (tok == null) {
                println("EOPARSER")
                break
            }
            if (tok.id() == JsonTokenId.ID_NOT_AVAILABLE) {
                Thread.sleep(100)
                continue;
            }
            tokBuf.copyCurrentEvent(p)


            if (waitIngFirst) {
                if (tok.id() != JsonTokenId.ID_START_OBJECT) {
                    throw IllegalArgumentException("waiting for START_OBJECT but got ${tok.id()}")
                }
                waitIngFirst = false
            }
            pack.add(tok)
            if (tok.id() == JsonTokenId.ID_START_OBJECT) {
                level++
            }
            if (tok.id() == JsonTokenId.ID_END_OBJECT) {
                level--
                if ( level < 0) {
                    tokBuf.serialize(gen)
                    //ge =
                    gen.close()
                    println(sw.toString())
                    tokBuf = TokenBuffer(ObjectMapper(), false)
                    sw = StringWriter()
                    gen = fac.createGenerator(sw)

                    packs.add(pack)
                    pack = ArrayList()
                    waitIngFirst = true
                    println("----------------")
                }
            }
            //println("$tok")
        }
        /*
        packs.forEach {
            println("=================================================================================")
            val gen = fac.createGenerator(System.err)

            it.forEach {
                print("${it.id()}-")
            }
            println("=================================================================================")
        }

         */
    }


}
