package com.nominanuda.emboot.examples.pipedfork

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonTokenId
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.TokenBuffer
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

private const val BUF_SIZE = 1024

class JsonSplitter(
    val iStr: InputStream,
    val sink: Consumer<ByteArray>
) {
    val log = LoggerFactory.getLogger(JsonSplitter::class.java)
    val fac = JsonFactory()
    val p = fac.createNonBlockingByteArrayParser() as NonBlockingJsonParser
    fun loop() : CompletableFuture<Void> {
        val fut = CompletableFuture<Void>()
        val bailOut = AtomicBoolean(false)
        Thread {
            var barr = ByteArray(BUF_SIZE)
            while (true) {
                if (bailOut.get()) {
                    break
                }
                val nRead = iStr.read(barr)
                if (bailOut.get()) {
                    break
                }
                if (nRead < 0) {
                    log.debug("inputstream closed")
                    break;
                } else if (nRead > 0) {
                    while (! p.needMoreInput()) {
                        log.debug("parser not ready bytes going to sleep")
                        Thread.sleep(100)
                    }
                    p.feedInput(barr, 0, nRead)
                } else {
                    log.debug("got 0 bytes going to sleep")
                    Thread.sleep(1000)
                }
                barr = ByteArray(BUF_SIZE)
            }
            p.endOfInput()
            log.debug("END OF INPUT")
        }.start()
        Thread {
            var level  = -1
            var waitIngFirst = true
            var tokBuf = TokenBuffer(ObjectMapper(), false)
            var baos = ByteArrayOutputStream()
            var gen = fac.createGenerator(baos)
            while (!p.isClosed()) {
                if (bailOut.get()) {
                    break
                }
                val tok: JsonToken?  = p.nextToken()
                if (tok == null) {
                    log.debug("END OF PARSER")
                    break
                }
                if (tok.id() == JsonTokenId.ID_NOT_AVAILABLE) {
                    log.debug("NOT_AVAILABLE going to sleep")
                    Thread.sleep(1000)
                    continue;
                }
                tokBuf.copyCurrentEvent(p)
                if (waitIngFirst) {
                    if (tok.id() != JsonTokenId.ID_START_OBJECT) {
                        bailOut.set(true)
                        val ex = IllegalArgumentException("waiting for START_OBJECT but got ${tok.id()}")
                        log.error("exiting stdin reading loop", ex)
                        fut.completeExceptionally(ex)
                        break
                    }
                    waitIngFirst = false
                }
                if (tok.id() == JsonTokenId.ID_START_OBJECT) {
                    level++
                }
                if (tok.id() == JsonTokenId.ID_END_OBJECT) {
                    level--
                    if ( level < 0) {
                        tokBuf.serialize(gen)
                        //ge =
                        gen.close()
                        sink.accept(baos.toByteArray())
                        tokBuf = TokenBuffer(ObjectMapper(), false)
                        baos = ByteArrayOutputStream()
                        gen = fac.createGenerator(baos)
                        waitIngFirst = true
                    }
                }
            }
            if (! fut.isDone) {
                fut.complete(null)
            }
        }.start()
        return fut
    }
}