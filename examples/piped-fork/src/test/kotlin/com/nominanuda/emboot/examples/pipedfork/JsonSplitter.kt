package com.nominanuda.emboot.examples.pipedfork

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonTokenId.*
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.TokenBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.Exception
import java.lang.IllegalStateException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

private const val BUF_SIZE = 4096

private const val WAIT_FOR_INPUT_SLEEP = 1000L


class JsonSplitter(
    private val iStr: InputStream,
    private val sink: Consumer<ByteArray>
) {
    private val log: Logger = LoggerFactory.getLogger(JsonSplitter::class.java)
    private val jsonFactory = JsonFactory()
    private val jsonParser = jsonFactory.createNonBlockingByteArrayParser() as NonBlockingJsonParser
    private val resFut = CompletableFuture<Void>()
    val quitLoop = AtomicBoolean(false)
    private var looping = false
    //parser state
    private var objectNestingLevel  = -1
    private var waitingStartObject = true
    var tokBuf = tokenBuffer()

    fun loop() : CompletableFuture<Void> {
        if (looping) {
            throw IllegalStateException("already looping")
        }
        looping = true
        Thread {
            var inputBuf: ByteArray
            val bufsToFeed = ArrayList<Pair<ByteArray,Int>>()
            try {
                while (true) {
                    if (quitLoop.get()) {
                        break
                    }
                    inputBuf = ByteArray(BUF_SIZE)
                    val nRead = iStr.read(inputBuf)
                    if (quitLoop.get()) {
                        break
                    }
                    if (nRead < 0) {
                        log.debug("input closed")
                        quitLoop.set(true)
                        continue
                    } else if (nRead == 0) {
                        log.debug("got 0 bytes going to sleep")
                        Thread.sleep(WAIT_FOR_INPUT_SLEEP)
                    } else {//nRead > 0
                        bufsToFeed.add(Pair(inputBuf, nRead))
                        if (jsonParser.needMoreInput()) {
                            val (tot, bufToFeed) = concatAll(bufsToFeed)
                            bufsToFeed.clear()
                            jsonParser.feedInput(bufToFeed, 0, tot)
                        }
                        spinParserAndFeedOutput()
                    }
                }
                jsonParser.endOfInput()
                log.debug("END OF INPUT")
            }
            catch (e: Exception) {
                resFut.completeExceptionally(e)
                log.error("unexpected exception reading input", e)
            }
            finally {
                if (! resFut.isDone) {
                    resFut.complete(null)
                }
            }
        }.start()
        return resFut
    }

    private fun spinParserAndFeedOutput() {
        while (true) {
            val tok: JsonToken? = jsonParser.nextToken()
            if (tok == null) {
                val ex = IllegalStateException("nextToken() == null, invariant violated, bailing out input read loop")
                log.error("nextToken() == null", ex)
                resFut.completeExceptionally(ex)
                quitLoop.set(true)
                break
            }
            if (tok.id() == ID_NOT_AVAILABLE) {
                log.debug("nextToken() NOT_AVAILABLE")
                break
            }
            tokBuf.copyCurrentEvent(jsonParser)
            if (waitingStartObject) {
                if (tok.id() != ID_START_OBJECT) {
                    quitLoop.set(true)
                    val ex = IllegalArgumentException("waiting for START_OBJECT but got ${tok.id()}")
                    log.error("exiting stdin reading loop", ex)
                    resFut.completeExceptionally(ex)
                    quitLoop.set(true)
                    break
                }
                waitingStartObject = false
            }
            if (tok.id() == ID_START_OBJECT) {
                objectNestingLevel++
            }
            if (tok.id() == ID_END_OBJECT) {
                objectNestingLevel--
                if (objectNestingLevel < 0) {
                    val jsonObjectOutBytes = serializeTokenBuffer(tokBuf)
                    sink.accept(jsonObjectOutBytes)
                    tokBuf = tokenBuffer()
                    waitingStartObject = true
                }
            }
        }
    }


    private fun serializeTokenBuffer(tokBuf: TokenBuffer): ByteArray {
        val jsonOutBaos = ByteArrayOutputStream()
        val jacksonGen = jsonFactory.createGenerator(jsonOutBaos)
        tokBuf.serialize(jacksonGen)
        jacksonGen.close()
        val jsonObjectOutBytes = jsonOutBaos.toByteArray()
        return jsonObjectOutBytes
    }

    private fun concatAll(bufsToFeed: ArrayList<Pair<ByteArray, Int>>): Pair<Int, ByteArray> {
        val tot = bufsToFeed.map { it.second }.sum()
        val bufToFeed = ByteArray(tot)
        var soFar = 0
        bufsToFeed.forEach {
            try {
                System.arraycopy(it.first, 0, bufToFeed, soFar, it.second)
            } catch (e: Exception) {
                e.printStackTrace()
            }
            soFar += it.second
        }
        return Pair(tot, bufToFeed)
    }
    private val objectMapper = ObjectMapper()
    private fun tokenBuffer() = TokenBuffer(objectMapper, false)

}