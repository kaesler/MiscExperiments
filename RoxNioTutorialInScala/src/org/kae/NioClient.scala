package org.kae

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.nio.ByteBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Queue => MQueue}
import scala.parallel.Future

/** Class for an NIO client.
  */
class NioClient(
  // The host:port on which to listen:
  hostAddress: InetAddress, port: Int)
{
  /** Commands for changing the selector's behaviour.
    */
  sealed trait SelectionChangeRequest
  case class RegisterChannel(socket: SocketChannel, ops: Int) extends SelectionChangeRequest
  case class ChangeOps(socket: SocketChannel, ops: Int) extends SelectionChangeRequest
  case object Stop extends SelectionChangeRequest

  /** Trait for receiving responses.
    */
  trait ResponseHandler {
    // Response content.
    var data: Option[Array[Byte]] = None

    // Returns true when has seen enough
    def handleResponse(data_ : Array[Byte]): Boolean = {
      this.synchronized[Boolean] {
        data = Some(data_)
        notify
        // Signal we have all we want
        data.size > 0
      }
    }

    def waitForResponse = {
      synchronized {
        while (data.isEmpty)
          try {
            wait
          } catch {
            case ex: InterruptedException => // Ignore
          }
        println("Response received: " + new String(data.get))
      }
    }
  }

  // Selector to receive events.
  private val selector = SelectorProvider.provider.openSelector

  // Buffer into which we read data.
  private val readBuffer = ByteBuffer.allocate(8*1024)

  // Requests to change selector behaviour.
  private val changeRequests = MQueue[SelectionChangeRequest]()

  // Per-channel queues of data to be written.
  private val pendingData = MMap[SocketChannel, MQueue[ByteBuffer]]()

  private def initiateConnection: SocketChannel = {
    val socketChannel = SocketChannel.open()
    socketChannel.configureBlocking(false)

    // Initiate connection establishment
    socketChannel.connect(new InetSocketAddress(hostAddress, port))

    // Queue a channel registration
    changeRequests.synchronized {
      changeRequests enqueue RegisterChannel(socketChannel, SelectionKey.OP_CONNECT)
    }

    return socketChannel
  }

  private def finishConnection(key: SelectionKey) = {
    val socketChannel = key.channel.asInstanceOf[SocketChannel]

    // Finish the connection. If the connection operation failed
    // this will raise IOException.

    var connected = false
    try {
      connected = socketChannel.finishConnect
    } catch {
      case ex: IOException => key.cancel
    }

    if (connected)
      key.interestOps(SelectionKey.OP_WRITE)
  }

  private def read(key: SelectionKey) = {

    val socketChannel = key.channel.asInstanceOf[SocketChannel]

    // Prepare the buffer.
    readBuffer.clear

    var bytesRead = 0
    try {
      bytesRead = socketChannel.read(readBuffer)
    } catch {
      case ex: IOException => {
        // Interpret read exception as the remote end closing the connection.
        key.cancel
        socketChannel.close
      }
    }

    if (bytesRead == -1) {
      // Interpret EOF as the remote end closing the connection.
      key.cancel
      socketChannel.close
    } else {
      // We have data to consume. Pass it to handler.
      handleResponse(socketChannel, readBuffer.array, bytesRead)
    }
  }

  private def handleResponse(socketChannel: SocketChannel,
                             data: Array[Byte],
                             byteCount: Int) = {
    // Make a correctly sized copy of the data before handing it to the client.
    val dataCopy = new Array[Byte](byteCount)
    Array.copy(data, 0, dataCopy, 0, byteCount)

    if (futures(socketChannel).completeResponse(dataCopy)) {
      socketChannel.close
      socketChannel.keyFor(selector).cancel
    }
  }

  private def write(key: SelectionKey) = {

    val socketChannel = key.channel.asInstanceOf[SocketChannel]

    pendingData.synchronized {
      val queue =
        pendingData.getOrElseUpdate(socketChannel, MQueue[ByteBuffer]())

      // Write until there's no more we can write.
      var writeIncomplete = false
      while (!queue.isEmpty && !writeIncomplete) {
        val buf = queue.head
        socketChannel.write(buf)
        if (buf.remaining == 0)
          queue.dequeue
        else
          // We could not write all this buffer so leave it on the queue
          // but indicate we can't write any more.
          writeIncomplete = true
      }

      if (queue.isEmpty)
        // We've written all the data that needed writing.
        // Now indicate interest in reading only.
        key.interestOps(SelectionKey.OP_READ)
    }
  }

  /** Enqueue data for writing to the socket.
    */
  val futures = MMap[SocketChannel, ResponseFuture]()
  class ResponseFuture extends Future[String]
  {
    // Response content.
    private var data: Option[Array[Byte]] = None

    // Returns true when has seen enough
    def completeResponse(data_ : Array[Byte]): Boolean = {
      synchronized[Boolean] {
        data = Some(data_)
        notify
        true
      }
    }

    override def apply = {
      synchronized {
        while (data.isEmpty)
          try {
            wait
          } catch {
            case ex: InterruptedException => // Ignore
          }
        new String(data.get)
      }
    }

    override def isDone: Boolean = {
      synchronized[Boolean] {
        !data.isEmpty
      }
    }
  }

  def sendExpectingResponse(data:Array[Byte]): Future[String] = {
    val socketChannel = initiateConnection
    val result = new ResponseFuture
    futures(socketChannel) = result

    // Queue the data to be written
    pendingData.synchronized {
      val queue = pendingData.getOrElseUpdate(socketChannel, MQueue[ByteBuffer]())
      queue enqueue ByteBuffer.wrap(data)
    }

    // Wake the selecting thread to act on our request.
    selector.wakeup

    // Return the future response
    result
  }

  private var thread: Option[Thread] = None

  /** Start the selector thread.
    */
  def start() = {
    synchronized {
      val t = new Thread(new Runnable {
        override def run = {
          mainLoop
        }
      })
      t.setDaemon(true)
      thread = Some(t)
      t.start()
    }
  }

  def stop() = {
    changeRequests.synchronized {
      changeRequests enqueue Stop
    }
    selector.wakeup
  }

  private def mainLoop: Unit = {
    while (true) {
      try {
        // Dequeue and process any pending changes.
        changeRequests.synchronized {
          while (!changeRequests.isEmpty) {

            changeRequests.dequeue match {

              case Stop => return

              case ChangeOps(socketChannel, ops) =>
                println("Client selector thread: processing ChangeOps command")
                val key = socketChannel.keyFor(selector)
                key.interestOps(ops)

              case RegisterChannel(socketChannel, ops) =>
                println("Client selector thread: processing RegisterChannel command")
                socketChannel.register(selector, ops)

              case _ => sys.error("Can't happen")
            }
          }
        }

        // Wait for an event on one of the registered channels.
        println("Client selector thread: sleeping")
        selector.select
        println("Client selector thread: waking")

        // For each key for which events are available
        selector.selectedKeys.foreach { key =>

          selector.selectedKeys.remove(key)
          if (key.isValid) {
            if (key.isConnectable()) {
              println("Client selector thread: processing connectable")
              finishConnection(key)
            }
            else if (key.isReadable) {
              println("Client selector thread: processing readable")
              read(key)
            }
            else if (key.isWritable) {
              println("Client selector thread: processing writeable")
              write(key)
            }
          }
        }

      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
  }
}

/** Application to run the client.
  */
object NioClientApp extends App
{
  // Start the client selector thread.
  val client = new NioClient(InetAddress.getByName("localhost"), 9090)
  try {
    client.start()

    // Send some data and wait for a response.
    val f1 = client.sendExpectingResponse("Hello world".getBytes)
    println(f1())

    // Send some data and wait for a response.
    val f2 = client.sendExpectingResponse("Hello mundo".getBytes)
    println(f2())
  } catch {
    case ex: Exception => ex.printStackTrace
  } finally {
    client.stop()
  }
}
