package org.kae

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.SelectionKey
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.ByteBuffer

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Queue => MQueue}

/** Class for an NIO server.
  */
class NioServer(
  // The host:port on which to listen:
  hostAddress: InetAddress, port: Int)
{
  /** Commands for changing the selector.
    */
  sealed trait SelectionChangeRequest
  case class ChangeOps(socket: SocketChannel, ops: Int) extends SelectionChangeRequest
  case object Stop extends SelectionChangeRequest

  /** Trait for processing received data.
    */
  trait InputReceiver
  {
    def processData(server: NioServer,
                    socket: SocketChannel,
                    bytes: Array[Byte],
                    byteCount: Int)
  }

  /** Class for worker thread which echoes back what is received.
    */
  class EchoWorker extends InputReceiver with Runnable
  {
    private val queue = MQueue[(NioServer, SocketChannel, Array[Byte])]()

    // Called by the server when new data is available.
    override def processData(server: NioServer,
                             socket: SocketChannel,
                             bytes: Array[Byte],
                             byteCount: Int) = {
      // Make a copy of the data
      val dataCopy = new Array[Byte](byteCount)
      Array.copy(bytes, 0, dataCopy, 0, byteCount)

      // Queue the copied data
      queue.synchronized {
        queue enqueue Tuple3(server, socket, dataCopy)
        queue.notify
      }
    }

    // Main loop of worker thread.
    override def run = {
      while (true) {
        queue.synchronized {
          while (queue.isEmpty) {
            try {
              queue.wait
            } catch {
              case ex: InterruptedException => // ignore
            }
          }
          // There is data in the queue. Remove the first chunk...
          val (server, socketChannel, data) = queue.dequeue
          // ... and echo it back to sender.
          server.send(socketChannel, data)
        }
      }
    }
  }

  // Initialize a channel and selector for ACCEPT events.
  private val (
    // Channel on which to accept connections
    serverChannel,
    // Selector to monitor
    selector) = {

      // Create instance.
      val selector = SelectorProvider.provider.openSelector

      // Create new non-blocking server socket channel.
      val serverChannel = ServerSocketChannel.open()
      serverChannel.configureBlocking(false)

      // Bind the server socket to specified address/port.
      val isa = new InetSocketAddress(hostAddress, port)
      serverChannel.socket.bind(isa)

      // Register the the server socket channel, with our selector,
      // indicating an interest in accepting new connections.
      serverChannel.register(selector, SelectionKey.OP_ACCEPT)

      (serverChannel, selector)
  }

  // Buffer into which we read data.
  private val readBuffer = ByteBuffer.allocate(8*1024)

  // Sink for data we read.
  private val worker = new EchoWorker

  // Requests to change selector behaviour.
  private val changeRequests = MQueue[SelectionChangeRequest]()

  // Per-channel queues of data to be written.
  private val pendingData = MMap[SocketChannel, MQueue[ByteBuffer]]()

  // Process "acceptable" event by accepting a new connection.
  private def accept(key: SelectionKey) = {
   val serverChannel = key.channel.asInstanceOf[ServerSocketChannel]

   // Accept the connection.
   val socketChannel = serverChannel.accept

   // Make the new socket non-blocking.
   socketChannel.configureBlocking(false)

   // Register the new channel with our selector, for read events.
   socketChannel.register(selector, SelectionKey.OP_READ)
  }

  // Process "readable" event by reading data from a channel.
  private def read(key: SelectionKey) = {

    val socketChannel = key.channel.asInstanceOf[SocketChannel]

    // Prepare the buffer.
    readBuffer.clear

    // Perform the read.
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
      // We have data to consume. Pass it to worker.
      println("Server selector thread: read %d bytes".format(bytesRead))
      worker.processData(this, socketChannel, readBuffer.array, bytesRead)
    }
  }

  // Process "writeable" event by writing data to a channel.
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
  private def send(socket: SocketChannel, data: Array[Byte]) = {
    changeRequests.synchronized {
      // We are now interested in when the channel is writeable.
      changeRequests enqueue ChangeOps(socket, SelectionKey.OP_WRITE)

      // Queue the data to be written
      pendingData.synchronized {
        val queue = pendingData.getOrElseUpdate(socket, MQueue[ByteBuffer]())
        queue enqueue ByteBuffer.wrap(data)
      }
    }

    // Wake the selecting thread to act on our request.
    selector.wakeup
  }

  private var thread: Option[Thread] = None

  /** Start the server.
    */
  def start() = {
    synchronized {
      val t = new Thread(new Runnable() {
        override def run = mainLoop
      })
      thread = Some(t)
      t.start()
    }
  }

  /** Stop the server.
    */
  def stop() = {
    changeRequests.synchronized {
      changeRequests enqueue Stop
    }
    selector.wakeup
  }

  private def mainLoop: Unit = {
    // Start a worker to process input received.
    val w = new Thread(worker)
    w.setDaemon(true)
    w.start()

    while (true) {
      try {
        // Dequeue and process any pending changes.
        changeRequests.synchronized {
          while (!changeRequests.isEmpty) {
            changeRequests.dequeue match {

              case Stop => { return }

              case ChangeOps(socketChannel, ops) => {
                println("Server selector thread: ChangeOps")
                val key = socketChannel.keyFor(selector)
                key.interestOps(ops)
              }

              case _ => sys.error("Can't happen")
            }
          }
        }

        // Wait for an event on one of the registered channels.
        println("Server selector thread: sleeping")
        selector.select

        // For each key for which events are available
        selector.selectedKeys.foreach { key =>

          // Remove the key to indicate it has been processed.
          selector.selectedKeys.remove(key)

          if (key.isValid) {
            if (key.isAcceptable) {
              println("Server selector thread: event acceptable")
              accept(key)
            }
            else if (key.isReadable) {
              println("Server selector thread: event readable")
              read(key)
            }
            else if (key.isWritable) {
              println("Server selector thread: event writeable")
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

object NioServerApp extends App {
  new NioServer(null, 9090).start()
}
