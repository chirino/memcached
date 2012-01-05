/*
 * Copyright (C) 2011-2012, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.memcached.server

import java.io.IOException
import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.transport._
import org.fusesource.memcached.codec._
import java.util.concurrent.TimeUnit._
import org.fusesource.hawtbuf.Buffer
import org.iq80.memory.{ByteBufferAllocation, Allocation, Allocator}
import java.util.concurrent.atomic.AtomicInteger
import collection.mutable.ListBuffer

object BufferAllocator extends Allocator {

  def region(address: Long, length: Long) = throw new UnsupportedOperationException
  def region(address: Long) = throw new UnsupportedOperationException

  def allocate(size: Long): Allocation = {
    if (size < 0) {
      throw new IllegalArgumentException("Size is negative: " + size)
    }
    if (size > Integer.MAX_VALUE) {
      throw new IllegalAccessError("Size is greater than 31 bits: " + size)
    }
    if (size == 0) {
      return Allocator.NULL_POINTER
    }
    wrap(new Buffer(size.asInstanceOf[Int]))
  }

  def wrap(buffer:Buffer) = new BufferAllocation(buffer, idGenerator.incrementAndGet)

  private final val idGenerator: AtomicInteger = new AtomicInteger
  
}

class BufferAllocation(buffer:Buffer, id:Int)
  extends ByteBufferAllocation(buffer.toByteBuffer.slice(), id) {
  override def getAllocator: Allocator = BufferAllocator
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MemcacheConnection( val server: MemcacheServer, val transport: Transport) extends TransportListener {

  this.transport.setProtocolCodec(new MemcacheProtocolCodec(server.cache_entry_allocator))
  this.transport.setDispatchQueue(Dispatch.createQueue(transport.getRemoteAddress.toString))
  this.transport.setTransportListener(this)
  this.transport.start(null)
  
    
  var quit = false

  def onTransportConnected: Unit = {
    println("client connected: "+transport.getRemoteAddress)
    this.transport.resumeRead()
  }

  def onTransportDisconnected(reconnecting: Boolean) {
    println("client disconnected: "+transport.getRemoteAddress)
  }

  def onTransportFailure(error: IOException): Unit = {
    if(!quit) {
      transport.stop(NOOP)
    }
  }

  def onRefill: Unit = {
    while(!sendQueue.isEmpty) {
      if(transport.offer(sendQueue.head)) {
        sendQueue.remove(0)
      } else {
        return
      }
    }
  }

  val sendQueue = ListBuffer[Response]()
  def send(resp:Response) = {
    if( sendQueue.isEmpty ) {
      if(transport.offer(resp)) {
        true
      } else {
        sendQueue += resp
        false
      }
    } else {
      sendQueue += resp
      false
    }
  }

  def onTransportCommand(command: AnyRef): Unit = {


    command match {

      case TEXT_FLUSH_ALL(delay, noreply) =>
        if( delay == 0 ) {
          server.cache.flush()
        } else {
          transport.getDispatchQueue.after(delay, SECONDS) {
            server.cache.flush()
          }
        }
        send(TEXT_OK)
        
      case TEXT_QUIT =>
        quit = true
        transport.stop(NOOP);
        
      case TEXT_SET(entry, flags, expire, noreply) =>

        if( entry.value !=null ) {

          val value = entry.asInstanceOf[CacheEntry].item
          value.setExptime(expire)
          value.setFlags(flags.toByte)

          var original = server.cache.peek(BufferAllocator.wrap(entry.key))
          if( original!=null ) {
            // TODO set value.
            server.cache.replace(original, value)
          } else {
            server.cache.insert(value)
          }
          send(TEXT_STORED)
        } else {
          // Not enough memory..
          send(TEXT_NOT_STORED)
        }
      case TEXT_GET(keys) =>

        keys.foreach { key =>
          val item = server.cache.get(BufferAllocator.wrap(key))
          if(item!=null) {
            send(TEXT_VALUE(server.cache_entry_allocator.wrap(item), item.getFlags, Some(item.getCas.toInt) ))
          }
        }
        send(TEXT_END)

      case x:UNKNOWN =>
        transport.offer(TEXT_ERROR)

// TODO: implement the binary protocol handling.
//
//      case BINARY_REQUEST(opcode, cas, extras, key, entry) =>
//        var quiet = false
//
//        def get = {
//          assert(extras==null)
//          assert(key!=null)
//          assert(value==null)
//        }
//        def flush = {
//          assert(key==null)
//          assert(value==null)
//          val expiration = if( extras!=null ) {
//            extras.getInt(0)
//          } else {
//            0
//          }
//        }
//        def noop = {
//          assert(extras==null)
//          assert(key==null)
//          assert(value==null)
//        }
//        def version = {
//          assert(extras==null)
//          assert(key==null)
//          assert(value==null)
//        }
//        def append = {
//          assert(extras==null)
//          assert(key!=null)
//          assert(value!=null)
//        }
//        def prepend = {
//          assert(extras==null)
//          assert(key!=null)
//          assert(value!=null)
//        }
//        def stat = {
//          assert(extras==null)
//          assert(value!=null)
//
//        }
//        def delete = {
//          assert(extras==null)
//          assert(key!=null)
//          assert(value==null)
//        }
//
//        def incr(negative:Boolean) = {
//          assert(extras.size() == 20)
//          assert(key!=null)
//          assert(value==null)
//          var amount = extras.getLong(0)
//          val initial = extras.getLong(8)
//          val expiration = extras.getInt(16)
//          if( negative ) {
//            amount *= -1
//          }
//        }
//
//        def set = {
//          assert(extras.size == 8)
//          assert(key!=null)
//          assert(value!=null)
//          val flags = extras.getInt(0)
//          val expiration = extras.getInt(4)
//        }
//        def add = {
//          assert(extras.size == 8)
//          assert(key!=null)
//          assert(value!=null)
//          val flags = extras.getInt(0)
//          val expiration = extras.getInt(4)
//        }
//        def replace = {
//          assert(extras.size == 8)
//          assert(key!=null)
//          assert(value!=null)
//          val flags = extras.getInt(0)
//          val expiration = extras.getInt(4)
//        }
//        def do_quit = {
//          assert(extras==null)
//          assert(key==null)
//          assert(value==null)
//
//          transport.stop(NOOP);
//        }
//        opcode match {
//          case 0x00 => //Get
//            get
//          case 0x09 => // GetQ
//            quiet = true
//            get
//          case 0x0C => // GetK
//            get
//          case 0x0D => // GetKQ
//            quiet = true
//            get
//          case 0x08 => // Flush
//            flush
//          case 0x18 => // FlushQ
//            quiet = true
//            flush
//          case 0x0E => // Append
//            append
//          case 0x19 => // AppendQ
//            quiet = true
//            append
//          case 0x0F => // Prepend
//            prepend
//          case 0x1A => // PrependQ
//            quiet = true
//            prepend
//          case 0x04 => // Delete
//            delete
//          case 0x14 => // DeleteQ
//            quiet = true
//            delete
//          case 0x05 => // Increment
//            incr(false)
//          case 0x15 => // IncrementQ
//            quiet = true
//            incr(false)
//          case 0x06 => // Decrement
//            incr(true)
//          case 0x16 => // DecrementQ
//            quiet = true
//            incr(true)
//          case 0x01 => // Set
//            set
//          case 0x11 => // SetQ
//            quiet = true
//            set
//          case 0x02 => // Add
//            add
//          case 0x12 => // AddQ
//            quiet = true
//            add
//          case 0x03 => // Replace
//            replace
//          case 0x13 => // ReplaceQ
//            quiet = true
//            replace
//          case 0x07 => // Quit
//            do_quit
//          case 0x17 => // QuitQ
//            quiet = true
//            do_quit
//          case 0x0A => // No-op
//            noop
//          case 0x0B => // Version
//            version
//          case 0x10 => // Stat
//            stat
//          case _ =>
//            // TODO:
//        }

      case _ =>
        println("Unhandled "+command)
    }
  }


}