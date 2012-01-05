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
package org.fusesource.memcached.transport

import java.nio.ByteBuffer
import org.fusesource.hawtbuf.{Buffer, DataByteArrayOutputStream}
import org.fusesource.hawtdispatch.transport.ProtocolCodec
import java.io.EOFException
import java.net.ProtocolException
import collection.mutable.ListBuffer
import java.nio.channels.{GatheringByteChannel, SocketChannel, ReadableByteChannel, WritableByteChannel}

/**
 * Provides an abstract base class to make implementing the ProtocolCodec interface
 * easier.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class AbstractProtocolCodec extends ProtocolCodec {

  protected var write_buffer_size: Int = 1024 * 64
  protected var write_counter: Long = 0L
  protected var write_channel: GatheringByteChannel = null
  protected var next_write_buffer: DataByteArrayOutputStream = new DataByteArrayOutputStream(write_buffer_size)
  protected var last_write_io_size: Int = 0

  protected var write_buffer = ListBuffer[ByteBuffer]()
  private var write_buffer_remaining = 0L

  def setWritableByteChannel(channel: WritableByteChannel): Unit = {
    // TODO: if it's SSL based socket, it won't be a GatheringByteChannel
    this.write_channel = channel.asInstanceOf[GatheringByteChannel]
    if (this.write_channel.isInstanceOf[SocketChannel]) {
      write_buffer_size = (this.write_channel.asInstanceOf[SocketChannel]).socket.getSendBufferSize
    }
  }

  def getReadBufferSize: Int = {
    return read_buffer_size
  }

  def getWriteBufferSize: Int = {
    return write_buffer_size
  }

  def full: Boolean = {
    return write_buffer_remaining >= write_buffer_size
  }

  def is_empty: Boolean = {
    return write_buffer_remaining == 0 && next_write_buffer.size() == 0 
  }

  def getWriteCounter: Long = {
    return write_counter
  }

  def getLastWriteSize: Int = {
    return last_write_io_size
  }

  protected def encode(value: AnyRef):Unit //= value.write(next_write_buffer)

  def write(value: AnyRef): ProtocolCodec.BufferState = {
    if (full) {
      return ProtocolCodec.BufferState.FULL
    }
    else {
      var was_empty: Boolean = is_empty
      encode(value)
      if( next_write_buffer.size >= (write_buffer_size >> 1) ) {
          flush_next_write_buffer
      }
      if (was_empty) {
        return ProtocolCodec.BufferState.WAS_EMPTY
      } else {
        return ProtocolCodec.BufferState.NOT_EMPTY
      }
    }
  }
  
  def write_direct(value:ByteBuffer) = {
    // is the direct buffer small enough to just fit into the next_write_buffer?
    var next_pos: Int = next_write_buffer.position()
    var value_length: Int = value.remaining()
    val available = next_write_buffer.getData.length - next_pos
    if( available > value_length ) {
      value.get(next_write_buffer.getData, next_pos, value_length)
      next_write_buffer.position(next_pos+value_length)
    } else {
      if (next_write_buffer.size != 0) {
        flush_next_write_buffer
      }
      write_buffer += value
      write_buffer_remaining += value.remaining()
    }
  }

  def flush_next_write_buffer {
    var next_size = (next_write_buffer.position).max(80).min(write_buffer_size)
    var bb: ByteBuffer = next_write_buffer.toBuffer.toByteBuffer
    write_buffer += bb
    write_buffer_remaining += bb.remaining()
    next_write_buffer = new DataByteArrayOutputStream(next_size)
  }

  def flush: ProtocolCodec.BufferState = {
    while (true) {
      if (write_buffer_remaining != 0) {
        val buffers: Array[ByteBuffer] = write_buffer.toArray
        // TODO.. codec interface needs to be updated to allow the last io to be a long
        last_write_io_size = write_channel.write(buffers, 0, buffers.length).toInt
        if (last_write_io_size == 0) {
          return ProtocolCodec.BufferState.NOT_EMPTY
        } else {
          write_buffer_remaining -= last_write_io_size
          write_counter += last_write_io_size
          if( write_buffer_remaining == 0 ) {
            write_buffer.clear()
          } else {
            while( !write_buffer.head.hasRemaining ) {
              write_buffer.remove(0)
            }
          }
        }
      } else {
        if (next_write_buffer.size == 0) {
          return ProtocolCodec.BufferState.EMPTY
        } else {
          flush_next_write_buffer
        }
      }
    }
    null
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Non blocking read impl
  //
  /////////////////////////////////////////////////////////////////////
  protected var read_counter: Long = 0L
  protected var read_buffer_size: Int = 1024 * 64
  protected var read_channel: ReadableByteChannel = null
  protected var read_buffer: ByteBuffer = ByteBuffer.allocate(read_buffer_size)
  protected var direct_read_buffer: ByteBuffer = _
  
  protected var read_end: Int = 0
  protected var read_start: Int = 0
  protected var last_read_io_size: Int = 0
  protected var next_decode_action: ()=>AnyRef = initial_decode_action
  protected var trim: Boolean = true

  protected def initial_decode_action: ()=>AnyRef

  def setReadableByteChannel(channel: ReadableByteChannel): Unit = {
    this.read_channel = channel
    if (this.read_channel.isInstanceOf[SocketChannel]) {
      read_buffer_size = (this.read_channel.asInstanceOf[SocketChannel]).socket.getReceiveBufferSize
    }
  }

  def unread(buffer: Array[Byte]): Unit = {
    assert((read_counter == 0))
    read_buffer.put(buffer)
    read_counter += buffer.length
  }

  def getReadCounter: Long = {
    return read_counter
  }

  def getLastReadSize: Int = {
    return last_read_io_size
  }

  def read: AnyRef = {
    var command: AnyRef = null
    while (command == null) {
      if (direct_read_buffer!=null) {
        while( direct_read_buffer.hasRemaining ) {
          last_read_io_size = read_channel.read(direct_read_buffer)
          read_counter += last_read_io_size
          if (last_read_io_size == -1) {
            throw new EOFException("Peer disconnected")
          } else if (last_read_io_size == 0) {
            return null
          }
        }
        command = next_decode_action()
      } else {
        if (read_end == read_buffer.position) {
          
          if (read_buffer.remaining == 0) {
            var size: Int = read_end - read_start
            var new_capacity: Int = 0
            if (read_start == 0) {
              new_capacity = size + read_buffer_size
            }
            else {
              if (size > read_buffer_size) {
                new_capacity = size + read_buffer_size
              }
              else {
                new_capacity = read_buffer_size
              }
            }
            var new_buffer: Array[Byte] = new Array[Byte](new_capacity)
            if (size > 0) {
              System.arraycopy(read_buffer.array, read_start, new_buffer, 0, size)
            }
            read_buffer = ByteBuffer.wrap(new_buffer)
            read_buffer.position(size)
            read_start = 0
            read_end = size
          }
          var p: Int = read_buffer.position
          last_read_io_size = read_channel.read(read_buffer)
          read_counter += last_read_io_size
          if (last_read_io_size == -1) {
            read_counter += 1 // to compensate for that -1
            throw new EOFException("Peer disconnected")
          } else if (last_read_io_size == 0) {
            return null
          }
        }
        command = next_decode_action()
        assert((read_start <= read_end))
        assert((read_end <= read_buffer.position))
      }
    }
    return command
  }

  protected def read_until(octet: Byte, max:Int= -1, msg:String="Maximum protocol buffer length exeeded"): Buffer = {
    val array = read_buffer.array
    val buf = new Buffer(array, read_end, read_buffer.position - read_end)
    val pos = buf.indexOf(octet)
    if( pos >= 0 ) {
      val offset = read_start
      read_end += pos+1
      read_start = read_end
      var length: Int = read_end - offset
      if( max>=0 && length>max) {
        throw new ProtocolException(msg)
      }
      new Buffer(array, offset, length)
    } else {
      read_end += buf.length
      if( max>=0 && (read_end-read_start) > max) {
        throw new ProtocolException(msg)
      }
      null
    }
  }
  
  protected def read_bytes(length: Int): Buffer = {
    if ((read_buffer.position - read_start) < length) {
      read_end = read_buffer.position
      null
    } else {
      val offset = read_start
      read_end = offset + length
      read_start = read_end
      new Buffer(read_buffer.array, offset, length)
    }
  }

  protected def peek_bytes(length: Int): Buffer = {
    if ((read_buffer.position - read_start) < length) {
      read_end = read_buffer.position
      null
    } else {
      new Buffer(read_buffer.array, read_start, length)
    }
  }

  protected def read_direct(buffer: ByteBuffer): Boolean = {
    assert( direct_read_buffer==null || (direct_read_buffer eq buffer) )
    
    if(buffer.hasRemaining) {
      // First we need to transfer the read bytes from the non-direct
      // byte buffer into the direct one..
      val limit = read_buffer.position
      val transfer_size = (limit - read_start).min(buffer.remaining())
      val read_buffer_array = read_buffer.array()
      buffer.put(read_buffer_array, read_start, transfer_size)

      // The direct byte buffer might have been smaller than our read_buffer one..
      // compact the read_buffer to avoid doing additional mem allocations.
      val trailing_size = limit - (read_start + transfer_size)
      if( trailing_size > 0 ) {
        System.arraycopy(read_buffer_array, read_start+transfer_size, read_buffer_array, read_start, trailing_size)
      }
      read_buffer.position(read_start+trailing_size)
    }

    // For big direct byte buffers, it will still not have been filled,
    // so install it so that we directly read into it until it is filled.
    if( buffer.hasRemaining ) {
      direct_read_buffer = buffer;
      return false
    } else {
      direct_read_buffer = null
      buffer.flip()
      return true
    }
  }
}