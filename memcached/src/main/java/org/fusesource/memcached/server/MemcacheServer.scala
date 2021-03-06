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

import java.net.URI
import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.transport._
import org.iq80.memcached._
import org.fusesource.hawtbuf.Buffer
import org.iq80.memory._
import org.fusesource.memcached.codec.{EntryAllocator, Entry}


/**
 * Provides an abstract base class to make implementing the ProtocolCodec interface
 * easier.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object MemcacheServer {
  def main(args: Array[String]): Unit = {
    println("Starting...")
    val server = new MemcacheServer(new URI("tcp://0.0.0.0:11211"))
    server.start {
      "Started. press [enter] to shutdown."
    }
    System.in.read()
    println("Stopping...")
    server.stop {
      println("Stopped.")
    }
  }
}

case class CacheEntry(key: Buffer, size: Int, item:FlatMap.Item) extends Entry {

//  def retain = it
//  def release:Unit
  def release = item.addReference()

  def retain = item.addReference()

  def value = item.getValue
}


class MemcacheServer(uri:URI) {

  var transport_server: TcpTransportServer = _
  var allocator:Allocator = _

  def toBuffer(region:Region) = {
    val rc = new Buffer(region.size().toInt);
    region.getBytes(0, rc.data)
    rc
  }
  
  var cache:FlatMap = _

  object cache_entry_allocator extends EntryAllocator {
    def allocate(key: Buffer, size: Int) = {
      CacheEntry(key, size, cache.allocateItem(key.toByteArray, 0, 0, size))
    }

    def wrap(item:FlatMap.Item) = {
      CacheEntry(toBuffer(item.getKey), item.getValue.size().toInt, item)
    }
  }

  var max_memory = 1024*1024*256
  var factor=1.25d
  var prealloc=false
  var chunk_size = 1024*1024;
  var max_item_size = 1024*4;

  def start[T](on_complete: =>T ): Unit = {
    allocator = ByteBufferAllocator.INSTANCE
    var slab_allocator = new SlabAllocator(allocator, max_memory, factor, prealloc, chunk_size, max_item_size)
    cache = new FlatMap(slab_allocator)

    transport_server = new TcpTransportServer(uri)
    transport_server.setDispatchQueue(Dispatch.createQueue("server"))
    transport_server.setTransportServerListener(new TransportServerListener {
      def onAccept(transport: Transport): Unit = {
        try {
          new MemcacheConnection(MemcacheServer.this, transport)
        } catch {
          case e: Exception => {
            transport.stop(null)
            onAcceptError(e)
          }
        }
      }

      def onAcceptError(error: Exception): Unit = {
        System.out.println("Server error: " + error)
      }
    })
    transport_server.start(^{
      on_complete
    })
  }

  def stop[T](on_complete: =>T ): Unit = {
    transport_server.stop(^{
      on_complete
    })
  }
}
