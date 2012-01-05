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

import junit.framework.TestCase
import java.util.concurrent.CountDownLatch
import java.net.{Socket, URI}
import collection.mutable.ListBuffer
import java.io.EOFException
import junit.framework.Assert._

class MemcacheServerTest extends TestCase {

  var server:MemcacheServer = new MemcacheServer(new URI("tcp://0.0.0.0:0"))
  var clients = ListBuffer[Client]()
  
  override def setUp: Unit = {
    server = new MemcacheServer(new URI("tcp://0.0.0.0:0"))
    val done = new CountDownLatch(1)
    server.start(done.countDown())
    done.await()
  }

  override def tearDown: Unit = {
    clients.foreach(_.close)
    clients.clear()
    
    val done = new CountDownLatch(1)
    server.stop(done.countDown())
    done.await()
  }

  class Client {
    var socket = new Socket("127.0.0.1", server.transport_server.getSocketAddress.getPort)
    
    def write(value:String) = socket.getOutputStream.write(value.getBytes("UTF-8"));

    def write_data(command:String, key:String, value:String, noreply:Boolean=false, flags:Int=0, exp:Int=0) = {
      write("%s %s %d %d %d%s\r\n%s\r\n".format(
        command, key, flags, exp, value.length(), if(noreply) " noreply" else "", value
      ))
    }
    
    def read(max:Int=1024*1024):String = {
      val buffer = new Array[Byte](max);
      val count = socket.getInputStream.read(buffer);
      if( count < 0 ) {
        throw new EOFException();
      }
      new String(buffer, 0, count, "UTF-8")
    }
     
    def close = {
      if( socket!=null ) {
        try {
          socket.close
        } catch {
          case _ =>
        }
        socket = null
      }
    }
  }

  def connect = {
    val rc = new Client
    clients += rc
    rc
  }
  
  def testPutGet = {

    val client = connect
    client.write_data("set", "hello", "world")
    assertEquals("STORED", client.read())

  }
}