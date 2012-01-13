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
package org.fusesource.memcached.codec

import java.net.ProtocolException
import org.fusesource.hawtbuf.DataByteArrayInputStream

import org.fusesource.hawtbuf.Buffer
import org.fusesource.hawtbuf.Buffer._
import org.iq80.memory.Region
import org.fusesource.hawtdispatch.transport.AbstractProtocolCodec

trait Entry {
  def key:Buffer
  def size:Int
  def value:Region
  def retain:Unit
  def release:Unit
}

trait EntryAllocator {
  def allocate(key:Buffer, size:Int):Entry
}

sealed trait Message
sealed trait Response extends Message
sealed trait Request extends Message

sealed trait EntryBlock {
  def entry:Entry
}

object TEXT_OK extends Response
object TEXT_ERROR extends Response
object TEXT_END extends Response
object TEXT_DELETED extends Response
object TEXT_NOT_FOUND extends Response
object TEXT_STORED extends Response
object TEXT_NOT_STORED extends Response
object TEXT_EXISTS extends Response
case class TEXT_VALUE(entry:Entry, flags:Int, cas:Option[Int]) extends Response with EntryBlock

case class TEXT_SET(entry:Entry, flags:Int, expire:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_ADD(entry:Entry, flags:Int, expire:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_REPLACE(entry:Entry, flags:Int, expire:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_APPEND(entry:Entry, flags:Int, expire:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_PREPEND(entry:Entry, flags:Int, expire:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_CAS(entry:Entry, flags:Int, expire:Int, cas:Int, noreply:Boolean) extends Request with EntryBlock
case class TEXT_GET(keys:Array[Buffer]) extends Request
case class TEXT_GETS(keys:Array[Buffer]) extends Request
case class TEXT_DELETE(key:Buffer, time:Int, noreply:Boolean) extends Request
case class TEXT_INCR(key:Buffer, value:Long, noreply:Boolean) extends Request
case class TEXT_DECR(key:Buffer, value:Long, noreply:Boolean) extends Request
case class TEXT_STATS(args:Array[Buffer]) extends Request
case class TEXT_FLUSH_ALL(delay:Int, noreply:Boolean) extends Request
case class TEXT_VERSION() extends Request
case class UNKNOWN(request:Buffer) extends Request
object TEXT_QUIT extends Request

case class BINARY_REQUEST(opcode:Byte, cas:Int, extras:Buffer, key:Buffer, entry:Entry)
case class BINARY_RESPONSE(opcode:Byte, status:Int, cas:Int, extras:Buffer, key:Buffer, entry:Entry)

object MemcacheProtocolConstants {

  val SET_REQ = ascii("set")
  val ADD_REQ = ascii("add")
  val REPLACE_REQ = ascii("replace")
  val APPEND_REQ = ascii("append")
  val PREPEND_REQ = ascii("prepend")
  val CAS_REQ = ascii("cas")
  val GET_REQ = ascii("get")
  val GETS_REQ = ascii("gets")
  val DELETE_REQ = ascii("delete")
  val INCR_REQ = ascii("incr")
  val DECR_REQ = ascii("decr")
  val STATS_REQ = ascii("stats")
  val FLUSH_ALL_REQ = ascii("flush_all")
  val VERSION_REQ = ascii("version")
  val QUIT_REQ = ascii("quit")

  val OK_RESP = ascii("OK")
  val ERROR_RESP = ascii("ERROR")
  val END_RESP = ascii("END")
  val DELETED_RESP = ascii("DELETED")
  val NOT_FOUND_RESP = ascii("NOT_FOUND")
  val STORED_RESP = ascii("STORED")
  val NOT_STORED_RESP = ascii("NOT_STORED")
  val EXISTS_RESP = ascii("EXISTS")
  val VALUE_RESP = ascii("VALUE")

  val SPACE = ' '.toByte
  val NL = '\n'.toByte
  val RETURN = '\r'.toByte
  val MAX_LINE_LENGTH = 1024

  val LINE_END = ascii("\r\n")
  val NOREPLY_ARG = ascii("noreply")

}

sealed trait ProtocolType
object DETECT_PROTOCOL extends ProtocolType
object TEXT_PROTOCOL extends ProtocolType
object BINARY_PROTOCOL extends ProtocolType

/**
 * Implementation of the Memcache Protocol encoding/decoding.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MemcacheProtocolCodec(val allocator:EntryAllocator, var protocol_type:ProtocolType=DETECT_PROTOCOL) extends AbstractProtocolCodec {
  import MemcacheProtocolConstants._

  implicit def toAction(func:()=>AnyRef):AbstractProtocolCodec.Action = new AbstractProtocolCodec.Action {
    def apply = func()
  }
  
  protected def initialDecodeAction = protocol_type match {
    case DETECT_PROTOCOL => detect_protocol _
    case TEXT_PROTOCOL => read_text_frame _
    case BINARY_PROTOCOL => read_binary_frame _
  }

  private def detect_protocol():AnyRef = {
    val initial = peekBytes(1)
    if( initial==null ) {
      null
    } else {
      if( initial.get(0) >= 0x80 ) {
        protocol_type = BINARY_PROTOCOL
        nextDecodeAction = read_binary_frame _
      } else {
        protocol_type = TEXT_PROTOCOL
        nextDecodeAction = read_text_frame _
      }
      nextDecodeAction()
    }
  }

  private def read_text_frame():AnyRef = {
    val line = readUntil(NL, MAX_LINE_LENGTH)
    if( line == null ) {
      return null
    } else {
      strip_nl(line)
      val args = line.split(SPACE).toList
      if( args.isEmpty ) {
        return UNKNOWN(line)
      }
      val cmd = args.head

      def opt(index:Int) = if( index < args.length ) {
        Some(args(index))
      } else {
        None
      }
      
      def noreply(arg:Int) = opt(arg).map(_ == NOREPLY_ARG).getOrElse(false)

      def try_read_entry_value(x:EntryBlock):AnyRef = {
        nextDecodeAction = read_text_entry_value(x) _
        nextDecodeAction()
      }
      
      implicit def to_int(value:Buffer):Int = value.ascii().toString.toInt
      implicit def to_long(value:Buffer):Long = value.ascii().toString.toLong

      def allocate(key:Buffer, length:Int) = allocator.allocate(key, length)

      cmd.ascii() match {
        case SET_REQ =>
          try_read_entry_value(TEXT_SET(allocate(args(1), args(4)), args(2), args(3), noreply(5)))
        case PREPEND_REQ =>
          try_read_entry_value(TEXT_PREPEND(allocate(args(1), args(4)), args(2), args(3), noreply(5)))
        case APPEND_REQ => 
          try_read_entry_value(TEXT_APPEND(allocate(args(1), args(4)), args(2), args(3), noreply(5)))
        case ADD_REQ =>
          try_read_entry_value(TEXT_ADD(allocate(args(1), args(4)), args(2), args(3), noreply(5)))
        case REPLACE_REQ =>
          try_read_entry_value(TEXT_REPLACE(allocate(args(1), args(4)), args(2), args(3), noreply(5)))
        case CAS_REQ =>
          try_read_entry_value(TEXT_CAS(allocate(args(1), args(4)), args(2), args(3), args(5), noreply(6)))

        case INCR_REQ =>
          TEXT_INCR( args(1), args(2), noreply(3))
        case DECR_REQ =>
          TEXT_DECR( args(1), args(2), noreply(3))
        case DELETE_REQ =>
          val time = opt(2).map(to_int(_)).getOrElse(0)
          TEXT_DELETE( args(1), time, noreply(3))
        case GET_REQ =>
          TEXT_GET( args.drop(1).toArray )
        case GETS_REQ => 
          TEXT_GETS( args.drop(1).toArray )
        case FLUSH_ALL_REQ =>
          val delay = opt(1).map(to_int(_)).getOrElse(0)
          TEXT_FLUSH_ALL( delay, noreply(2) )

        case QUIT_REQ        => TEXT_QUIT

        case OK_RESP         =>  TEXT_OK
        case VALUE_RESP      =>
          try_read_entry_value(TEXT_VALUE(allocate(args(1), args(3)), args(2), opt(4).map(to_int(_))))
        case ERROR_RESP      => TEXT_ERROR
        case END_RESP        => TEXT_END
        case DELETED_RESP    => TEXT_DELETED
        case NOT_FOUND_RESP  => TEXT_NOT_FOUND
        case STORED_RESP     => TEXT_STORED
        case NOT_STORED_RESP => TEXT_NOT_STORED
        case EXISTS_RESP     => TEXT_EXISTS

        case _ => UNKNOWN(line)
      }
    }
  }

  private def read_text_entry_value(x:EntryBlock)() = {
    val buffer = x.entry.value.toByteBuffer
    if( readDirect(buffer) ) {

      def read_text_value_nl():AnyRef = {
        val line = readUntil(NL, 2)
        if( line == null ) {
          return null
        } else {
          strip_nl(line)
          if(line.length != 0) {
            throw new ProtocolException("trailing data detected after value")
          } else {
            nextDecodeAction = read_text_frame _
            return x
          }
        }
      }

      nextDecodeAction = read_text_value_nl _
      nextDecodeAction()
    } else {
      null
    }
  }

  private def read_binary_frame():AnyRef = {
    val header = readBytes(24)
    if( header == null ) {
      return null
    } else {

      val is = new DataByteArrayInputStream(header)
      val magic = is.readByte()
      val opcode = is.readByte()

      val key_length = (is.readShort() & 0xFFFF).toInt
      val extras_length = (is.readByte() & 0xFF).toShort
      val data_type = is.readByte()
      val status = (is.readShort() & 0xFFFF).toInt

      val body_length = is.readInt()
      is.skip(4) // opaque[4]
      val cas = is.readInt()

      if( key_length+extras_length > body_length ) {
        throw new ProtocolException("key length + extras length > body length")
      }

      def read_extras():AnyRef = {
        val extras = readBytes(extras_length);
        if( extras == null ) {
          null
        } else {

          def read_key():AnyRef = {
            val key = readBytes(key_length);
            if( key == null ) {
              null
            } else {
              val value_length = body_length-(extras_length+key_length)
              if( value_length == 0 ) {
                nextDecodeAction = read_binary_frame _
                binary_decode(header, magic, opcode, data_type, status, cas, extras, key, null)
              } else {
                val item = allocator.allocate(key, value_length)
                def read_value():AnyRef = {
                  val value_buffer = item.value.toByteBuffer
                  if( readDirect(value_buffer) ) {
                    nextDecodeAction = read_binary_frame _
                    binary_decode(header, magic, opcode, data_type, status, cas, extras, key, item)
                  } else {
                    null
                  }
                }
                nextDecodeAction = read_value _
                nextDecodeAction()
              }
            }
          }
          nextDecodeAction = read_key _
          nextDecodeAction()
        }
      }
      nextDecodeAction = read_extras _
      nextDecodeAction()
    }
  }

  private def binary_decode(header:Buffer, magic:Byte, opcode:Byte, data_type:Byte, status:Int, cas:Int, extras:Buffer, key:Buffer, item:Entry):AnyRef = {
    assert ( data_type==0 )
    magic match {
      case 0x80 =>
        assert(status == 0)
        BINARY_REQUEST(opcode, cas, extras, key, item)
      case 0x81 =>
        BINARY_RESPONSE(opcode, status, cas, extras, key, item)
      case _ => UNKNOWN(header)
    }
    null
  }

  private def strip_nl(buffer:Buffer) = {
    var l = buffer.length
    if( buffer.get(l-1) == NL) {
      l -= 1
    }
    if( l > 0 && buffer.get(l-1) == RETURN) {
      l -= 1
    }
    buffer.length = l
  }


  protected def encode(value: AnyRef):Unit = protocol_type match {
    case TEXT_PROTOCOL =>
      encode_text(value)
    case BINARY_PROTOCOL =>
      encode_binary(value)
    case DETECT_PROTOCOL =>
      // figure out what protocol is being used..
      protocol_type = value match {
        case BINARY_RESPONSE => BINARY_PROTOCOL
        case BINARY_REQUEST => BINARY_PROTOCOL
        case _ =>  TEXT_PROTOCOL
      }
      // any try again.
      encode(value)
  }

  protected def encode_binary(value: AnyRef) = value match {
    case BINARY_RESPONSE =>
      throw new RuntimeException("TODO: implement me")
    case BINARY_REQUEST =>
      throw new RuntimeException("TODO: implement me")
    case _ =>
      throw new ProtocolException("Invalid binary protocol command: "+value)
  }

  protected def encode_text(value: AnyRef) = value match {

    case TEXT_OK         => encode7(OK_RESP)
    case TEXT_VALUE(item, flags, cas) =>
      encode8(VALUE_RESP, item, flags, cas);

    case TEXT_ERROR      => encode7(ERROR_RESP)
    case TEXT_END        => encode7(END_RESP)
    case TEXT_DELETED    => encode7(DELETED_RESP)
    case TEXT_NOT_FOUND  => encode7(NOT_FOUND_RESP)
    case TEXT_STORED     => encode7(STORED_RESP)
    case TEXT_NOT_STORED => encode7(NOT_STORED_RESP)
    case TEXT_EXISTS     => encode7(EXISTS_RESP)

    case TEXT_SET(item, flags, expire, noreply) =>
      encode0(SET_REQ, item, flags, expire, noreply);
    case TEXT_PREPEND(item, flags, expire, noreply) =>
      encode0(PREPEND_REQ, item, flags, expire, noreply);
    case TEXT_APPEND(item, flags, expire, noreply) =>
      encode0(APPEND_REQ, item, flags, expire, noreply);
    case TEXT_ADD(item, flags, expire, noreply) =>
      encode0(ADD_REQ, item, flags, expire, noreply);
    case TEXT_REPLACE(item, flags, expire, noreply) =>
      encode0(REPLACE_REQ, item, flags, expire, noreply);
    case TEXT_CAS(item, flags, expire, cas, noreply) =>
      encode2(CAS_REQ, item, flags, expire, cas, noreply);

    case value:TEXT_INCR =>
      encode3(INCR_REQ, value.key, value.value, value.noreply);
    case value:TEXT_DECR =>
      encode3(DECR_REQ, value.key, value.value, value.noreply);
    case value:TEXT_DELETE =>
      encode4(DELETE_REQ, value.key, value.time, value.noreply);
    case value:TEXT_GET =>
      encode5(GET_REQ, value.keys)
    case value:TEXT_GETS =>
      encode5(GETS_REQ, value.keys)
    case value:TEXT_FLUSH_ALL =>
      encode6(FLUSH_ALL_REQ, value.delay, value.noreply);
    case TEXT_QUIT =>
      encode7(QUIT_REQ)

    case _ =>
      throw new ProtocolException("Invalid text protocol command: "+value)
  }

  def encode7(command:Buffer):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(LINE_END)
  }

  def encode0(command:Buffer, item:Entry, flags:Int, expire:Long, noreply:Boolean):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(item.key)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(flags.toString))
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(expire.toString))
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(item.size.toString))
    if(noreply) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(NOREPLY_ARG)
    }
    nextWriteBuffer.write(LINE_END)
    writeDirect(item.value.toByteBuffer)
  }

  def encode2(command:Buffer, item:Entry, flags:Int, expire:Long, cas:Int, noreply:Boolean):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(item.key)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(flags.toString))
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(expire.toString))
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(item.size.toString))
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(cas.toString))
    if(noreply) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(NOREPLY_ARG)
    }
    nextWriteBuffer.write(LINE_END)
    writeDirect(item.value.toByteBuffer)
  }

  def encode3(command:Buffer, key:Buffer, value:Long, noreply:Boolean):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(key)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(value.toString))
    if(noreply) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(NOREPLY_ARG)
    }
    nextWriteBuffer.write(LINE_END)
  }

  def encode4(command:Buffer, key:Buffer, time:Long, noreply:Boolean):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(key)
    if(time!=0 || noreply) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(ascii(time.toString))
      if(noreply) {
        nextWriteBuffer.write(SPACE)
        nextWriteBuffer.write(NOREPLY_ARG)
      }
    }
    nextWriteBuffer.write(LINE_END)
  }

  def encode5(command:Buffer, args:Array[Buffer]):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    args.foreach { arg =>
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(arg)
    }
    nextWriteBuffer.write(LINE_END)
  }

  def encode6(command:Buffer, time:Long, noreply:Boolean):Unit = {
    nextWriteBuffer.write(command)
    if(time!=0 || noreply) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(ascii(time.toString))
      if(noreply) {
        nextWriteBuffer.write(SPACE)
        nextWriteBuffer.write(NOREPLY_ARG)
      }
    }
    nextWriteBuffer.write(LINE_END)
  }

  def encode8(command:Buffer, item:Entry, flags:Int, cas:Option[Int]):Unit = {
    nextWriteBuffer.write(command)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(item.key)
    nextWriteBuffer.write(SPACE)
    nextWriteBuffer.write(ascii(flags.toString))
    if(cas.isDefined) {
      nextWriteBuffer.write(SPACE)
      nextWriteBuffer.write(ascii(cas.get.toString))
    }
    nextWriteBuffer.write(LINE_END)
    writeDirect(item.value.toByteBuffer)
    nextWriteBuffer.write(LINE_END)
  }

}
