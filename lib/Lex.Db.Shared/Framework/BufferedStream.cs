#if SILVERLIGHT || NETFX_CORE
using System;
using System.Security;

namespace System.IO
{
  sealed class BufferedStream : Stream
  {
    Stream _s;         
    byte[] _buffer;    
    int _readPos;      
    int _readLen;      
    int _writePos;     
    readonly int _bufferSize;   

    const int DefaultBufferSize = 4096;

    public BufferedStream(Stream stream)
      : this(stream, DefaultBufferSize)
    {
    }

    public BufferedStream(Stream stream, int bufferSize)
    {
      if (stream == null)
        throw new ArgumentNullException("stream");

      if (bufferSize <= 0)
        throw new ArgumentOutOfRangeException("bufferSize");

      _s = stream;
      _bufferSize = bufferSize;

      if (!_s.CanRead && !_s.CanWrite) ErrorStreamIsClosed();
    }

    public override bool CanRead
    {
      get { return _s != null && _s.CanRead; }
    }

    public override bool CanWrite
    {
      get { return _s != null && _s.CanWrite; }
    }

    public override bool CanSeek
    {
      get { return _s != null && _s.CanSeek; }
    }

    public override long Length
    {
      get
      {
        if (_s == null) ErrorStreamIsClosed();
        if (_writePos > 0) FlushWrite();
        return _s.Length;
      }
    }

    public override long Position
    {
      get
      {
        if (_s == null) ErrorStreamIsClosed();
        if (!_s.CanSeek) ErrorSeekNotSupported();
        //              return _s.Seek(0, SeekOrigin.Current) + (_readPos + _writePos - _readLen);
        return _s.Position + (_readPos - _readLen + _writePos);
      }
      set
      {
        if (value < 0) throw new ArgumentOutOfRangeException("value");

        if (_s == null) ErrorStreamIsClosed();
        if (!_s.CanSeek) ErrorSeekNotSupported();
        if (_writePos > 0) FlushWrite();
        _readPos = 0;
        _readLen = 0;
        _s.Seek(value, SeekOrigin.Begin);
      }
    }

    protected override void Dispose(bool disposing)
    {
      try
      {
        if (disposing && _s != null)
        {
          try
          {
            Flush();
          }
          finally
          {
            _s.Dispose();
          }
        }
      }
      finally
      {
        _s = null;
        _buffer = null;

        // Call base.Dispose(bool) to cleanup async IO resources 
        base.Dispose(disposing);
      }
    }

    public override void Flush()
    {
      if (_s == null) ErrorStreamIsClosed();
      if (_writePos > 0)
      {
        FlushWrite();
      }
      else if (_readPos < _readLen && _s.CanSeek)
      {
        FlushRead();
      }
      _readPos = 0;
      _readLen = 0;
    }

    // Reading is done by blocks from the file, but someone could read
    // 1 byte from the buffer then write.  At that point, the OS's file 
    // pointer is out of [....] with the stream's position.  All write
    // functions should call this function to preserve the position in the file.
    private void FlushRead()
    {
      if (_readPos - _readLen != 0)
        _s.Seek(_readPos - _readLen, SeekOrigin.Current);
      _readPos = 0;
      _readLen = 0;
    }

    // Writes are buffered.  Anytime the buffer fills up
    // (_writePos + delta > _bufferSize) or the buffer switches to reading
    // and there is dirty data (_writePos > 0), this function must be called. 
    private void FlushWrite()
    {
      _s.Write(_buffer, 0, _writePos);
      _writePos = 0;
      _s.Flush();
    }

    public override int Read(byte[] array, int offset, int count)
    {
      if (array == null)
        throw new ArgumentNullException("array");
      if (offset < 0)
        throw new ArgumentOutOfRangeException("offset");
      if (count < 0)
        throw new ArgumentOutOfRangeException("count");
      if (array.Length - offset < count)
        throw new ArgumentException();

      if (_s == null) ErrorStreamIsClosed();

      var n = _readLen - _readPos;
      // if the read buffer is empty, read into either user's array or our 
      // buffer, depending on number of bytes user asked for and buffer size.
      if (n == 0)
      {
        if (!_s.CanRead) ErrorReadNotSupported();
        if (_writePos > 0) FlushWrite();
        if (count >= _bufferSize)
        {
          n = _s.Read(array, offset, count);
          // Throw away read buffer. 
          _readPos = 0;
          _readLen = 0;
          return n;
        }
        if (_buffer == null) _buffer = new byte[_bufferSize];
        n = _s.Read(_buffer, 0, _bufferSize);
        if (n == 0) return 0;
        _readPos = 0;
        _readLen = n;
      }
      // Now copy min of count or numBytesAvailable (ie, near EOF) to array. 
      if (n > count) n = count;
      Buffer.BlockCopy(_buffer, _readPos, array, offset, n);
      _readPos += n;

      // We may have read less than the number of bytes the user asked
      // for, but that is part of the Stream contract.  Reading again for 
      // more data may cause us to block if we're using a device with 
      // no clear end of file, such as a serial port or pipe.  If we
      // blocked here & this code was used with redirected pipes for a 
      // process's standard output, this can lead to deadlocks involving
      // two processes.
      //  BUT - this is a breaking change.

      // If we hit the end of the buffer and didn't have enough bytes, we must
      // read some more from the underlying stream. 
      if (n < count)
      {
        int moreBytesRead = _s.Read(array, offset + n, count - n);
        n += moreBytesRead;
        _readPos = 0;
        _readLen = 0;
      }

      return n;
    }

    // Reads a byte from the underlying stream.  Returns the byte cast to an int
    // or -1 if reading from the end of the stream. 
    public override int ReadByte()
    {
      if (_s == null) ErrorStreamIsClosed();
      if (_readLen == 0 && !_s.CanRead) ErrorReadNotSupported();
      if (_readPos == _readLen)
      {
        if (_writePos > 0) FlushWrite();
        if (_buffer == null) _buffer = new byte[_bufferSize];
        _readLen = _s.Read(_buffer, 0, _bufferSize);
        _readPos = 0;
      }
      if (_readPos == _readLen) return -1;

      return _buffer[_readPos++];
    }

    [SecuritySafeCritical]  // auto-generated 
    public override void Write(byte[] array, int offset, int count)
    {
      if (array == null)
        throw new ArgumentNullException("array");
      if (offset < 0)
        throw new ArgumentOutOfRangeException("offset");
      if (count < 0)
        throw new ArgumentOutOfRangeException("count");
      if (array.Length - offset < count)
        throw new ArgumentException();

      if (_s == null) ErrorStreamIsClosed();
      if (_writePos == 0)
      {
        // Ensure we can write to the stream, and ready buffer for writing.
        if (!_s.CanWrite) ErrorWriteNotSupported();
        if (_readPos < _readLen)
          FlushRead();
        else
        {
          _readPos = 0;
          _readLen = 0;
        }
      }

      // If our buffer has data in it, copy data from the user's array into
      // the buffer, and if we can fit it all there, return.  Otherwise, write 
      // the buffer to disk and copy any remaining data into our buffer.
      // The assumption here is memcpy is cheaper than disk (or net) IO. 
      // (10 milliseconds to disk vs. ~20-30 microseconds for a 4K memcpy) 
      // So the extra copying will reduce the total number of writes, in
      // non-pathological cases (ie, write 1 byte, then write for the buffer 
      // size repeatedly)
      if (_writePos > 0)
      {
        int numBytes = _bufferSize - _writePos;   // space left in buffer
        if (numBytes > 0)
        {
          if (numBytes > count)
            numBytes = count;
          Buffer.BlockCopy(array, offset, _buffer, _writePos, numBytes);
          _writePos += numBytes;
          if (count == numBytes) return;
          offset += numBytes;
          count -= numBytes;
        }
        // Reset our buffer.  We essentially want to call FlushWrite 
        // without calling Flush on the underlying Stream.
        _s.Write(_buffer, 0, _writePos);
        _writePos = 0;
      }
      // If the buffer would slow writes down, avoid buffer completely. 
      if (count >= _bufferSize)
      {
        _s.Write(array, offset, count);
        return;
      }
      else if (count == 0)
        return;  // Don't allocate a buffer then call memcpy for 0 bytes. 
      if (_buffer == null) _buffer = new byte[_bufferSize];
      // Copy remaining bytes into buffer, to write at a later date. 

      Buffer.BlockCopy(array, offset, _buffer, 0, count);

      _writePos = count;
    }

    static void ErrorSeekNotSupported()
    {
      throw new NotSupportedException("Underlying stream does not support seek operation");
    }

    static void ErrorReadNotSupported()
    {
      throw new NotSupportedException("Underlying stream does not support read operation");
    }

    static void ErrorStreamIsClosed()
    {
      throw new InvalidOperationException("Underlying stream is closed");
    }

    static void ErrorWriteNotSupported()
    {
      throw new NotSupportedException("Underlying stream does not support write operation");
    }

    public override void WriteByte(byte value)
    {
      if (_s == null) ErrorStreamIsClosed();
      if (_writePos == 0)
      {
        if (!_s.CanWrite) ErrorWriteNotSupported();
        if (_readPos < _readLen)
          FlushRead();
        else
        {
          _readPos = 0;
          _readLen = 0;
        }
        if (_buffer == null) _buffer = new byte[_bufferSize];
      }
      if (_writePos == _bufferSize)
        FlushWrite();

      _buffer[_writePos++] = value;
    }

    [SecuritySafeCritical]  // auto-generated 
    public override long Seek(long offset, SeekOrigin origin)
    {
      if (_s == null) ErrorStreamIsClosed();
      if (!_s.CanSeek) ErrorSeekNotSupported();

      // If we've got bytes in our buffer to write, write them out.
      // If we've read in and consumed some bytes, we'll have to adjust 
      // our seek positions ONLY IF we're seeking relative to the current
      // position in the stream. 
      if (_writePos > 0)
      {
        FlushWrite();
      }
      else if (origin == SeekOrigin.Current)
      {
        // Don't call FlushRead here, which would have caused an infinite
        // loop.  Simply adjust the seek origin.  This isn't necessary 
        // if we're seeking relative to the beginning or end of the stream.
        offset -= (_readLen - _readPos);
      }
      /* 
      _readPos = 0;
      _readLen = 0;
      return _s.Seek(offset, origin);
      */
      long oldPos = _s.Position + (_readPos - _readLen);
      long pos = _s.Seek(offset, origin);

      // We now must update the read buffer.  We can in some cases simply
      // update _readPos within the buffer, copy around the buffer so our 
      // Position property is still correct, and avoid having to do more
      // reads from the disk.  Otherwise, discard the buffer's contents.
      if (_readLen > 0)
      {
        // We can optimize the following condition: 
        // oldPos - _readPos <= pos < oldPos + _readLen - _readPos
        if (oldPos == pos)
        {
          if (_readPos > 0)
          {
            //Console.WriteLine("Seek: seeked for 0, adjusting buffer back by: "+_readPos+"  _readLen: "+_readLen);
            Buffer.BlockCopy(_buffer, _readPos, _buffer, 0, _readLen - _readPos);
            _readLen -= _readPos;
            _readPos = 0;
          }
          // If we still have buffered data, we must update the stream's 
          // position so our Position property is correct.
          if (_readLen > 0)
            _s.Seek(_readLen, SeekOrigin.Current);
        }
        else if (oldPos - _readPos < pos && pos < oldPos + _readLen - _readPos)
        {
          int diff = (int)(pos - oldPos);
          //Console.WriteLine("Seek: diff was "+diff+", readpos was "+_readPos+"  adjusting buffer - shrinking by "+ (_readPos + diff));
          Buffer.BlockCopy(_buffer, _readPos + diff, _buffer, 0, _readLen - (_readPos + diff));
          _readLen -= (_readPos + diff);
          _readPos = 0;
          if (_readLen > 0)
            _s.Seek(_readLen, SeekOrigin.Current);
        }
        else
        {
          // Lose the read buffer.
          _readPos = 0;
          _readLen = 0;
        }
      }
      return pos;
    }

    public override void SetLength(long value)
    {
      if (value < 0) throw new ArgumentOutOfRangeException("value");

      if (_s == null) ErrorStreamIsClosed();
      if (!_s.CanSeek) ErrorSeekNotSupported();
      if (!_s.CanWrite) ErrorWriteNotSupported();
      if (_writePos > 0)
      {
        FlushWrite();
      }
      else if (_readPos < _readLen)
      {
        FlushRead();
      }
      _readPos = 0;
      _readLen = 0;

      _s.SetLength(value);
    }
  }
}
#endif
