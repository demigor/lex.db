using System.IO;

namespace Lex.Db
{
  sealed class MStream : MemoryStream
  {
    public MStream() { }

    public MStream(int capacity) : base(capacity) { }

    public MStream(byte[] data) : base(data) { }

    public MStream(byte[] data, bool writable) : base(data, writable) { }

#if NETFX_CORE || PORTABLE
    public byte[] GetBuffer() 
    {
      return ToArray();
    }
#endif
  }
}
