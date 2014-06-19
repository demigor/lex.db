using System.Runtime.CompilerServices;

namespace Lex.Db
{
  static class Hash
  {
    public static uint Compute(byte[] data)
    {
      if (data == null)
        return 0;

      var len = (uint)data.Length;
      if (len <= 0)
        return 0;

      var hash = len;
      var rem = len & 3;
    
      len = len >> 2;

      var idx = 0;

      for (; len > 0; len--)
      {
        hash += GetUInt(data, idx);
        var tmp = (GetUInt(data, idx + 2) << 11) ^ hash;
        hash = (hash << 16) ^ tmp;
        idx += 4;
        hash += hash >> 11;
      }

      switch (rem)
      {
        case 3:
          hash += GetUInt(data, idx);
          hash ^= hash << 16;
          hash ^= (uint)data[idx + 2] << 18;
          hash += hash >> 11;
          break;

        case 2:
          hash += GetUInt(data, idx);
          hash ^= hash << 11;
          hash += hash >> 17;
          break;

        case 1:
          hash += data[idx];
          hash ^= hash << 10;
          hash += hash >> 1;
          break;
      }

      hash ^= hash << 3;
      hash += hash >> 5;
      hash ^= hash << 4;
      hash += hash >> 17;
      hash ^= hash << 25;
      hash += hash >> 6;

      return hash;
    }

#if !NET40 && !PORTABLE
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    static uint GetUInt(byte[] data, int idx)
    {
      return data[idx] + ((uint)data[idx + 1] << 8);
    }
  }
}
