using System;
using System.IO;

namespace Lex.Db
{
  static partial class Extensions
  {
    public static void Write(this BinaryWriter writer, TimeSpan value)
    {
      writer.Write(value.Ticks);
    }

    public static TimeSpan ReadTimeSpan(this BinaryReader reader)
    {
      return new TimeSpan(reader.ReadInt64());
    }

    public static void Write(this BinaryWriter writer, DateTime value)
    {
      writer.Write(DateTimeToRaw(value));
    }

    static long DateTimeToRaw(DateTime value)
    {
      return value.Ticks | (((long)value.Kind) << 0x3e);
    }

    static DateTime RawToDateTime(long value)
    {
      return new DateTime(value & 0x3fffffffffffffffL, (DateTimeKind)((value >> 0x3e) & 0x3));
    }

    public static DateTime ReadDateTime(this BinaryReader reader)
    {
      return RawToDateTime(reader.ReadInt64());
    }

    public static void Write(this BinaryWriter writer, Guid guid)
    {
      writer.Write(guid.ToByteArray(), 0, 16);
    }

    public static Guid ReadGuid(this BinaryReader reader)
    {
      return new Guid(reader.ReadBytes(16));
    }

    public static void Write(this BinaryWriter writer, decimal value)
    {
      var bits = decimal.GetBits(value);
      writer.Write(bits[0]);
      writer.Write(bits[1]);
      writer.Write(bits[2]);
      writer.Write(bits[3]);
    }

    public static byte[] ReadArray(this BinaryReader reader)
    {
      return reader.ReadBytes(reader.ReadInt32());
    }

    public static void WriteArray(this BinaryWriter writer, byte[] value)
    {
      writer.Write(value.Length);
      writer.Write(value);
    }

    public static decimal ReadDecimal(this BinaryReader reader)
    {
      return new decimal(new[] { reader.ReadInt32(), reader.ReadInt32(), reader.ReadInt32(), reader.ReadInt32() });
    }

    public static bool Like(this string source, string substring, StringComparison comparison = StringComparison.CurrentCultureIgnoreCase)
    {
      return source != null && substring != null && source.IndexOf(substring, comparison) >= 0;
    }
  }
}
