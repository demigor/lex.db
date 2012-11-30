using System;

namespace Lex.Db
{
  public interface IDbTableStorage
  {
    IDbTableReader BeginRead();
    IDbTableWriter BeginWrite();
    IDbTableWriter BeginCompact();
    void Flush();
  }

  public interface IDbTableReader : IDisposable
  {
    DateTimeOffset Ts { get; }
    byte[] ReadIndex();
    byte[] ReadData(long position, int length);
  }

  public interface IDbTableWriter : IDbTableReader
  {
    void WriteIndex(byte[] data, int length);
    void WriteData(byte[] data, long position, int length);
    void CopyData(long position, long target, int length);
    void CropData(long position);
    void Purge();
  }
}
