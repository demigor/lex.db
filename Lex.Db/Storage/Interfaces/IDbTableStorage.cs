using System;

namespace Lex.Db
{
  /// <summary>
  /// Table size information container
  /// </summary>
  public class DbTableInfo
  {
    /// <summary>
    /// Actual size of the index file
    /// </summary>
    public long IndexSize;

    /// <summary>
    /// Actual size of the data file (including gaps) 
    /// </summary>
    public long DataSize;

    /// <summary>
    /// Effective size of the data file  (excluding gaps)
    /// </summary>
    public long EffectiveDataSize;

    /// <summary>
    /// Summs all numeric properties
    /// </summary>
    public static DbTableInfo operator +(DbTableInfo a, DbTableInfo b)
    {
      return new DbTableInfo
      {
        DataSize = a.DataSize + b.DataSize,
        IndexSize = a.IndexSize + b.IndexSize,
        EffectiveDataSize = a.EffectiveDataSize + b.EffectiveDataSize
      };
    }
  }

  interface IDbTableStorage
  {
    IDbTableReader BeginRead();
    IDbTableWriter BeginWrite();
    IDbTableWriter BeginCompact();
    void Flush();
  }

  interface IDbTableReader : IDisposable
  {
    DateTimeOffset Ts { get; }
    byte[] ReadIndex();
    byte[] ReadData(long position, int length);
    DbTableInfo GetInfo();
  }

  interface IDbTableWriter : IDbTableReader
  {
    DateTimeOffset WriteIndex(byte[] data, int length);
    void WriteData(byte[] data, long position, int length);
    void CopyData(long position, long target, int length);
    void CropData(long position);
    void Purge();
  }
}
