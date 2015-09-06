#if !NETFX_CORE && !PORTABLE
using System;
using System.IO;
using System.IO.IsolatedStorage;
using System.Threading;

namespace Lex.Db.IsolatedStorage
{
  class DbTableStorage : IDbTableStorage
  {
    readonly IsolatedStorageFile _storage;
    readonly string _indexName;
    readonly string _dataName;

    public DbTableStorage(IsolatedStorageFile storage, string path, string name)
    {
      _storage = storage;
      _indexName = Path.Combine(path, name + ".index");
      _dataName = Path.Combine(path, name + ".data");
    }

    readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    public void Flush()
    {
    }

    const int BufferSize = 256 * 1024; // 256K

    Stream OpenRead(string name, bool buffered = false)
    {
      for (int i = 0; i < 10; i++)
        try
        {
          Stream s = _storage.OpenFile(name, FileMode.OpenOrCreate, FileAccess.Read, FileShare.Read);
          return buffered ? new BufferedStream(s, BufferSize) : s;
        }
        catch (IsolatedStorageException)
        {
          Thread.Sleep(100);
        }

      throw new IOException("Cannot aquire read lock");
    }

    Stream OpenWrite(string name, bool buffered = false)
    {
      for (int i = 0; i < 10; i++)
        try
        {
          Stream s = _storage.OpenFile(name, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
          return buffered ? new BufferedStream(s, BufferSize) : s;
        }
        catch (IsolatedStorageException)
        {
          Thread.Sleep(100);
        }

      throw new IOException("Cannot aquire read lock");
    }

    public IDbTableReader BeginRead()
    {
      _lock.EnterReadLock();
      try
      {
        return new Reader(this, _lock.ExitReadLock);
      }
      catch
      {
        _lock.ExitReadLock();
        throw;
      }
    }

    public IDbTableWriter BeginWrite()
    {
      _lock.EnterWriteLock();
      try
      {
        return new Writer(this, _lock.ExitWriteLock);
      }
      catch
      {
        _lock.ExitWriteLock();
        throw;
      }
    }

    public IDbTableWriter BeginCompact()
    {
      _lock.EnterWriteLock();
      try
      {
        return new Compacter(this, _lock.ExitWriteLock);
      }
      catch
      {
        _lock.ExitWriteLock();
        throw;
      }
    }

    class Reader : IDbTableReader
    {
      protected Stream _readStream, _indexStream;
      protected readonly DbTableStorage _table;
      readonly Action _finalizer;
      internal DateTimeOffset _ts;

      public Reader(DbTableStorage table, Action finalizer)
      {
        _table = table;
        _finalizer = finalizer;
        CreateStreams();
        UpdateTs();
      }

      protected void UpdateTs()
      {
        _ts = _table._storage.GetLastWriteTime(_table._indexName);
      }

      protected virtual void CreateStreams()
      {
        _indexStream = _table.OpenRead(_table._indexName);
        try
        {
          _readStream = _table.OpenRead(_table._dataName, true);
        }
        catch
        {
          _indexStream.Dispose();
          throw;
        }
      }

      public DateTimeOffset Ts { get { return _ts; } }

      public DbTableInfo GetInfo()
      {
        return new DbTableInfo
        {
          DataSize = _readStream.Length,
          IndexSize = _indexStream.Length
        };
      }

      public byte[] ReadIndex()
      {
        if (_indexStream.Length == 0)
          return null;

        var result = new byte[_indexStream.Length];
        _indexStream.Read(result, 0, result.Length);
        return result;
      }

      public byte[] ReadData(long position, int length)
      {
        if (_readStream.Position != position)
          _readStream.Seek(position, SeekOrigin.Begin);

        var result = new byte[length];

        _readStream.Read(result, 0, length);

        return result;
      }

      public virtual void Dispose()
      {
        _readStream.Dispose();
        _indexStream.Dispose();

        _finalizer();
      }
    }

    class Writer : Reader, IDbTableWriter
    {
      Stream _writeStream;

      public Writer(DbTableStorage table, Action finalizer)
        : base(table, finalizer)
      {
      }

      protected override void CreateStreams()
      {
        _indexStream = _table.OpenWrite(_table._indexName);
        try
        {
          _readStream = _table.OpenWrite(_table._dataName, true);
          _writeStream = _readStream;
        }
        catch
        {
          _indexStream.Dispose();
          throw;
        }
      }

      public DateTimeOffset WriteIndex(byte[] data, int length)
      {
        _indexStream.SetLength(0);
        _indexStream.Write(data, 0, length);
        _indexStream.Dispose();

        UpdateTs();

        return _ts;
      }

      public void CopyData(long position, long target, int length)
      {
        var data = new byte[length];

        if (_readStream.Position != position)
          _readStream.Seek(position, SeekOrigin.Begin);

        _readStream.Read(data, 0, length);

        if (_writeStream.Position != target)
          _writeStream.Seek(target, SeekOrigin.Begin);

        _writeStream.Write(data, 0, length);
      }

      public void WriteData(byte[] data, long position, int length)
      {
        if (_writeStream.Position != position)
          _writeStream.Seek(position, SeekOrigin.Begin);

        _writeStream.Write(data, 0, length);
      }

      public void Purge()
      {
        _writeStream.SetLength(0);
        _indexStream.SetLength(0);
      }

      public void CropData(long size)
      {
        _writeStream.SetLength(size);
      }

      public override void Dispose()
      {
        _writeStream.Dispose();
        base.Dispose();
      }
    }

    class Compacter : Writer
    {
      public Compacter(DbTableStorage table, Action finalizer)
        : base(MoveFile(table), finalizer)
      {
        _readStream = _table.OpenRead(GetBackupName(_table), true);
      }

      static DbTableStorage MoveFile(DbTableStorage table)
      {
        var backup = GetBackupName(table);

        if (table._storage.FileExists(backup))
          table._storage.DeleteFile(backup);

        if (table._storage.FileExists(table._dataName))
          table._storage.MoveFile(table._dataName, backup);

        return table;
      }

      static string GetBackupName(DbTableStorage table)
      {
        return table._dataName + ".bak";
      }

      public override void Dispose()
      {
        base.Dispose();

        _table._storage.DeleteFile(GetBackupName(_table));
      }
    }
  }
}
#endif