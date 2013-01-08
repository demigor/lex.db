#if NETFX_CORE
using System;
using System.IO;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading;
using Windows.Storage;
using Windows.Storage.Streams;

namespace Lex.Db.WindowsStorage
{
  using Streams;

  class DbTableStorage : IDbTableStorage
  {
    readonly StorageFolder _folder;
    readonly string _indexName;
    readonly string _dataName;
    StorageFile _indexFile, _dataFile;

    public DbTableStorage(StorageFolder folder, string name)
    {
      _folder = folder;
      _indexName = name + ".index";
      _dataName = name + ".data";

      using (var awaiter = new Awaiter(true))
        Open(awaiter);
    }

    internal void Open(Awaiter awaiter)
    {
      _indexFile = awaiter.Await(_folder.CreateFileAsync(_indexName, CreationCollisionOption.OpenIfExists));
      _dataFile = awaiter.Await(_folder.CreateFileAsync(_dataName, CreationCollisionOption.OpenIfExists));
    }

    readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    public void Flush()
    {
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
      protected IRandomAccessStream _indexStream;
      protected Stream _readStream;
      protected readonly DbTableStorage _table;
      readonly Action _finalizer;
      DateTimeOffset _ts;
      readonly protected Awaiter _awaiter = new Awaiter(true);

      public Reader(DbTableStorage table, Action finalizer)
      {
        _table = table;
        _finalizer = finalizer;
        CreateStreams();
        UpdateTs();
      }

      protected void UpdateTs()
      {
        _ts = _awaiter.Await(_table._indexFile.GetBasicPropertiesAsync()).DateModified;
      }

      protected virtual void CreateStreams()
      {
        _indexStream = _awaiter.Await(_table._indexFile.OpenAsync(FileAccessMode.Read));
        _readStream = _awaiter.Await(_table._dataFile.OpenAsync(FileAccessMode.Read)).AsBuffered();
      }

      public DateTimeOffset Ts { get { return _ts; } }

      public DbTableInfo GetInfo()
      {
        return new DbTableInfo
        {
          DataSize = _readStream.Length,
          IndexSize = (long)_indexStream.Size
        };
      }

      public byte[] ReadIndex()
      {
        if (_indexStream.Size == 0)
          return null;

        var result = new byte[_indexStream.Size];
        _awaiter.Await(_indexStream.ReadAsync(result.AsBuffer(), (uint)result.Length, InputStreamOptions.Partial));
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

      void IDisposable.Dispose()
      {
        Dispose();
        _awaiter.Dispose();
      }

      protected virtual void Dispose()
      {
        _indexStream.Dispose();
        _readStream.Dispose();

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
        _indexStream = _awaiter.Await(_table._indexFile.OpenAsync(FileAccessMode.ReadWrite));
        _readStream = _awaiter.Await(_table._dataFile.OpenAsync(FileAccessMode.ReadWrite)).AsBuffered();
        _writeStream = _readStream;
      }

      public void WriteIndex(byte[] data, int length)
      {
        _indexStream.Seek(0);
        _awaiter.Await(_indexStream.WriteAsync(data.AsBuffer(0, length)));
        _indexStream.Size = (ulong)length;
        UpdateTs();
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
        _indexStream.Size = 0;
      }

      public void CropData(long size)
      {
        _writeStream.SetLength(size);
      }

      protected override void Dispose()
      {
        _writeStream.Dispose();

        base.Dispose();
      }
    }

    class Compacter : Writer
    {
      StorageFile _backup;

      public Compacter(DbTableStorage table, Action finalizer) : base(table, finalizer) { }

      protected override void CreateStreams()
      {
        _backup = _table._dataFile;

        _awaiter.Await(_table._dataFile.RenameAsync(_table._dataName + ".bak"));
        _table._dataFile = _awaiter.Await(_table._folder.CreateFileAsync(_table._dataName, CreationCollisionOption.OpenIfExists));

        base.CreateStreams();

        _readStream = _awaiter.Await(_backup.OpenAsync(FileAccessMode.Read)).AsBuffered();
      }

      protected override void Dispose()
      {
        base.Dispose();

        _awaiter.Await(_backup.DeleteAsync());
      }
    }
  }
}
#endif