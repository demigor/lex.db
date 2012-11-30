using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Lex.Db
{
  public class DbInstance : IDisposable
  {
    static readonly IDbStorage Storage = new DbStorage();
    readonly IDbSchemaStorage _schema;
    bool _sealed;

    Dictionary<Type, TypeMap> _maps = new Dictionary<Type, TypeMap>();
    Dictionary<Type, DbTable> _tables;

    public DbInstance(string path)
    {
      _schema = Storage.OpenSchema(path);
    }

    void CheckNotSealed()
    {
      if (_sealed)
        throw new InvalidOperationException("DbInstance is already initialized");
    }

    void CheckSealed()
    {
      if (!_sealed)
        throw new InvalidOperationException("DbInstance is not initialized");
    }

    public void Initialize()
    {
      CheckNotSealed();

      if (_maps.Values.Select(i => i.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() != _maps.Count)
        throw new InvalidOperationException("Duplicate table names detected");

      if (_maps.Values.Select(i => i.Name).Any(string.IsNullOrWhiteSpace))
        throw new InvalidOperationException("Invalid table name detected");

      _schema.Open();

      _tables = (from m in _maps.Values select m.Initialize(_schema.GetTable(m.Name))).ToDictionary(i => i.Type);

      _sealed = true;

      #region Drop maps - we don't need them anymore
    
      foreach (var m in _maps.Values)
        m.Clear();

      _maps = null;

      #endregion
    }

    public string Path { get { return _schema.Path; } }

    internal Type GetKeyType(Type type)
    {
      // only valid during initialization phase
      TypeMap map;

      if (_maps.TryGetValue(type, out map))
        return map.KeyType;

      return null;
    }

    public bool HasMap(Type type)
    {
      return _maps != null ? _maps.ContainsKey(type) : _tables.ContainsKey(type);
    }

    public bool HasMap<T>()
    {
      return HasMap(typeof(T));
    }

    public TypeMap<T> Map<T>() where T : class, new()
    {
      CheckNotSealed();

      lock (_maps)
      {
        TypeMap result;

        if (_maps.TryGetValue(typeof(T), out result))
          return (TypeMap<T>)result;

        var map = new TypeMap<T>(this);

        _maps[typeof(T)] = map;

        return map;
      }
    }

    public DbTable<T> Table<T>() where T : class
    {
      CheckSealed();

      DbTable result;

      if (!_tables.TryGetValue(typeof(T), out result))
        throw new ArgumentException(string.Format("Type {0} is not registered with this DbInstance", typeof(T).Name));

      return (DbTable<T>)result;
    }

    readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    [ThreadStatic]
    static ITransactionScope Current;

    internal ITransactionScope ReadScope()
    {
#if NLOG
      if (Log.IsTraceEnabled)
        Log.Trace("Begin read transaction");
#endif
      var result = Current;

      if (result == null)
        result = new BulkReadScope();

      result.AddRef();

      return Current = result;
    }

    internal ITransactionScope WriteScope()
    {
#if NLOG
      if (Log.IsTraceEnabled)
        Log.Trace("Begin write transaction");
#endif
      var result = Current;

      if (result is BulkReadScope)
        throw new InvalidOperationException("Nested write transaction inside read only one");

      if (result == null)
        result = new BulkWriteScope();

      result.AddRef();

      return Current = result;
    }

    class BulkReadScope : Dictionary<DbTable, IDbTableReader>, ITransactionScope
    {
      int _usage = 0;

      void AddRef()
      {
        _usage++;
      }

      public void Dispose()
      {
        _usage--;

        if (_usage == 0)
        {
          foreach (var reader in Values)
            reader.Dispose();

          Current = null;
        }
      }

      public IDbTableReader GetReader(DbTable table)
      {
        IDbTableReader reader;

        if (!TryGetValue(table, out reader))
        {
#if NLOG
      if (Log.IsTraceEnabled)
        Log.Trace("Begin read {0}", table.Type.Name);
#endif
          reader = table.Storage.BeginRead();
          try
          {
            table.LoadIndex(reader);
            this[table] = reader;
          }
          catch
          {
            reader.Dispose();
            throw;
          }
        }

        return reader;
      }

      public IDbTableWriter GetWriter(DbTable table, bool autoReload)
      {
        throw new InvalidOperationException("Attempt to write in read only transaction");
      }

      void ITransactionScope.AddRef()
      {
        _usage++;
      }

      public void Modified(DbTable table, bool crop)
      {
        throw new InvalidOperationException("Attempt to commit read only transaction");
      }
    }

    class CommitInfo
    {
      public IDbTableWriter Writer;
      public bool Crop, Modified;
    }

    class BulkWriteScope : Dictionary<DbTable, CommitInfo>, ITransactionScope
    {
      int _usage = 0;

      public void AddRef()
      {
        _usage++;
      }

      public void Dispose()
      {
        _usage--;

        if (_usage == 0)
        {
          foreach (var i in this)
          {
            var info = i.Value;
            try
            {
              if (info.Modified)
                i.Key.SaveIndex(info.Writer, info.Crop);
            }
            finally
            {
              info.Writer.Dispose();
            }
          }

          Current = null;
        }
      }

      public IDbTableReader GetReader(DbTable table)
      {
        return GetWriter(table, true);
      }

      public IDbTableWriter GetWriter(DbTable table, bool autoReload)
      {
        CommitInfo info;

        if (!TryGetValue(table, out info))
        {
#if NLOG
      if (Log.IsTraceEnabled)
        Log.Trace("Begin write {0}", table.Type.Name);
#endif
          var writer = table.Storage.BeginWrite();
          try
          {
            if (autoReload)
              table.LoadIndex(writer);

            this[table] = info = new CommitInfo { Writer = writer };
          }
          catch
          {
            writer.Dispose();
            throw;
          }
        }

        return info.Writer;
      }

      public void Modified(DbTable table, bool crop)
      {
        var info = this[table];
        info.Crop |= crop;
        info.Modified = true;
      }
    }

    public void BulkRead(Action action)
    {
      _lock.EnterReadLock();
      try
      {
        using (ReadScope())
          action();
      }
      finally
      {
        _lock.ExitReadLock();
      }
    }

    public void BulkWrite(Action action)
    {
      _lock.EnterWriteLock();
      try
      {
        using (WriteScope())
          action();
      }
      finally
      {
        _lock.ExitWriteLock();
      }
    }

    public List<T> LoadAll<T>() where T : class
    {
      return Table<T>().LoadAll();
    }

    public T LoadByKey<T>(object key) where T : class
    {
      return Table<T>().LoadByKey(key);
    }

    public T LoadByKey<T, K>(K key) where T : class
    {
      return Table<T>().LoadByKey(key);
    }

    public int Count<T>() where T : class
    {
      return Table<T>().Count();
    }

    public void Save<T>(T item) where T : class
    {
      Table<T>().Save(item);
    }

    public void Save<T>(params T[] items) where T : class
    {
      Table<T>().Save(items);
    }

    public void Save<T>(IEnumerable<T> items) where T : class
    {
      Table<T>().Save(items);
    }

    public int DeleteByKeys<T, K>(IEnumerable<K> keys) where T : class
    {
      return Table<T>().DeleteByKeys(keys);
    }

    public bool DeleteByKey<T>(object key) where T : class
    {
      return Table<T>().DeleteByKey(key);
    }

    public bool DeleteByKey<T, K>(K key) where T : class
    {
      return Table<T>().DeleteByKey(key);
    }

    public bool Delete<T>(T item) where T : class
    {
      return Table<T>().Delete(item);
    }

    public T Refresh<T>(T item) where T : class
    {
      return Table<T>().Refresh(item);
    }

    public List<K> LoadAllKeys<T, K>() where T : class
    {
      return Table<T>().AllKeys<K>();
    }

    public IEnumerable LoadAllKeys<T>() where T : class
    {
      return Table<T>().AllKeys();
    }

    public void Flush()
    {
      foreach (var i in _tables.Values)
        i.Flush();
    }

    public void Flush<T>() where T : class
    {
      Table<T>().Flush();
    }

    public void Compact()
    {
      _lock.EnterWriteLock();
      try
      {
        using (WriteScope())
          foreach (var i in _tables.Values)
            i.Compact();
      }
      finally
      {
        _lock.ExitWriteLock();
      }
    }

    public void Purge()
    {
      _lock.EnterWriteLock();
      try
      {
        _schema.Purge();
      }
      finally
      {
        _lock.ExitWriteLock();
      }
    }

    public void Purge<T>() where T : class
    {
      Table<T>().Purge();
    }

    public void Dispose()
    {
      _lock.Dispose();
    }
  }
}
