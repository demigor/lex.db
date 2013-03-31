using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Lex.Db
{
  /// <summary>
  /// Database access and management
  /// </summary>
  public class DbInstance : IDisposable
  {
    static readonly IDbStorage Storage = new DbStorage();
    readonly IDbSchemaStorage _schema;
    bool _sealed, _disposed;

    Dictionary<Type, TypeMap> _maps = new Dictionary<Type, TypeMap>();
    Dictionary<Type, DbTable> _tables;

    /// <summary>
    /// Creates database instance with specified path
    /// </summary>
    /// <param name="path">Path to database storage folder (relative to default app storage)</param>
    public DbInstance(string path)
    {
      _schema = Storage.OpenSchema(path);
    }

#if NETFX_CORE
    /// <summary>
    /// Creates database instance with specified path
    /// </summary>
    /// <param name="path">Path to database storage folder (relative to home)</param>
    /// <param name="home">Home folder</param>
    public DbInstance(string path, Windows.Storage.StorageFolder home)
    {
      if (home == null)
        throw new ArgumentNullException("home");

      _schema = Storage.OpenSchema(path, home);
    }
#endif

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

    /// <summary>
    /// Initializes database
    /// </summary>
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

    /// <summary>
    /// Indicates path to database storage folder (relative to default app storage)
    /// </summary>
    public string Path { get { return _schema.Path; } }

    internal Type GetKeyType(Type type)
    {
      // only valid during initialization phase
      TypeMap map;

      if (_maps.TryGetValue(type, out map))
        return map.KeyType;

      return null;
    }

    /// <summary>
    /// Gets database instance storage statistics
    /// </summary>
    /// <returns>Database instance storage statistics</returns>
    public DbTableInfo GetInfo()
    {
      CheckSealed();

      _lock.EnterReadLock();
      try
      {
        var result = new DbTableInfo();

        foreach (var t in _tables)
          result += t.Value.GetInfo();

        return result;
      }
      finally
      {
        _lock.ExitReadLock();
      }
    }

    /// <summary>
    /// Indicates whether specified entity type is mapped in database
    /// </summary>
    /// <param name="type">Entity type</param>
    /// <returns>True if type is mapped in database, false otherwise</returns>
    public bool HasMap(Type type)
    {
      if (type == null)
        throw new ArgumentNullException("type");

      return _maps != null ? _maps.ContainsKey(type) : _tables.ContainsKey(type);
    }

    /// <summary>
    /// Indicates whether specified entity type is mapped in database
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>True if type is mapped in database, false otherwise</returns>
    public bool HasMap<T>()
    {
      return HasMap(typeof(T));
    }

    /// <summary>
    /// Maps specified entity type in database and provides mapping infrastructure
    /// </summary>
    /// <typeparam name="T">Entity type</typeparam>
    /// <returns>Entity T mapping configurator</returns>
    public TypeMap<T> Map<T>() where T : class, new()
    {
      return Map<T>(Ctor<T>.New);
    }

    /// <summary>
    /// Maps specified entity type in database and provides mapping infrastructure
    /// </summary>
    /// <typeparam name="T">Entity prototype</typeparam>
    /// <param name="ctor">Entity implementation constructor</param>
    /// <returns>Entity T mapping configurator</returns>
    public TypeMap<T> Map<T>(Func<T> ctor) where T : class
    {
      if (ctor == null)
        throw new ArgumentNullException("ctor");

      CheckNotSealed();

      lock (_maps)
      {
        TypeMap result;

        if (_maps.TryGetValue(typeof(T), out result))
          return (TypeMap<T>)result;

        var map = new TypeMap<T>(this, ctor);

        _maps[typeof(T)] = map;

        return map;
      }
    }

    /// <summary>
    /// Maps specified entity type in database and provides mapping infrastructure
    /// </summary>
    /// <typeparam name="T">Entity prototype</typeparam>
    /// <typeparam name="TClass">Entity implementation type</typeparam>
    /// <returns>Entity T mapping configurator</returns>
    public TypeMap<T> Map<T, TClass>()
      where T : class
      where TClass : T, new()
    {
      return Map<T>(Ctor<T, TClass>.New);
    }

    /// <summary>
    /// Provides database table infrastructure to read/write/query entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public DbTable<T> Table<T>() where T : class
    {
      CheckSealed();

      DbTable result;

      if (!_tables.TryGetValue(typeof(T), out result))
        throw new ArgumentException(string.Format("Type {0} is not registered with this DbInstance", typeof(T).Name));

      return (DbTable<T>)result;
    }

    /// <summary>
    /// global db read/write lock
    /// </summary>
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

    /// <summary>
    /// Performs specified action inside read transaction scope
    /// </summary>
    /// <param name="action">Action to execute inside read transaction scope</param>
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

    /// <summary>
    /// Performs specified action inside write transcantion scope.
    /// </summary>
    /// <param name="action">Action to execute inside write transaction scope</param>
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

    /// <summary>
    /// Loads all entities of specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <returns>Array of all entities</returns>
    public T[] LoadAll<T>() where T : class
    {
      return Table<T>().LoadAll();
    }

    /// <summary>
    /// Loads an entity of specified entity type by specified PK value
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="key">PK value as object</param>
    /// <returns>Entity identified by the PK value, if any</returns>
    public T LoadByKey<T>(object key) where T : class
    {
      return Table<T>().LoadByKey(key);
    }

    /// <summary>
    /// Loads an entity of specified entity type by specified PK value
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="key">PK value</param>
    /// <returns>Entity identified by the PK value, if any</returns>
    public T LoadByKey<T, K>(K key) where T : class
    {
      return Table<T>().LoadByKey(key);
    }

    /// <summary>
    /// Determines number of entities stored in specified entity table
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <returns>Number of entities stored in specified entity table</returns>
    public int Count<T>() where T : class
    {
      return Table<T>().Count();
    }

    /// <summary>
    /// Saves specified entity, adding or updating as needed
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="item">Entity to upsert into table</param>
    public void Save<T>(T item) where T : class
    {
      Table<T>().Save(item);
    }

    /// <summary>
    /// Saves specified entity sequence, adding or updating as needed
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="items">Entity sequence to upsert into table</param>
    public void Save<T>(params T[] items) where T : class
    {
      Table<T>().Save(items);
    }

    /// <summary>
    /// Saves specified entity sequence, adding or updating as needed
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="items">Entity sequence to upsert into table</param>
    public void Save<T>(IEnumerable<T> items) where T : class
    {
      Table<T>().Save(items);
    }

    /// <summary>
    /// Deletes entities specified by key sequence
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="keys">Sequence of key values to specify entities to delete</param>
    /// <returns>Returns count of the deleted entities</returns>
    public int DeleteByKeys<T, K>(IEnumerable<K> keys) where T : class
    {
      return Table<T>().DeleteByKeys(keys);
    }

    /// <summary>
    /// Deletes entity specified by PK value
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="key">Key of entity to delete</param>
    /// <returns>True if entity was deleted</returns>
    public bool DeleteByKey<T>(object key) where T : class
    {
      return Table<T>().DeleteByKey(key);
    }

    /// <summary>
    /// Deletes entity specified by PK value
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="key">Key of entity to delete</param>
    /// <returns>True if entity was deleted</returns>
    public bool DeleteByKey<T, K>(K key) where T : class
    {
      return Table<T>().DeleteByKey(key);
    }

    /// <summary>
    /// Deletes specified entity
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="item">Entity to delete</param>
    /// <returns>True if entity was deleted</returns>
    public bool Delete<T>(T item) where T : class
    {
      return Table<T>().Delete(item);
    }

    /// <summary>
    /// Refreshes specified entity from disk
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <param name="item">Entity to refresh</param>
    /// <returns>Same entity, updated from disk</returns>
    public T Refresh<T>(T item) where T : class
    {
      return Table<T>().Refresh(item);
    }

    /// <summary>
    /// Lists all current key values for specified entity type
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <returns>Array of all current key values for specified entity type</returns>
    public K[] AllKeys<T, K>() where T : class
    {
      return Table<T>().AllKeys<K>();
    }

    /// <summary>
    /// Lists all current key values 
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    /// <returns>Array of all current key values for specified entity type</returns>
    public object[] AllKeys<T>() where T : class
    {
      return Table<T>().AllKeys();
    }

    /// <summary>
    /// Flushes underlying database storage
    /// </summary>
    public void Flush()
    {
      foreach (var i in _tables.Values)
        i.Flush();
    }

    /// <summary>
    /// Flushes the underlying table storage
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    public void Flush<T>() where T : class
    {
      Table<T>().Flush();
    }

    /// <summary>
    /// Compacts all data files in database
    /// </summary>
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

    /// <summary>
    /// Clears the database
    /// </summary>
    public void Purge()
    {
      _lock.EnterWriteLock();
      try
      {
        if (_tables == null)
          _schema.Purge();
        else
          foreach (var t in _tables)
            t.Value.Purge();
      }
      finally
      {
        _lock.ExitWriteLock();
      }
    }

    /// <summary>
    /// Clears the specified entity table
    /// </summary>
    /// <typeparam name="T">Type of the entity</typeparam>
    public void Purge<T>() where T : class
    {
      Table<T>().Purge();
    }

    /// <summary>
    /// Disposes database
    /// </summary>
    public void Dispose()
    {
      _lock.Dispose();
      _disposed = true;
    }

    void CheckDisposed()
    {
      if (_disposed)
        throw new ObjectDisposedException("DbInstance");
    }
  }
}
