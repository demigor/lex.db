using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

#if NLOG
using NLog;
#endif

namespace Lex.Db
{
  using Indexing;
  using Mapping;
  using Serialization;

  public abstract class DbTable
  {
    internal IDbTableStorage Storage;

    public abstract Type Type { get; }
    public abstract string Name { get; internal set; }

    public abstract int Count();

    public abstract void Purge();
    public abstract void Compact();
    public abstract void Flush();

    public abstract string this[string property] { get; set; }

    public abstract List<K> AllKeys<K>();
    public abstract IEnumerable AllKeys();
    public abstract K GetMinKey<K>();
    public abstract K GetMaxKey<K>();

    public abstract bool DeleteByKey<K>(K key);
    public abstract int DeleteByKeys<K>(IEnumerable<K> keys);

    internal abstract void LoadIndex(IDbTableReader reader);
    internal abstract void SaveIndex(IDbTableWriter writer, bool crop);
  }

  [DebuggerDisplay("{Name}")]
  public sealed class DbTable<T> : DbTable where T : class
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();
#endif

    internal readonly Metadata<T> Metadata = new Metadata<T>();
    internal IKeyIndex<T> KeyIndex;
    Action<T, IKeyIndex<T>> _autoGen;

    Dictionary<string, IDataIndex<T>> _indexes;

    public override Type Type { get { return typeof(T); } }

    string _name = typeof(T).Name;
    public override string Name { get { return _name; } internal set { _name = value; } }
    readonly DbInstance _db;

    public DbTable(DbInstance db)
    {
      _db = db;
    }

    internal void Initialize(IDbTableStorage table)
    {
      if (KeyIndex == null)
        throw new InvalidOperationException("Type " + typeof(T).Name + " is missing primary key");

      Storage = table;

      Metadata.Add(KeyIndex);
      Metadata.Initialize(_db);

      if (_indexes != null)
        foreach (var i in _indexes)
          Metadata.Add(i.Value);

      Metadata.Prepare();
    }

    #region Metadata ctors

    internal void Add<K>(Expression<Func<T, K>> keyBuilder, MemberInfo key, bool autoGen)
    {
      KeyIndex = new KeyIndex<T, K>(this, keyBuilder.Compile(), key);
      Metadata.Key = DbTypes.GetDbType(keyBuilder.Body.Type);

      if (autoGen && key != null)
        GenerateAutoGen<K>(key);
    }

    void GenerateAutoGen<K>(MemberInfo key)
    {
      var type = typeof(K);

      if (Nullable.GetUnderlyingType(type) != null)
        throw new NotSupportedException("Nullable types are not supported for automatic key generation");

      if (type == typeof(Guid))
        GenerateGuidAutoGen(key);
      else if (type == typeof(int))
        GenerateIntAutoGen(key);
      else if (type == typeof(long))
        GenerateLongAutoGen(key);
      else throw new NotSupportedException("Type '" + type.Name + "' is not supported for automatic key generation");
    }

    void GenerateLongAutoGen(MemberInfo key)
    {
      // void GenAuto(T item, IKeyIndex<T> index)
      // {
      //    if (item.Member == 0) 
      //      item.Member = ((KeyIndex<T,long>)index).GetMaxValue() + 1;
      // }
      var obj = Expression.Parameter(typeof(T));
      var index = Expression.Parameter(typeof(IKeyIndex<T>));
      var member = obj.Member(key);

      var indexLong = Expression.Convert(index, typeof(KeyIndex<T, long>));
      var lastValue = Expression.Call(indexLong, typeof(KeyIndex<T, long>).GetPublicInstanceMethod("GetMaxKey"));
      var setter = Expression.Assign(member, Expression.Add(lastValue, Expression.Constant(1L)));

      var ifBlock = Expression.IfThen(Expression.Equal(member, Expression.Constant(0L)), setter);
      var lambda = Expression.Lambda<Action<T, IKeyIndex<T>>>(ifBlock, obj, index);
      _autoGen = lambda.Compile();
    }

    void GenerateIntAutoGen(MemberInfo key)
    {
      // void GenAuto(T item, IKeyIndex<T> index)
      // {
      //    if (item.Member == 0) 
      //      item.Member = ((KeyIndex<T,int>)index).GetMaxValue() + 1;
      // }
      var obj = Expression.Parameter(typeof(T));
      var index = Expression.Parameter(typeof(IKeyIndex<T>));
      var member = obj.Member(key);

      var indexInt = Expression.Convert(index, typeof(KeyIndex<T, int>));
      var lastValue = Expression.Call(indexInt, typeof(KeyIndex<T, int>).GetPublicInstanceMethod("GetMaxKey"));
      var setter = Expression.Assign(member, Expression.Add(lastValue, Expression.Constant(1)));

      var ifBlock = Expression.IfThen(Expression.Equal(member, Expression.Constant(0)), setter);
      var lambda = Expression.Lambda<Action<T, IKeyIndex<T>>>(ifBlock, obj, index);
      _autoGen = lambda.Compile();
    }

    void GenerateGuidAutoGen(MemberInfo key)
    {
      // void GenAuto(T item, IKeyIndex<T> index)
      // {
      //    if (item.Member == Guid.Empty) 
      //      item.Member = Guid.NewGuid();
      // }
      var obj = Expression.Parameter(typeof(T));
      var index = Expression.Parameter(typeof(IKeyIndex<T>));
      var member = obj.Member(key);

      var gen = Expression.Call(typeof(Guid).GetPublicStaticMethod("NewGuid"));
      var setter = Expression.Assign(member, gen);

      var ifBlock = Expression.IfThen(Expression.Equal(member, Expression.Constant(Guid.Empty)), setter);
      var lambda = Expression.Lambda<Action<T, IKeyIndex<T>>>(ifBlock, obj, index);
      _autoGen = lambda.Compile();
    }

    internal void Add(MemberMap<T> map)
    {
      Debug.WriteLine("Adding {0} Mapping {1}", typeof(T), map.Member.Name);

      Metadata.Add(map);
    }

    internal void Add(Interceptor<T> interceptor)
    {
      Debug.WriteLine("Adding {0} Interceptor", typeof(T));

      Metadata.Interceptor = interceptor;
    }

    internal void Remove(MemberInfo member)
    {
      Debug.WriteLine("Removing {0} Mapping {1}", typeof(T), member.Name);

      Metadata.Remove(member);
    }

    internal void Clear()
    {
      Debug.WriteLine("Removing {0} all mappings", typeof(T));

      Metadata.Clear();
    }

    #endregion

    #region Index ctors

    Dictionary<string, IDataIndex<T>> Indexes
    {
      get
      {
        return _indexes ?? (_indexes = new Dictionary<string, IDataIndex<T>>(StringComparer.OrdinalIgnoreCase));
      }
    }

    internal IDataIndex<T> GetIndex(string name)
    {
      if (_indexes == null)
        return null;

      IDataIndex<T> result;
      _indexes.TryGetValue(name, out result);
      return result;
    }

    internal IDataIndex<T, K> GetIndex<K>(string name)
    {
      return (IDataIndex<T, K>)GetIndex(name);
    }

    internal IDataIndex<T> GetIndex(params MemberInfo[] members)
    {
      if (_indexes == null)
        return null;

      return _indexes.Values.FirstOrDefault(i => members.SequenceEqual(i.Keys));
    }

    internal IDataIndex<T> CreateIndex<I1>(string name, Func<T, I1> getter, MemberInfo[] members, Func<DataNode<I1>, Lazy<T>> lazyCtor) where I1 : IComparable<I1>
    {
      return new DataIndex<T, I1>(this, name, getter, Comparer<I1>.Default, lazyCtor, i => LoadByKey(i.KeyNode.Key), members);
    }

    internal void CreateIndex<I1>(string name, Func<T, I1> getter, MemberInfo member) where I1 : IComparable<I1>
    {
      Indexes[name] = CreateIndex(name, getter, new[] { member },
        i => new Lazy<T, I1>(i.Key, () => LoadByKey(i.KeyNode.Key)));
    }

    internal void CreateIndex<I1, I2>(string name, MemberInfo member1, MemberInfo member2)
    {
      Indexes[name] = CreateIndex(name, BuildGetter<I1, I2>(member1, member2), new[] { member1, member2 },
        i => new Lazy<T, I1, I2>(i.Key, () => LoadByKey(i.KeyNode.Key)));
    }

    internal void CreateIndex<I1, I2, I3>(string name, MemberInfo member1, MemberInfo member2, MemberInfo member3)
    {
      Indexes[name] = CreateIndex(name, BuildGetter<I1, I2, I3>(member1, member2, member3), new[] { member1, member2, member3 },
        i => new Lazy<T, I1, I2, I3>(i.Key, () => LoadByKey(i.KeyNode.Key)));
    }

    static Func<T, Indexer<I1, I2>> BuildGetter<I1, I2>(MemberInfo member1, MemberInfo member2)
    {
      var obj = Expression.Parameter(typeof(T), "obj");
      var tuple = Expression.New(typeof(Indexer<I1, I2>).GetConstructor(new[] { typeof(I1), typeof(I2) }), obj.Member(member1), obj.Member(member2));

      return Expression.Lambda<Func<T, Indexer<I1, I2>>>(tuple, obj).Compile();
    }

    static Func<T, Indexer<I1, I2, I3>> BuildGetter<I1, I2, I3>(MemberInfo member1, MemberInfo member2, MemberInfo member3)
    {
      var obj = Expression.Parameter(typeof(T), "obj");
      var tuple = Expression.New(typeof(Indexer<I1, I2, I3>).GetConstructor(new[] { typeof(I1), typeof(I2), typeof(I3) }),
        obj.Member(member1), obj.Member(member2), obj.Member(member3));

      return Expression.Lambda<Func<T, Indexer<I1, I2, I3>>>(tuple, obj).Compile();
    }

    #endregion

    /// <summary>
    /// Returns actual item count 
    /// </summary>
    /// <returns></returns>
    public override int Count()
    {
      using (ReadScope())
        return KeyIndex.Count;
    }

    #region Scopes

    class Scope<E> : IDisposable
    {
      public readonly ITransactionScope Transaction;
      public readonly E Element;

      public Scope(ITransactionScope scope, E element)
      {
        Transaction = scope;
        Element = element;
      }

      public void Dispose()
      {
        Transaction.Dispose();
      }
    }

    Scope<IDbTableReader> ReadScope()
    {
      var scope = _db.ReadScope();
      return new Scope<IDbTableReader>(scope, scope.GetReader(this));
    }

    Scope<IDbTableWriter> WriteScope(bool autoReload = true)
    {
      var scope = _db.WriteScope();
      return new Scope<IDbTableWriter>(scope, scope.GetWriter(this, autoReload));
    }

    #endregion

    /// <summary>
    /// Lists all current keys 
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <returns>List of key values</returns>
    public override List<K> AllKeys<K>()
    {
      using (ReadScope())
        return (List<K>)KeyIndex.MakeKeyList();
    }

    /// <summary>
    /// Lists all current keys 
    /// </summary>
    /// <returns>Sequence of key values</returns>
    public override IEnumerable AllKeys()
    {
      using (ReadScope())
        return KeyIndex.MakeKeyList();
    }



    /// <summary>
    /// Bulk loads all instances
    /// </summary>
    /// <returns>List of all instances</returns>
    public List<T> LoadAll()
    {
      using (var scope = ReadScope())
        return KeyIndex.Load(scope.Element, Metadata);
    }

    /// <summary>
    /// Bulk loads by key instances
    /// </summary>
    /// <returns>List of all instances</returns>
    public List<T> LoadAll<I1>(string index, I1 key)
    {
      var idx = GetIndex<I1>(index);
      if (idx == null)
        throw new InvalidOperationException("Index {0} not found");

      using (ReadScope())
        return idx.Load(key).ToList();
    }

    /// <summary>
    /// Lazy load via primary key
    /// </summary>
    /// <typeparam name="K">Primary key type</typeparam>
    /// <returns>List of lazy instances</returns>
    public List<Lazy<T, K>> LazyLoad<K>()
    {
      using (ReadScope())
        return KeyIndex.LazyLoad().Cast<Lazy<T, K>>().ToList();
    }

    /// <summary>
    /// Lazy load via normal index
    /// </summary>
    /// <typeparam name="I1">Index type parameter</typeparam>
    /// <param name="index">Name of the index (case insensitive)</param>
    /// <returns>List of lazy instances</returns>
    public List<Lazy<T, I1>> LazyLoad<I1>(string index)
    {
      var idx = GetIndex<I1>(index);
      if (idx == null)
        throw new ArgumentException("index");

      using (ReadScope())
        return idx.LazyLoad().Cast<Lazy<T, I1>>().ToList();
    }

    /// <summary>
    /// Lazy load via normal index
    /// </summary>
    /// <typeparam name="I1">Index type parameter</typeparam>
    /// <param name="index">Name of the index (case insensitive)</param>
    /// <param name="key">Key value</param>
    /// <returns>List of lazy instances</returns>
    public List<Lazy<T, I1>> LazyLoad<I1>(string index, I1 key)
    {
      var idx = GetIndex<I1>(index);
      if (idx == null)
        throw new ArgumentException("index");

      using (ReadScope())
        return idx.LazyLoad(key).Cast<Lazy<T, I1>>().ToList();
    }
    /// <summary>
    /// Lazy load via normal index by two columns
    /// </summary>
    /// <typeparam name="I1">Index type first parameter</typeparam>
    /// <typeparam name="I2">Index type second parameter</typeparam>
    /// <param name="index">Name of the index (case insensitive)</param>
    /// <returns>List of lazy instances</returns>
    public List<Lazy<T, I1, I2>> LazyLoad<I1, I2>(string index)
    {
      var idx = GetIndex(index);
      if (idx == null)
        throw new ArgumentException("index");

      using (ReadScope())
        return idx.LazyLoad().Cast<Lazy<T, I1, I2>>().ToList();
    }

    /// <summary>
    /// Lazy load via normal index by three columns
    /// </summary>
    /// <typeparam name="I1">Index type first parameter</typeparam>
    /// <typeparam name="I2">Index type second parameter</typeparam>
    /// <typeparam name="I3">Index type third parameter</typeparam>
    /// <param name="index">Name of the index (case insensitive)</param>
    /// <returns>List of lazy instances</returns>
    public List<Lazy<T, I1, I2, I3>> LazyLoad<I1, I2, I3>(string index)
    {
      var idx = GetIndex(index);
      if (idx == null)
        throw new ArgumentException("index");

      using (ReadScope())
        return idx.LazyLoad().Cast<Lazy<T, I1, I2, I3>>().ToList();
    }

    /// <summary>
    /// Loads an instance specified by key 
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="key">Key value</param>
    /// <returns>Instance identified by the key, if any</returns>
    public T LoadByKey<K>(K key)
    {
      using (var scope = ReadScope())
      {
        var info = KeyIndex.FindByKey(key, true);
        if (info == null)
          return default(T);

        Metadata.Deserialize(new DataReader(new MStream(scope.Element.ReadData(info.Offset, info.Length))), info.Result);

        return info.Result;
      }
    }

    IEnumerable<T> LoadByKeysCore<K>(IEnumerable<K> keys, bool yieldNotFound)
    {
      using (var scope = ReadScope())
      {
        var reader = scope.Element;

        foreach (var key in keys)
        {
          var info = KeyIndex.FindByKey(key, true);
          if (info == null)
          {
            if (yieldNotFound)
              yield return default(T);
          }
          else
          {
            Metadata.Deserialize(new DataReader(new MStream(reader.ReadData(info.Offset, info.Length))), info.Result);
            yield return info.Result;
          }
        }
      }
    }

    /// <summary>
    /// Bulk load specified instances by key
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="keys">Sequence of keys to load</param>
    /// <param name="yieldNotFound">Specifies that missed keys will be returned as nulls</param>
    /// <returns>List of corresponding instances</returns>
    public IEnumerable<T> LoadByKeys<K>(IEnumerable<K> keys, bool yieldNotFound = false)
    {
      if (keys == null)
        throw new ArgumentNullException();

      return LoadByKeysCore(keys, yieldNotFound);
    }

    /// <summary>
    /// Refreshes specified instance from disk
    /// </summary>
    /// <param name="item">Instance to refresh</param>
    /// <returns>Same instance, updated from disk</returns>
    public T Refresh(T item)
    {
      if (item == null)
        throw new ArgumentNullException();

      using (var scope = ReadScope())
      {
        var info = KeyIndex.Find(item);
        if (info == null)
          return item;

        Metadata.Deserialize(new DataReader(new MStream(scope.Element.ReadData(info.Offset, info.Length))), item);

        return item;
      }
    }

    /// <summary>
    /// Saves specified item sequence, adding or updating as needed
    /// </summary>
    /// <param name="items">Sequence to merge in</param>
    /// <param name="flush">Flush table on disk</param>
    public void Save(IEnumerable<T> items)
    {
      if (items == null)
        throw new ArgumentNullException();

      var ic = Metadata.Interceptor;
      var ms = new MStream();
      var mw = new DataWriter(ms);
      var dirty = false;

      using (var scope = WriteScope())
      {
        var writer = scope.Element;

        foreach (var item in items)
        {
          dirty = true;
          ms.Position = 0;

          if (_autoGen != null)
            _autoGen(item, KeyIndex);

          Metadata.Serialize(ic, mw, item);

          WriteObjectData(writer, item, ms.GetBuffer(), (int)ms.Position);
        }

        if (dirty)
          scope.Transaction.Modified(this);
      }
    }

    /// <summary>
    /// Saves specified item, adding or updating as needed
    /// </summary>
    /// <param name="item">Item to merge in</param>
    /// <param name="flush">Flush table on disk</param>
    public void Save(T item)
    {
      if (item == null)
        throw new ArgumentNullException();

      var ic = Metadata.Interceptor;
      var ms = new MStream();
      var mw = new DataWriter(ms);

      using (var scope = WriteScope())
      {
        if (_autoGen != null)
          _autoGen(item, KeyIndex);

        Metadata.Serialize(ic, mw, item);

        WriteObjectData(scope.Element, item, ms.GetBuffer(), (int)ms.Length);
        scope.Transaction.Modified(this);
      }
    }

    /// <summary>
    /// Fast check index header and reloads it when changed
    /// </summary>
    void ReadIndexes(Stream stream)
    {
      var reader = new DataReader(stream);
      Metadata.Read(reader);
      try
      {
        KeyIndex.Read(reader);
      }
      catch (InvalidOperationException)
      {
        Metadata.ClearProperties();
      }

      var name = reader.ReadString();
      if (name != "" && _indexes != null)
      {
        KeyIndex.KeyMap = null;

        do
        {
          GetIndex(name).Read(reader);
          name = reader.ReadString();
        } while (name != "");

        KeyIndex.KeyMap = null;
      }
    }

    MStream WriteIndex()
    {
      var ms = new MStream();
      var writer = new DataWriter(ms);

      Metadata.Write(writer);
      KeyIndex.Write(writer);

      if (_indexes != null)
        foreach (var i in _indexes)
        {
          writer.Write(i.Key);
          i.Value.Write(writer);
        }

      writer.Write("");

      return ms;
    }

    public override void Flush()
    {
      Storage.Flush();
    }

    /// <summary>
    /// Deletes item specified by key
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="key">Key of item to delete</param>
    /// <param name="flush">Flush table on disk</param>
    /// <returns>Returns true if item deleted</returns>
    public override bool DeleteByKey<K>(K key)
    {
      using (var scope = WriteScope())
        if (KeyIndex.RemoveByKey(key))
        {
          scope.Transaction.Modified(this);
          return true;
        }

      return false;
    }

    /// <summary>
    /// Deletes items specified by key sequence 
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="keys">Key sequence to delete</param>
    /// <param name="flush">Flush table on disk</param>
    /// <returns>Returns count of the deleted items</returns>
    public override int DeleteByKeys<K>(IEnumerable<K> keys)
    {
      if (keys == null)
        throw new ArgumentNullException();

      using (var scope = WriteScope())
      {
        var result = keys.Count(key => KeyIndex.RemoveByKey(key));

        if (result > 0)
          scope.Transaction.Modified(this, true);

        return result;
      }
    }

    /// <summary>
    /// Deletes specified item
    /// </summary>
    /// <param name="item">Item to delete</param>
    /// <param name="flush">Flush table on disk</param>
    public bool Delete(T item)
    {
      if (item == null)
        throw new ArgumentNullException();

      using (var scope = WriteScope())
        if (KeyIndex.Remove(item))
        {
          scope.Transaction.Modified(this);
          return true;
        }

      return false;
    }

    /// <summary>
    /// Deletes specified item sequence
    /// </summary>
    /// <param name="items">Sequence of items to delete</param>
    /// <param name="flush">Flush table on disk</param>
    public int Delete(IEnumerable<T> items)
    {
      var result = 0;

      using (var scope = WriteScope())
      {
        foreach (var i in items)
          if (KeyIndex.Remove(i))
            result++;

        if (result > 0)
          scope.Transaction.Modified(this);
      }

      return result;
    }

    /// <summary>
    /// Compacts data file
    /// </summary>
    public override void Compact()
    {
      using (var compacter = Storage.BeginCompact())
      {
        LoadIndex(compacter);
        KeyIndex.Compact(compacter);
        SaveIndex(compacter, true);
      }
    }

    /// <summary>
    /// Truncates all items
    /// </summary>
    public override void Purge()
    {
      using (var scope = WriteScope(false))
      {
        PurgeCore();
        scope.Element.Purge();
      }
    }

    void PurgeCore()
    {
      Metadata.ClearProperties();
      KeyIndex.Purge();

      if (_indexes != null)
        foreach (var i in _indexes)
          i.Value.Purge();
    }

    void WriteObjectData(IDbTableWriter writer, T instance, byte[] data, int length)
    {
      var key = KeyIndex.Update(instance, length);

      var offset = key.Offset;

      writer.WriteData(data, offset, length);

      if (_indexes != null)
        foreach (var i in _indexes)
          i.Value.Update(key, instance);
    }

    DateTimeOffset _tableTs;

    internal override void LoadIndex(IDbTableReader reader)
    {
      var ts = reader.Ts;

      lock (KeyIndex)
      {
        if (ts > _tableTs)
        {
          _tableTs = ts;
          var index = reader.ReadIndex();
          if (index == null)
            PurgeCore();
          else
            ReadIndexes(new MStream(index));
        }
      }
    }

    internal override void SaveIndex(IDbTableWriter writer, bool crop)
    {
      var s = WriteIndex();
      writer.WriteIndex(s.GetBuffer(), (int)s.Length);

      if (crop)
        writer.CropData(KeyIndex.GetFileSize());
    }

    public override string this[string property]
    {
      get
      {
        using (ReadScope())
          return Metadata[property];
      }
      set
      {
        using (var scope = WriteScope())
        {
          var ov = Metadata[property];

          if (ov != value)
          {
            Metadata[property] = value;
            scope.Transaction.Modified(this);
          }
        }
      }
    }

    public override K GetMinKey<K>()
    {
      using (ReadScope())
        return (K)KeyIndex.MinKey();
    }

    public override K GetMaxKey<K>()
    {
      using (ReadScope())
        return (K)KeyIndex.MaxKey();
    }
  }
}
