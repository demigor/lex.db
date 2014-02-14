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

  /// <summary>
  /// Abstract database table interface
  /// </summary>
  public abstract class DbTable
  {
    internal IDbTableStorage Storage;

    /// <summary>
    /// Type of the table entity class
    /// </summary>
    public abstract Type Type { get; }

    /// <summary>
    /// Name of the table
    /// </summary>
    public abstract string Name { get; internal set; }

    /// <summary>
    /// Determines count of entities stored in the table
    /// </summary>
    /// <returns></returns>
    public abstract int Count();

    /// <summary>
    /// Removes all entities from the table
    /// </summary>
    public abstract void Purge();

    /// <summary>
    /// Compacts the data stream of the table
    /// </summary>
    public abstract void Compact();

    /// <summary>
    /// Flushed the underlying data and index streams to disk
    /// </summary>
    public abstract void Flush();

    /// <summary>
    /// Specifies key/values pairs of the table metadata 
    /// </summary>
    /// <param name="property">Metadata property name</param>
    /// <returns>Value of the named metadata property</returns>
    public abstract string this[string property] { get; set; }

    /// <summary>
    /// Gathers all currently used PK values from PK index
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <returns>Typed list of key values</returns>
    public abstract K[] AllKeys<K>();

    /// <summary>
    /// Gathers all currently used PK values from PK index
    /// </summary>
    /// <returns>Untyped IEnumerable of key values</returns>
    public abstract object[] AllKeys();

    /// <summary>
    /// Determines minimal PK value from PK index,
    /// returns default(K) if table is empty
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <returns>Minimal PK value</returns>
    public abstract K GetMinKey<K>();

    /// <summary>
    /// Determines maximal PK value from PK index,
    /// returns default(K) if table is empty
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <returns>Maximal PK value</returns>
    public abstract K GetMaxKey<K>();

    /// <summary>
    /// Deletes single entity by PK value
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <param name="key">Entity PK value</param>
    /// <returns>Returns true if record was deleted, false otherwise</returns>
    public abstract bool DeleteByKey<K>(K key);

    /// <summary>
    /// Deletes single entity by PK value
    /// </summary>
    /// <param name="key">Entity PK value</param>
    /// <returns>Returns true if record was deleted, false otherwise</returns>
    public abstract bool DeleteByKey(object key);

    /// <summary>
    /// Deletes set of entities, determined by their PK values
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <param name="keys">Enumeration of entity PK values</param>
    /// <returns>Returns number of deleted by key entities</returns>
    public abstract int DeleteByKeys<K>(IEnumerable<K> keys);

    /// <summary>
    /// Deletes set of entities, determined by their PK values
    /// </summary>
    /// <param name="keys">Enumeration of entity PK values</param>
    /// <returns>Returns number of deleted by key entities</returns>
    public abstract int DeleteByKeys(IEnumerable<object> keys);

    internal abstract void Read(IDbTableReader reader);
    internal abstract void Write(IDbTableWriter writer, bool crop);

    /// <summary>
    /// Returns table size information 
    /// </summary>
    /// <returns>DbTableInfo instance filled with size info</returns>
    public abstract DbTableInfo GetInfo();
  }

  /// <summary>
  /// Typed database table 
  /// </summary>
  /// <typeparam name="T">Table entity class</typeparam>
  [DebuggerDisplay("{Name}")]
  public sealed class DbTable<T> : DbTable, IEnumerable<T> where T : class
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();
#endif

    internal readonly Metadata<T> Metadata = new Metadata<T>();
    internal readonly Func<T> Ctor;
    internal IKeyIndex<T> KeyIndex;
    Action<T, IKeyIndex<T>> _autoGen;

    Dictionary<string, IDataIndex<T>> _indexes;

    /// <summary>
    /// Type of the table entity class
    /// </summary>
    public override Type Type { get { return typeof(T); } }

    string _name = typeof(T).Name;

    /// <summary>
    /// Name of the table
    /// </summary>
    public override string Name { get { return _name; } internal set { _name = value; } }

    readonly DbInstance _db;

    internal DbTable(DbInstance db, Func<T> ctor)
    {
      _db = db;
      Ctor = ctor;
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

    internal void Add<K>(Expression<Func<T, K>> keyBuilder, MemberInfo key, bool autoGen, IComparer<K> comparer = null)
    {
      KeyIndex = new KeyIndex<T, K>(this, keyBuilder.Compile(), key, comparer);
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
      var lastValue = Expression.Property(indexLong, "MaxKey");
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
      var lastValue = Expression.Property(indexInt, "MaxKey");
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

    internal Lazy<T, I1> LazyCtor<I1>(I1 key, IKeyNode node)
    {
      return new Lazy<T, I1>(this, node.Key, key);
    }

    internal Lazy<T, I1, I2> LazyCtor<I1, I2>(Indexer<I1, I2> key, IKeyNode node)
    {
      return new Lazy<T, I1, I2>(this, node.Key, key);
    }

    internal Lazy<T, I1, I2, I3> LazyCtor<I1, I2, I3>(Indexer<I1, I2, I3> key, IKeyNode node)
    {
      return new Lazy<T, I1, I2, I3>(this, node.Key, key);
    }

    Dictionary<string, IDataIndex<T>> Indexes
    {
      get
      {
        return _indexes ?? (_indexes = new Dictionary<string, IDataIndex<T>>(StringComparer.OrdinalIgnoreCase));
      }
    }

    internal IKeyIndex<T, K> GetPrimaryIndex<K>()
    {
      var result = KeyIndex as IKeyIndex<T, K>;
      if (result == null)
        throw new ArgumentException(string.Format("Primary Index {0} mismatch ({1} expected)", typeof(K).Name, KeyIndex.KeyType.Name));

      return result;
    }

    internal IDataIndex<T> GetIndex(string name)
    {
      if (_indexes == null)
        return null;

      IDataIndex<T> result;
      _indexes.TryGetValue(name, out result);
      return result;
    }

    /// <summary>
    /// Gets primary index query constructor
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <returns>Primary index query constructor</returns>
    public IIndexQuery<T, K> Query<K>()
    {
      return new IndexQuery<T, K>(GetPrimaryIndex<K>());
    }

    internal IDataIndex<T, I1> GetIndex<I1>(string name)
    {
      var result = GetIndex(name) as IDataIndex<T, I1>;
      if (result == null)
        throw new ArgumentException(string.Format("Index {0}<{1}> not found", name, typeof(I1).Name));

      return result;
    }

    /// <summary>
    /// Returns new named index query constructor
    /// </summary>
    /// <typeparam name="I1">Type of the indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <returns>New index query constructor</returns>
    public IIndexQuery<T, I1> IndexQuery<I1>(string name)
    {
      return new IndexQuery<T, I1>(GetIndex<I1>(name));
    }

    /// <summary>
    /// Returns new named index query constructor, set to look for supplied key 
    /// </summary>
    /// <typeparam name="I1">Type of the indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="key">Key value to filter using index</param>
    /// <returns>New index query constructor, set to look for specified key</returns>
    public IIndexQuery<T, I1> IndexQueryByKey<I1>(string name, I1 key)
    {
      return IndexQuery<I1>(name).Key(key);
    }

    internal IDataIndex<T, Indexer<I1, I2>> GetIndex<I1, I2>(string name)
    {
      var result = GetIndex(name) as IDataIndex<T, Indexer<I1, I2>>;
      if (result == null)
        throw new ArgumentException(string.Format("Index {0}<{1}, {2}> not found", name, typeof(I1).Name, typeof(I2).Name));

      return result;
    }

    /// <summary>
    /// Returns new named index query constructor
    /// </summary>
    /// <typeparam name="I1">Type of the first indexed component</typeparam>
    /// <typeparam name="I2">Type of the second indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <returns>New named index query constructor</returns>
    public IIndexQuery<T, I1, I2> IndexQuery<I1, I2>(string name)
    {
      return new IndexQuery<T, I1, I2>(GetIndex<I1, I2>(name));
    }

    /// <summary>
    /// Returns named index query constructor, set to look for supplied key components
    /// </summary>
    /// <typeparam name="I1">Type of the first indexed component</typeparam>
    /// <typeparam name="I2">Type of the second indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="keyPart1">First part of the key to filter using index</param>
    /// <param name="keyPart2">Second part of the key to filter using index</param>
    /// <returns>New named index query constructor, set to look for supplied key components</returns>
    public IIndexQuery<T, I1, I2> IndexQueryByKey<I1, I2>(string name, I1 keyPart1, I2 keyPart2)
    {
      return IndexQuery<I1, I2>(name).Key(keyPart1, keyPart2);
    }

    internal IDataIndex<T, Indexer<I1, I2, I3>> GetIndex<I1, I2, I3>(string name)
    {
      var result = GetIndex(name) as IDataIndex<T, Indexer<I1, I2, I3>>;
      if (result == null)
        throw new ArgumentException(string.Format("Index {0}<{1}, {2}, {3}> not found", name, typeof(I1).Name, typeof(I2).Name, typeof(I3).Name));

      return result;
    }

    /// <summary>
    /// Returns new named index query constructor
    /// </summary>
    /// <typeparam name="I1">Type of the first indexed component</typeparam>
    /// <typeparam name="I2">Type of the second indexed component</typeparam>
    /// <typeparam name="I3">Type of the third indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <returns>New named index query constructor</returns>
    public IIndexQuery<T, I1, I2, I3> IndexQuery<I1, I2, I3>(string name)
    {
      return new IndexQuery<T, I1, I2, I3>(GetIndex<I1, I2, I3>(name));
    }

    /// <summary>
    /// Returns named index query constructor, set to look for supplied key components
    /// </summary>
    /// <typeparam name="I1">Type of the first indexed component</typeparam>
    /// <typeparam name="I2">Type of the second indexed component</typeparam>
    /// <typeparam name="I3">Type of the third indexed component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="keyPart1">First part of the key to filter using index</param>
    /// <param name="keyPart2">Second part of the key to filter using index</param>
    /// <param name="keyPart3">Third part of the key to filter using index</param>
    /// <returns>New named index query constructor, set to look for supplied key components</returns>
    public IIndexQuery<T, I1, I2, I3> IndexQueryByKey<I1, I2, I3>(string name, I1 keyPart1, I2 keyPart2, I3 keyPart3)
    {
      return IndexQuery<I1, I2, I3>(name).Key(keyPart1, keyPart2, keyPart3);
    }

    IDataIndex<T> CreateIndex<I1>(string name, Func<T, I1> getter, IComparer<I1> comparer, MemberInfo[] members, Func<I1, object, Lazy<T>> lazyCtor)
    {
      return new DataIndex<T, I1>(this, name, getter, comparer ?? Comparer<I1>.Default, lazyCtor, members);
    }

    internal void CreateIndex<I1>(string name, Func<T, I1> getter, MemberInfo member, IComparer<I1> comparer)
    {
      Indexes[name] = CreateIndex(name,
        getter,
        comparer,
        new[] { member },
        (k, pk) => new Lazy<T, I1>(this, pk, k));
    }

    internal void CreateIndex<I1, I2>(string name, MemberInfo member1, IComparer<I1> comparer1, MemberInfo member2, IComparer<I2> comparer2)
    {
      Indexes[name] = CreateIndex(name,
        BuildGetter<I1, I2>(member1, member2),
        new Indexer<I1, I2>.Comparer(comparer1, comparer2),
        new[] { member1, member2 },
        (k, pk) => new Lazy<T, I1, I2>(this, pk, k));
    }

    internal void CreateIndex<I1, I2, I3>(string name, MemberInfo member1, IComparer<I1> comparer1, MemberInfo member2, IComparer<I2> comparer2, MemberInfo member3, IComparer<I3> comparer3)
    {
      Indexes[name] = CreateIndex(name,
        BuildGetter<I1, I2, I3>(member1, member2, member3),
        new Indexer<I1, I2, I3>.Comparer(comparer1, comparer2, comparer3),
        new[] { member1, member2, member3 },
        (k, pk) => new Lazy<T, I1, I2, I3>(this, pk, k));
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
    /// Determines number of entities stored in the table
    /// </summary>
    /// <returns>Number of entities stored in the table</returns>
    public override int Count()
    {
      using (ReadScope())
        return KeyIndex.Count;
    }

    #region Scopes

    internal class Scope<E> : IDisposable
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

    internal Scope<IDbTableReader> ReadScope()
    {
      var scope = _db.ReadScope();
      try
      {
        return new Scope<IDbTableReader>(scope, scope.GetReader(this));
      }
      catch
      {
        scope.Dispose();
        throw;
      }
    }

    Scope<IDbTableWriter> WriteScope(bool autoReload = true)
    {
      var scope = _db.WriteScope();
      try
      {
        return new Scope<IDbTableWriter>(scope, scope.GetWriter(this, autoReload));
      }
      catch
      {
        scope.Dispose();
        throw;
      }
    }

    #endregion

    /// <summary>
    /// Lists all current key values 
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <returns>List of all current key values</returns>
    public override K[] AllKeys<K>()
    {
      var idx = GetPrimaryIndex<K>();

      using (ReadScope())
        return idx.MakeKeyList();
    }

    /// <summary>
    /// Lists all current keys 
    /// </summary>
    /// <returns>Sequence of key values</returns>
    public override object[] AllKeys()
    {
      using (ReadScope())
        return KeyIndex.MakeKeyList();
    }

    /// <summary>
    /// Loads all entities  
    /// </summary>
    /// <returns>Array of all entities</returns>
    public T[] LoadAll()
    {
      using (var scope = ReadScope())
        return KeyIndex.Load(scope.Element, Metadata);
    }

    public T LoadByKey(object key)
    {
      return KeyIndex.LoadByObjectKey(key);
    }

    /// <summary>
    /// Loads an entity by specified PK value
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="key">PK value</param>
    /// <returns>Entity identified by the PK value, if any</returns>
    public T LoadByKey<K>(K key)
    {
      return LoadByKeyCore<K>(GetPrimaryIndex<K>(), key);
    }

    internal T LoadByKeyCore<K>(IKeyIndex<T, K> idx, K key)
    {
      using (var scope = ReadScope())
        return LoadByKeyInfo(scope, idx.FindByKey(key, true));
    }

    internal T LoadByKeyNode(Scope<IDbTableReader> scope, IKeyNode node)
    {
      if (node == null)
        return default(T);

      return LoadByKeyInfo(scope, KeyIndex.GetLocation(node));
    }

    T LoadByKeyInfo(Scope<IDbTableReader> scope, Location<T> info)
    {
      if (info == null)
        return default(T);

      Metadata.Deserialize(new DataReader(new MStream(scope.Element.ReadData(info.Offset, info.Length))), info.Result);

      return info.Result;
    }

    /// <summary>
    /// Bulk load specified instances by key
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="keys">Sequence of keys to load</param>
    /// <param name="yieldNotFound">Specifies that missed keys will be returned as nulls</param>
    /// <returns>List of corresponding instances</returns>
    public IEnumerable<T> LoadByKeys(IEnumerable<object> keys, bool yieldNotFound = false)
    {
      if (keys == null)
        throw new ArgumentNullException();

      return KeyIndex.LoadByObjectKeys(keys, yieldNotFound);
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

      return LoadByKeysCore(GetPrimaryIndex<K>(), keys, yieldNotFound);
    }

    internal IEnumerable<T> LoadByKeysCore<K>(IKeyIndex<T, K> idx, IEnumerable<K> keys, bool yieldNotFound = false)
    {
      using (var scope = ReadScope())
      {
        var reader = scope.Element;

        foreach (var key in keys)
        {
          var info = idx.FindByKey(key, true);
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
    /// Refreshes specified entity from disk
    /// </summary>
    /// <param name="item">Entity to refresh</param>
    /// <returns>Same entity, updated from disk</returns>
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
    /// Saves specified entity sequence, adding or updating as needed
    /// </summary>
    /// <param name="items">Entity sequence to upsert into table</param>
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
    /// Saves specified entity, adding or updating as needed
    /// </summary>
    /// <param name="item">Entity to upsert into table</param>
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
      var format = Metadata.Read(reader);

      KeyIndex.Read(reader, format);

      var name = reader.ReadString();
      if (name != "" && _indexes != null)
      {
        KeyIndex.KeyMap = null;

        do
        {
          GetIndex(name).Read(reader, format);
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

    /// <summary>
    /// Flushes the underlying table storage
    /// </summary>
    public override void Flush()
    {
      Storage.Flush();
    }

    /// <summary>
    /// Returns size information about the table
    /// </summary>
    /// <returns>DbTableInfo instance filled with size info</returns>
    public override DbTableInfo GetInfo()
    {
      using (var scope = ReadScope())
      {
        var result = scope.Element.GetInfo();
        result.EffectiveDataSize = KeyIndex.GetFileSize();
        return result;
      }
    }

    /// <summary>
    /// Deletes entity specified by key
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="key">Key of entity to delete</param>
    /// <returns>True if entity was deleted</returns>
    public override bool DeleteByKey<K>(K key)
    {
      return DeleteByKeyCore<K>(GetPrimaryIndex<K>(), key);
    }

    internal bool DeleteByKeyCore<K>(IKeyIndex<T, K> idx, K key)
    {
      using (var scope = WriteScope())
        if (idx.RemoveByKey(key))
        {
          scope.Transaction.Modified(this);
          return true;
        }

      return false;
    }

    public override bool DeleteByKey(object key)
    {
      return KeyIndex.DeleteByObjectKey(key);
    }

    /// <summary>
    /// Deletes entities specified by key sequence 
    /// </summary>
    /// <param name="keys">Key sequence to delete</param>
    /// <returns>Count of the deleted entities</returns>
    public override int DeleteByKeys(IEnumerable<object> keys)
    {
      if (keys == null)
        throw new ArgumentNullException();

      return KeyIndex.DeleteByObjectKeys(keys);
    }


    /// <summary>
    /// Deletes entities specified by key sequence 
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <param name="keys">Key sequence to delete</param>
    /// <returns>Count of the deleted entities</returns>
    public override int DeleteByKeys<K>(IEnumerable<K> keys)
    {
      if (keys == null)
        throw new ArgumentNullException();

      return DeleteByKeysCore<K>(GetPrimaryIndex<K>(), keys);
    }

    internal int DeleteByKeysCore<K>(IKeyIndex<T, K> idx, IEnumerable<K> keys)
    {
      using (var scope = WriteScope())
      {
        var result = keys.Count(key => idx.RemoveByKey(key));

        if (result > 0)
          scope.Transaction.Modified(this, true);

        return result;
      }
    }

    /// <summary>
    /// Deletes specified entity
    /// </summary>
    /// <param name="item">Entity to delete</param>
    /// <returns>True is entity was deleted</returns>
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
    /// Deletes entities specified by the sequence 
    /// </summary>
    /// <param name="items">Sequence of entities to delete</param>
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
    /// Compacts data file of the table
    /// </summary>
    public override void Compact()
    {
      using (var compacter = Storage.BeginCompact())
      {
        Read(compacter);
        KeyIndex.Compact(compacter);
        Write(compacter, true);
      }
    }

    /// <summary>
    /// Clears the table
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
        foreach (var i in _indexes.Values)
          i.Update(key, instance);
    }

    DateTimeOffset _tableTs;

    internal override void Read(IDbTableReader reader)
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

    internal override void Write(IDbTableWriter writer, bool crop)
    {
      var s = WriteIndex();

      _tableTs = writer.WriteIndex(s.GetBuffer(), (int)s.Length);

      if (crop)
        writer.CropData(KeyIndex.GetFileSize());
    }

    /// <summary>
    /// Specifies key/values pairs of the table metadata 
    /// </summary>
    /// <param name="property">Metadata property name</param>
    /// <returns>Value of the named metadata property</returns>
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

    /// <summary>
    /// Determines minimal PK value from PK index,
    /// returns default(K) if table is empty
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <returns>Minimal PK value</returns>
    public override K GetMinKey<K>()
    {
      var idx = GetPrimaryIndex<K>();

      using (ReadScope())
        return idx.MinKey;
    }

    /// <summary>
    /// Determines maximal PK value from PK index,
    /// returns default(K) if table is empty
    /// </summary>
    /// <typeparam name="K">Type of the key</typeparam>
    /// <returns>Maximal PK value</returns>
    public override K GetMaxKey<K>()
    {
      var idx = GetPrimaryIndex<K>();

      using (ReadScope())
        return idx.MaxKey;
    }

    /// <summary>
    /// Retrieves an object that can iterate through the individual entities in this table.
    /// </summary>
    /// <returns>An enumerator object.</returns>
    public IEnumerator<T> GetEnumerator()
    {
      using (var scope = ReadScope())
        foreach (var i in KeyIndex.Enum(scope.Element, Metadata))
          yield return i;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }
  }
}
