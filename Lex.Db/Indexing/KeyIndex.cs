using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db.Indexing
{
  using Mapping;
  using Serialization;

  interface IKeyIndex<T> : IIndex<T> where T : class
  {
    Type KeyType { get; }
    long GetFileSize();

    void Compact(IDbTableWriter compacter);
    T[] Load(IDbTableReader reader, Metadata<T> metadata);
    IEnumerable<T> Enum(IDbTableReader reader, Metadata<T> metadata);

    IKeyNode Update(T instance, int length);
    bool Remove(T instance);

    Location<T> Find(T instance);
    Location<T> GetLocation(IKeyNode node);

    Dictionary<long, IKeyNode> KeyMap { get; set; }

    object[] MakeKeyList();

    IEnumerable<T> LoadByObjectKeys(IEnumerable<object> keys, bool yieldNotFound);
    T LoadByObjectKey(object key);

    bool DeleteByObjectKey(object key);

    int DeleteByObjectKeys(IEnumerable<object> keys);
  }

  interface IKeyIndex<T, K> : IKeyIndex<T>, IIndex<T, K> where T : class
  {
    K MinKey { get; }
    K MaxKey { get; }

    new K[] MakeKeyList();
    Location<T> FindByKey(K key, bool provision = false);
    bool RemoveByKey(K key);
  }

  /// <summary>
  /// Data block location info
  /// </summary>
  /// <typeparam name="T">Provisioned instance to load</typeparam>
  class Location<T>
  {
    public long Offset;
    public int Length;
    public T Result;
  }

  interface IKeyNode
  {
    long Offset { get; }
    int Length { get; }
    object Key { get; }
    object this[object key] { get; set; }
  }

  interface ICleanup
  {
    bool Cleanup(IKeyNode sender, object stuff);
  }

  class PropertyBag
  {
    PropertyBag _next;
    object _key, _value;

    public static void Clean(ref PropertyBag bag, Func<object, object, bool> filter)
    {
      var prev = default(PropertyBag);

      for (var scan = bag; scan != null; scan = scan._next)
      {
        if (filter(scan._key, scan._value))
        {
          if (prev == null)
            bag = scan._next;
          else
            prev._next = scan._next;

          continue;
        }
        prev = scan;
      }
    }

    public static void RemoveKey(ref PropertyBag bag, object key)
    {
      var prev = default(PropertyBag);

      for (var scan = bag; scan != null; scan = scan._next)
      {
        if (scan._key == key)
        {
          if (prev == null)
            bag = scan._next;
          else
            prev._next = scan._next;

          return;
        }
        prev = scan;
      }
    }

    public static void SetValue(ref PropertyBag bag, object key, object value)
    {
      for (var scan = bag; scan != null; scan = scan._next)
        if (scan._key == key)
        {
          scan._value = value;
          return;
        }

      bag = new PropertyBag { _next = bag, _value = value, _key = key };
    }

    public static object GetValue(PropertyBag bag, object key)
    {
      for (var scan = bag; scan != null; scan = scan._next)
        if (scan._key == key)
          return scan._value;

      return null;
    }
  }

  class KeyNode<K> : RBTreeNode<K, KeyNode<K>>, IKeyNode
  {
    public long Offset;
    public int Length;

    PropertyBag _root;

    internal void Clean()
    {
      PropertyBag.Clean(ref _root, (k, v) =>
      {
        var c = k as ICleanup;
        return c != null && c.Cleanup(this, v);
      });
    }

    object IKeyNode.this[object key]
    {
      get
      {
        return PropertyBag.GetValue(_root, key);
      }
      set
      {
        if (value == null)
          PropertyBag.RemoveKey(ref _root, key);
        else
          PropertyBag.SetValue(ref _root, key, value);
      }
    }

    long IKeyNode.Offset { get { return Offset; } }
    int IKeyNode.Length { get { return Length; } }

    object IKeyNode.Key { get { return Key; } }
  }

  class KeyIndex<T, K> : IKeyIndex<T, K>, IEnumerable<KeyNode<K>> where T : class
  {
    readonly Func<T, K> _getter;
    readonly Action<T, K> _setter;
    readonly RBTree<K, KeyNode<K>> _tree;
    readonly DbTable<T> _table;
    readonly MemberInfo[] _keys;

    public KeyIndex(DbTable<T> table, Func<T, K> getter, MemberInfo key, IComparer<K> comparer)
    {
      _tree = new RBTree<K, KeyNode<K>>(comparer);
      _getter = getter;
      _table = table;

      if (key != null)
        _setter = MakeSetter(key);

      _keys = new[] { key };
    }

    static Action<T, K> MakeSetter(MemberInfo member)
    {
#if iOS
      return member.GetSetter<T, K>();
#else
      // (TType instance, TKey keyValue) => instance.KeyProperty = keyValue;

      var obj = Expression.Parameter(typeof(T), "obj");
      var key = Expression.Parameter(typeof(K), "key");
      var assign = Expression.Assign(obj.Member(member), key);
      return Expression.Lambda<Action<T, K>>(assign, obj, key).Compile();
#endif
      }

    public DbTable<T> Table { get { return _table; } }

    string IIndex<T>.Name { get { return null; } }

    public Type KeyType { get { return typeof(K); } }

    MemberInfo[] IIndex<T>.Keys { get { return _keys; } }

    public Location<T> Find(T instance)
    {
      return FindByKey(_getter(instance));
    }

    public Location<T> FindByKey(K key, bool provision = false)
    {
      var node = _tree.Find(key);
      if (node == null)
        return null;

      return GetLocation(node, provision);
    }

    Location<T> GetLocation(KeyNode<K> node, bool provision)
    {
      var result = new Location<T>
      {
        Offset = node.Offset,
        Length = node.Length
      };

      if (provision)
      {
        result.Result = _table.Ctor();

        if (_setter != null)
          _setter(result.Result, node.Key);
      }

      return result;
    }

    Location<T> IKeyIndex<T>.GetLocation(IKeyNode node)
    {
      return GetLocation((KeyNode<K>)node, true);
    }

    public void Write(DataWriter writer)
    {
      WriteNode(writer, _tree.Root);
    }

    static readonly Action<DataWriter, K> _serializer = Serializer<K>.Writer;
    static readonly Func<DataReader, K> _deserializer = Serializer<K>.Reader;

    static void WriteNode(DataWriter writer, KeyNode<K> node)
    {
      if (node != null)
      {
        writer.Write((sbyte)node.Color);
        writer.Write(node.Length);
        writer.Write(node.Offset);

        _serializer(writer, node.Key);

        WriteNode(writer, node.Left);
        WriteNode(writer, node.Right);
      }
      else
      {
        writer.Write((sbyte)-1);
      }
    }

    static KeyNode<K> ReadNode(DataReader reader, KeyNode<K> parent)
    {
      var color = reader.ReadSByte();
      if (color == -1) return null;

      var length = reader.ReadInt32();
      var offset = reader.ReadInt64();
      var key = _deserializer(reader);

      var result = new KeyNode<K>
      {
        Key = key,
        Color = (RBTreeColor)color,
        Length = length,
        Offset = offset,
        Parent = parent,
      };
      result.Left = ReadNode(reader, result);
      result.Right = ReadNode(reader, result);
      return result;
    }

    public void Read(DataReader reader, DbFormat format)
    {
      _tree.Root = ReadNode(reader, null);
      _map = new DataMap<K>(_tree);
    }

    public void Purge()
    {
      _tree.Clear();
      _map = new DataMap<K>(_tree);
    }

    Dictionary<long, IKeyNode> _keyMap;

    public Dictionary<long, IKeyNode> KeyMap { get { return _keyMap ?? (_keyMap = MakeKeyMap()); } set { _keyMap = value; } }

    Dictionary<long, IKeyNode> MakeKeyMap()
    {
      var result = new Dictionary<long, IKeyNode>();
      MakeMap(result, _tree.Root);
      return result;
    }

    static void MakeMap(Dictionary<long, IKeyNode> result, KeyNode<K> node)
    {
      if (node == null)
        return;

      result[node.Offset] = node;

      MakeMap(result, node.Left);
      MakeMap(result, node.Right);
    }

    public int Count { get { return _tree.Count; } }

    public IKeyNode Update(T instance, int length)
    {
      var result = _tree.AddOrGet(_getter(instance));

      //is new node
      if (result.Length == 0)
      {
        result.Length = length;
        _map.Alloc(result);
        return result;
      }

      if (result.Length == length)
        return result;

      _map.Realloc(result, length);
      return result;
    }

    public bool Remove(T instance)
    {
      return RemoveByKey(_getter(instance));
    }

    public bool RemoveByKey(K key)
    {
      var node = _tree.Find(key);
      if (node == null)
        return false;

      _tree.Remove(node);
      _map.Free(node);

      node.Clean();

      return true;
    }

    public object CheckKey(object key)
    {
      if (key is T)
        return key;

      return (K)key;
    }

    public long GetFileSize()
    {
      return _map.Max;
    }

    DataMap<K> _map;

    public T[] Load(IDbTableReader reader, Metadata<T> metadata)
    {
      return _tree.Select(new Loader(reader, metadata, _table.Ctor, _setter).Map());
    }

    public IEnumerable<T> Enum(IDbTableReader reader, Metadata<T> metadata)
    {
      return Enumerable.Select(_tree, new Loader(reader, metadata, _table.Ctor, _setter).Map());
    }

    struct Loader
    {
      Func<T> _ctor;
      IDbTableReader _reader;
      Metadata<T> _metadata;
      Action<T, K> _setter;

      public Loader(IDbTableReader reader, Metadata<T> metadata, Func<T> ctor, Action<T, K> setter)
      {
        _reader = reader;
        _metadata = metadata;
        _ctor = ctor;
        _setter = setter;
      }

      public Func<KeyNode<K>, T> Map()
      {
        return _setter == null ? (Func<KeyNode<K>, T>)LoadNoKey : LoadWithKey;
      }

      T LoadWithKey(KeyNode<K> i)
      {
        var item = _ctor();
        _setter(item, i.Key);
        _metadata.Deserialize(_reader.ReadData(i.Offset, i.Length), item);
        return item;
      }

      T LoadNoKey(KeyNode<K> i)
      {
        var item = _ctor();
        _metadata.Deserialize(_reader.ReadData(i.Offset, i.Length), item);
        return item;
      }
    }

    public void Compact(IDbTableWriter writer)
    {
      long offset = 0;

      foreach (var i in _tree)
      {
        var length = i.Length;
        writer.CopyData(i.Offset, offset, length);
        i.Offset = offset;
        offset += length;
      }

      _map = new DataMap<K>(_tree);
    }

    object[] IKeyIndex<T>.MakeKeyList()
    {
      return _tree.Select(i => (object)i.Key);
    }

    public K[] MakeKeyList()
    {
      return _tree.Select(i => i.Key);
    }

    public IEnumerator<KeyNode<K>> GetEnumerator()
    {
      return _tree.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public K MinKey
    {
      get
      {
        var result = _tree.First();
        if (result == null)
          return default(K);

        return result.Key;
      }
    }

    public K MaxKey
    {
      get
      {
        var result = _tree.Last();
        if (result == null)
          return default(K);

        return result.Key;
      }
    }

    IEnumerable<L> ExecuteQuery<L>(IndexQueryArgs<K> args, Func<K, IKeyNode, L> selector)
    {
      var query = from i in _tree.Enum(args)
                  select selector(i.Key, i);

      if (args.Skip != null)
        query = query.Skip(args.Skip.GetValueOrDefault());

      if (args.Take != null)
        query = query.Take(args.Take.GetValueOrDefault());

      return query;
    }

    public int ExecuteCount(IndexQueryArgs<K> args)
    {
      using (_table.ReadScope())
        return ExecuteQuery(args, (k, pk) => pk).Count();
    }

    public List<T> ExecuteToList(IndexQueryArgs<K> args)
    {
      using (var scope = _table.ReadScope())
        return ExecuteQuery(args, (k, pk) => _table.LoadByKeyNode(scope, pk)).ToList();
    }

    public List<L> ExecuteToList<L>(IndexQueryArgs<K> args, Func<K, IKeyNode, L> selector)
    {
      using (_table.ReadScope())
        return ExecuteQuery(args, selector).ToList();
    }

    public IEnumerable<T> LoadByObjectKeys(IEnumerable<object> keys, bool yieldNotFound)
    {
      return _table.LoadByKeysCore(this, keys.OfType<K>(), yieldNotFound);
    }

    public T LoadByObjectKey(object key)
    {
      return _table.LoadByKeyCore(this, (K)key);
    }

    public bool DeleteByObjectKey(object key)
    {
      return _table.DeleteByKeyCore(this, (K)key);
    }

    public int DeleteByObjectKeys(IEnumerable<object> keys)
    {
      return _table.DeleteByKeysCore(this, keys.OfType<K>());
    }
  }
}
