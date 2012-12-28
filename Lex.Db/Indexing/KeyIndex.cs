using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db.Indexing
{
  using Mapping;
  using Serialization;

  interface IIndex<T>
  {
    MemberInfo[] Keys { get; }

    IEnumerable<Lazy<T>> LazyLoad();

    void Read(DataReader reader);
    void Write(DataWriter writer);
    void Purge();

    string Name { get; }
    int Count { get; }
  }

  interface IKeyIndex<T> : IIndex<T>
  {
    Type KeyType { get; }
    long GetFileSize();

    void Compact(IDbTableWriter compacter);
    T[] Load(IDbTableReader reader, Metadata<T> metadata);

    IKeyNode Update(T instance, int length);
    bool Remove(T instance);
    bool RemoveByKey(object key);

    KeyInfo<T> Find(T instance);
    KeyInfo<T> FindByKey(object key, bool provision = false);

    Dictionary<long, IKeyNode> KeyMap { get; set; }

    IEnumerable MakeKeyList();
    object MinKey();
    object MaxKey();
  }

  class KeyInfo<T>
  {
    public long Offset;
    public int Length;
    public T Result;
  }

  interface IKeyNode
  {
    long Offset { get; }
    long Length { get; }
    object Key { get; }
    object this[object key] { get; set; }
  }

  interface ICleanup
  {
    bool Cleanup(object stuff);
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
        return c != null && c.Cleanup(v);
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
    long IKeyNode.Length { get { return Length; } }

    object IKeyNode.Key { get { return Key; } }
  }

  class KeyIndex<T, K> : IKeyIndex<T>, IEnumerable<KeyNode<K>> where T: class
  {
    readonly Func<T, K> _getter;
    readonly Action<T, K> _setter;
    readonly RBTree<K, KeyNode<K>> _tree = new RBTree<K, KeyNode<K>>();
    readonly DbTable<T> _loader;
    readonly MemberInfo[] _keys;

    public KeyIndex(DbTable<T> loader, Func<T, K> getter, MemberInfo key)
    {
      _getter = getter;
      _loader = loader;

      if (key != null)
        _setter = MakeSetter(key);

      _keys = new[] { key };
    }

    static Action<T, K> MakeSetter(MemberInfo member)
    {
      // (TType instance, TKey keyValue) => instance.KeyProperty = keyValue;

      var obj = Expression.Parameter(typeof(T), "obj");
      var key = Expression.Parameter(typeof(K), "key");
      var assign = Expression.Assign(obj.Member(member), key);
      return Expression.Lambda<Action<T, K>>(assign, obj, key).Compile();
    }

    string IIndex<T>.Name { get { return null; } }

    public Type KeyType { get { return typeof(K); } }

    MemberInfo[] IIndex<T>.Keys { get { return _keys; } }

    public KeyInfo<T> Find(T instance)
    {
      return FindByKey(_getter(instance));
    }

    public KeyInfo<T> FindByKey(K key, bool provision = false)
    {
      var node = _tree.Find(key);
      if (node == null)
        return null;

      var result = new KeyInfo<T>
      {
        Offset = node.Offset,
        Length = node.Length
      };

      if (provision)
      {
        result.Result = Ctor<T>.New();

        if (_setter != null)
          _setter(result.Result, node.Key);
      }

      return result;
    }

    KeyInfo<T> IKeyIndex<T>.FindByKey(object key, bool provision)
    {
      return FindByKey((K)key, provision);
    }

    public void Write(DataWriter writer)
    {
      WriteNode(writer, _tree.Root);
    }

    static readonly Action<DataWriter, K> _serializer = Serializers.GetWriter<K>();
    static readonly Func<DataReader, K> _deserializer = Serializers.GetReader<K>();

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

    public void Read(DataReader reader)
    {
      try
      {
        _tree.Root = ReadNode(reader, null);
        _map = new DataMap<K>(_tree);
      }
      catch (InvalidOperationException)
      {
        _tree.Root = null;
        _map = new DataMap<K>(_tree);
        throw;
      }
    }

    bool IKeyIndex<T>.RemoveByKey(object key)
    {
      return RemoveByKey((K)key);
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

    public bool RemoveByKey(K k)
    {
      var node = _tree.Find(k);
      if (node == null)
        return false;

      _tree.Remove(node);
      _map.Free(node);

      node.Clean();

      return true;
    }

    public long GetFileSize()
    {
      return _map.Max;
    }

    DataMap<K> _map;

    public T[] Load(IDbTableReader reader, Metadata<T> metadata)
    {
      return _setter == null ? LoadNoKeys(reader, metadata) : LoadWithKeys(reader, metadata);
    }

    T[] LoadWithKeys(IDbTableReader reader, Metadata<T> metadata)
    {
      var result = new T[_tree.Count];
      var idx = 0;
      var ctor = Ctor<T>.New;
      for (var i = _tree.First(); i != null; i = _tree.Next(i))
      {
        var item = ctor();
        _setter(item, i.Key);
        metadata.Deserialize(reader.ReadData(i.Offset, i.Length), item);
        result[idx] = item;
        idx++;
      }
      return result;
    }

    T[] LoadNoKeys(IDbTableReader reader, Metadata<T> metadata)
    {
      var result = new T[_tree.Count];
      var idx = 0;
      var ctor = Ctor<T>.New;
      for (var i = _tree.First(); i != null; i = _tree.Next(i))
      {
        var item = ctor();
        metadata.Deserialize(reader.ReadData(i.Offset, i.Length), item);
        result[idx] = item;
        idx++;
      }
      return result;
    }

    public void Compact(IDbTableWriter writer)
    {
      long offset = 0;

      for (var i = _tree.First(); i != null; i = _tree.Next(i))
      {
        var length = i.Length;
        writer.CopyData(i.Offset, offset, length);
        i.Offset = offset;
        offset += length;
      }
      
      _map = new DataMap<K>(_tree);
    }

    public IEnumerable MakeKeyList()
    {
      return this.Select(i => i.Key).ToList();
    }

    public IEnumerator<KeyNode<K>> GetEnumerator()
    {
      var scan = _tree.First();
      while (scan != null)
      {
        yield return scan;
        scan = _tree.Next(scan);
      }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerable<Lazy<T>> LazyLoad()
    {
      return from node in this select (Lazy<T>)new Lazy<T, K>(node.Key, () => _loader.LoadByKey(node.Key));
    }

    public K GetMinKey()
    {
      var result = _tree.First();
      if (result == null)
        return default(K);

      return result.Key;
    }

    public K GetMaxKey()
    {
      var result = _tree.Last();
      if (result == null)
        return default(K);

      return result.Key;
    }

    object IKeyIndex<T>.MinKey()
    {
      return GetMinKey();
    }

    object IKeyIndex<T>.MaxKey()
    {
      return GetMaxKey(); 
    }
  }
}
