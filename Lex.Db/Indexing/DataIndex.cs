using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Lex.Db.Indexing
{
  using Serialization;

  internal interface IDataIndex<T> : IIndex<T>
  {
    void Update(IKeyNode key, T item);
  }

  internal interface IDataIndex<T, in K> : IDataIndex<T>
  {
    IEnumerable<Lazy<T>> LazyLoad(K key);
    IEnumerable<T> Load(K key);
  }

  internal class DataNode<K> : RBTreeNode<K, DataNode<K>>
  {
    public IKeyNode KeyNode;
  }

  [DebuggerDisplay("{_name} ({ToString()}) : {Count}")]
  internal class DataIndex<T, K> : IDataIndex<T, K>, IEnumerable<DataNode<K>>, ICleanup
  {
    readonly string _name;
    readonly Func<T, K> _getter;
    readonly MemberInfo[] _keys;
    readonly IComparer<K> _comparer;
    readonly RBTree<K, DataNode<K>> _tree;
    readonly DbTable<T> _loader;
    readonly Func<DataNode<K>, Lazy<T>> _lazyCtor;
    readonly Func<DataNode<K>, T> _ctor;

    public DataIndex(DbTable<T> loader, string name, Func<T, K> getter, IComparer<K> comparer, Func<DataNode<K>, Lazy<T>> lazyCtor, Func<DataNode<K>, T> ctor, MemberInfo[] members)
    {
      _name = name;
      _keys = members;
      _getter = getter;
      _loader = loader;
      _comparer = comparer;
      _lazyCtor = lazyCtor;
      _ctor = ctor;
      _tree = new RBTree<K, DataNode<K>>(false, comparer);
    }

    public int Count { get { return _tree.Count; } }

    public string Name { get { return _name; } }

    public MemberInfo[] Keys { get { return _keys; } }

    public void Update(IKeyNode keyNode, T instance)
    {
      var value = _getter(instance);

      var node = (DataNode<K>)keyNode[this];
      if (node != null)
      {
        Debug.Assert(node.KeyNode == keyNode);

        if (_comparer.Compare(value, node.Key) == 0)
          return;

        _tree.Remove(node);
      }

      node = _tree.Add(value);
      node.KeyNode = keyNode;
      keyNode[this] = node;
    }

    public void Write(DataWriter writer)
    {
      WriteNode(writer, _tree.Root);
    }

    static readonly Action<DataWriter, K> _serializer = Serializers.GetWriter<K>();
    static readonly Func<DataReader, K> _deserializer = Serializers.GetReader<K>();

    static void WriteNode(DataWriter writer, DataNode<K> node)
    {
      if (node != null)
      {
        WriteNodeData(writer, node);
        WriteNode(writer, node.Left);
        WriteNode(writer, node.Right);
      }
      else
        writer.Write((sbyte)-1);
    }

    static void WriteNodeData(DataWriter writer, DataNode<K> node)
    {
      writer.Write((sbyte)node.Color);
      _serializer(writer, node.Key);
      writer.Write(node.KeyNode.Offset);
    }

    public void Read(DataReader reader)
    {
      _tree.Root = ReadNode(reader, _loader.KeyIndex.KeyMap, null);
    }

    DataNode<K> ReadNode(DataReader reader, Dictionary<long, IKeyNode> keyMap, DataNode<K> parent)
    {
      var color = reader.ReadSByte();
      if (color == -1) return null;

      var result = new DataNode<K>
      {
        Parent = parent,
        Color = (RBTreeColor)color,
        Key = _deserializer(reader),
        KeyNode = keyMap[reader.ReadInt64()]
      };

      result.KeyNode[this] = result;
      result.Left = ReadNode(reader, keyMap, result);
      result.Right = ReadNode(reader, keyMap, result);

      return result;
    }

    public IEnumerator<DataNode<K>> GetEnumerator()
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

    IEnumerable<DataNode<K>> Enumerate(K key)
    {
      var node = _tree.Find(key);
      if (node == null)
        yield break;

      var scan = node;
      while (scan != null && _comparer.Compare(scan.Key, key) == 0)
      {
        node = scan;
        scan = _tree.Prev(scan);
      }

      while (node != null && _comparer.Compare(node.Key, key) == 0)
      {
        yield return node;
        node = _tree.Next(node);
      }
    }

    public void Purge()
    {
      _tree.Root = null;
    }

    bool ICleanup.Cleanup(object stuff)
    {
      return _tree.Remove((DataNode<K>)stuff);
    }

    public override string ToString()
    {
      return string.Join(", ", _keys.Select(i => i.Name));
    }

    public IEnumerable<Lazy<T>> LazyLoad()
    {
      return from node in this select _lazyCtor(node);
    }

    public IEnumerable<Lazy<T>> LazyLoad(K key)
    {
      return Enumerate(key).Select(_lazyCtor);
    }

    public IEnumerable<T> Load(K key)
    {
      return Enumerate(key).Select(_ctor);
    }
  }
}
