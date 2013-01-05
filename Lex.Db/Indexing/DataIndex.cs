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

  interface IDataIndex<T> : IIndex<T> where T : class
  {
    void Update(IKeyNode key, T item);
  }

  interface IDataIndex<T, K> : IDataIndex<T>, IIndex<T, K> where T : class
  {
  }

  internal class DataNode<K> : RBTreeNode<K, DataNode<K>>
  {
    public HashSet<IKeyNode> Keys = new HashSet<IKeyNode>();
  }

  [DebuggerDisplay("{_name} ({ToString()}) : {Count}")]
  internal class DataIndex<T, K> : IDataIndex<T, K>, IEnumerable<DataNode<K>>, ICleanup where T : class
  {
    readonly string _name;
    readonly Func<T, K> _getter;
    readonly MemberInfo[] _keys;
    readonly RBTree<K, DataNode<K>> _tree;
    readonly DbTable<T> _table;
    readonly Func<K, object, Lazy<T>> _lazyCtor;

    public DataIndex(DbTable<T> loader, string name, Func<T, K> getter, IComparer<K> comparer, Func<K, object, Lazy<T>> lazyCtor, MemberInfo[] members)
    {
      _name = name;
      _keys = members;
      _getter = getter;
      _table = loader;
      _lazyCtor = lazyCtor;
      _tree = new RBTree<K, DataNode<K>>(comparer);
    }

    public int Count { get { return _tree.Count; } }

    public DbTable<T> Table { get { return _table; } }

    public string Name { get { return _name; } }

    public MemberInfo[] Keys { get { return _keys; } }

    public void Update(IKeyNode keyNode, T instance)
    {
      var value = _getter(instance);

      var node = (DataNode<K>)keyNode[this];
      if (node != null)
      {
        Debug.Assert(node.Keys.Contains(keyNode));

        if (_tree.Comparer.Compare(value, node.Key) == 0)
          return;

        node.Keys.Remove(keyNode);
        if (node.Keys.Count == 0)
          _tree.Remove(node);
      }

      node = _tree.AddOrGet(value);
      node.Keys.Add(keyNode);
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

      writer.Write(node.Keys.Count);
      foreach (var i in node.Keys)
        writer.Write(i.Offset);
    }

    public void Read(DataReader reader)
    {
      _tree.Root = ReadNode(reader, _table.KeyIndex.KeyMap, null);
    }

    DataNode<K> ReadNode(DataReader reader, Dictionary<long, IKeyNode> keyMap, DataNode<K> parent)
    {
      var color = reader.ReadSByte();
      if (color == -1) return null;

      var result = new DataNode<K>
      {
        Parent = parent,
        Color = (RBTreeColor)color,
        Key = _deserializer(reader)
      };

      var keysCount = reader.ReadInt32();
      for (var i = 0; i < keysCount; i++)
      {
        var keyNode = keyMap[reader.ReadInt64()];
        result.Keys.Add(keyNode);
        keyNode[this] = result;
      }

      result.Left = ReadNode(reader, keyMap, result);
      result.Right = ReadNode(reader, keyMap, result);

      return result;
    }

    public IEnumerator<DataNode<K>> GetEnumerator()
    {
      return _tree.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
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

    IEnumerable<L> ExecuteQuery<L>(IndexQueryArgs<K> args, Func<K, IKeyNode, L> selector)
    {
      var index = _table.KeyIndex;
      var query = from i in _tree.Enum(args)
                  from k in i.Keys
                  select selector(i.Key, k);

      if (args.Skip != null)
        query = query.Skip(args.Skip.Value);

      if (args.Take != null)
        query = query.Take(args.Take.Value);

      return query;
    }

    public int ExecuteCount(IndexQueryArgs<K> args)
    {
      using (_table.ReadScope())
        return ExecuteQuery(args, (k, pk) => pk).Count();
    }

    public List<L> ExecuteToList<L>(IndexQueryArgs<K> args, Func<K, IKeyNode, L> selector)
    {
      using (_table.ReadScope())
        return ExecuteQuery(args, selector).ToList();
    }

    public List<T> ExecuteToList(IndexQueryArgs<K> args)
    {
      using (var scope = _table.ReadScope())
        return ExecuteQuery(args, (k, pk) => _table.LoadByKeyNode(scope, pk)).ToList();
    }
  }
}
