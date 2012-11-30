using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db.Mapping
{
  using Indexing;
  using Serialization;

  internal class Metadata<T>
  {
    public Metadata() { }

    public Metadata(DataReader reader)
    {
      Key = DbType.Read(reader);

      var count = reader.ReadInt32();

      for (int i = 0; i < count; ++i)
      {
        var map = new MemberMap<T>(reader);
        _members.Add(map.Id, map);
      }
    }

    public DbType Key;

    Dictionary<int, MemberMap<T>> _members = new Dictionary<int, MemberMap<T>>();
    byte[] _blob;
    uint _hash;

    readonly Dictionary<string, string> _properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    public string this[string prop]
    {
      get
      {
        string result;

        return _properties.TryGetValue(prop, out result) ? result : null;
      }
      set
      {
        if (value == null)
          _properties.Remove(prop);
        else
          _properties[prop] = value;
      }
    }

    public void Add(MemberMap<T> member)
    {
      member.Id = _members.Count;
      _members.Add(member.Id, member);
    }

    public void Remove(MemberInfo member)
    {
      var toRemove = (from m in _members where m.Value.Member == member select m.Key).ToList();

      foreach (var key in toRemove)
        _members.Remove(key);
    }

    public void Clear()
    {
      _members.Clear();
    }

    public void Add(IIndex<T> index)
    {
      //
    }

    public bool Read(DataReader reader)
    {
      var hash = reader.ReadUInt32();

      if (hash == 0)
        throw new ArgumentException("Invalid index stream");

      if (hash == _hash)
      {
        Skip(reader);
        ReadProperties(reader);
        return false;
      }

      Upgrade(new Metadata<T>(reader));
      ReadProperties(reader);
      return true;
    }

    public void ClearProperties()
    {
      _properties.Clear();
    }

    void ReadProperties(DataReader reader)
    {
      DictSerializers<string, string>.Read(reader, _properties);
    }

    void WriteProperties(DataWriter writer)
    {
      DictSerializers<string, string>.WriteDictionary(writer, _properties);
    }

    public void Write(DataWriter writer)
    {
      writer.Write(_hash);
      writer.Write(_blob);
      WriteProperties(writer);
    }

    void Upgrade(Metadata<T> masters)
    {
      if (!Key.Equals(masters.Key))
        throw new InvalidOperationException("Incompatible table storage");

      var local = new HashSet<MemberMap<T>>(_members.Values);
      var all = new List<MemberMap<T>>(local);

      // transfer ids from master members
      foreach (var master in masters._members.Values)
      {
        var m = master;
        var existing = local.FirstOrDefault(i => i.Name == m.Name && i.DbType.Equals(m.DbType));
        if (existing != null)
        {
          existing.Id = master.Id;
          local.Remove(existing);
        }
        else
          all.Add(master);
      }

      // assign free ids to rest new members
      var id = masters._members.Values.Max(i => i.Id);

      foreach (var i in local)
        i.Id = ++id;

      // rebuild dictionary
      _members = all.ToDictionary(i => i.Id);

      Prepare();
    }

    static void Skip(DataReader reader)
    {
      DbType.Read(reader); // skip PK
      var count = reader.ReadInt32();

      for (var i = 0; i < count; ++i)
        new MemberMap(reader);
    }

    public void Initialize(DbInstance db)
    {
      foreach (var m in _members.Values)
        m.Initialize(db);
    }

    public void Prepare()
    {
      //#region Remap Ids (because of unmapped fields) ??? dont we need to preserve the data stream ???
      //if (format)
      //{
      //  int idx = 0;
      //  foreach (var m in _members.Values)
      //    m.Id = idx++;

      //  _members = _members.Values.ToDictionary(i => i.Id);
      //}
      //#endregion

      MakeBlob();

      _hash = Hash.Compute(_blob);

      foreach (var member in _members.Values)
        member.Deserialize = MakeReadMethod(member);

      Serialize = MakeWriteMethod();
    }

    void MakeBlob()
    {
      var ms = new MStream();
      var writer = new DataWriter(ms);

      Key.Write(writer);
      writer.Write(_members.Count);

      foreach (var map in _members.Values.OrderBy(i => i.Id))
        map.Write(writer);

      _blob = ms.ToArray();
    }

    #region Interceptor Property

    static readonly MethodInfo _filter = typeof(Interceptor<T>).GetStaticMethod("Filter");

    Interceptor<T> _interceptor;
    public Interceptor<T> Interceptor
    {
      get
      {
        return _interceptor;
      }
      set
      {
        value._next = _interceptor;
        _interceptor = value;
      }
    }

    #endregion

    #region MakeWriteMethod logic

    static readonly MethodInfo _writeShort = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(short) });

    /// <summary>
    /// foreach(var property in properties) 
    ///   if (Interceptor.NeedSerialize(T, property.Name)
    ///   {
    ///     WritePropertyId();
    ///     WritePropertyValue();
    ///   }
    /// WritePropertyId(-1);
    /// </summary>
    Action<Interceptor<T>, DataWriter, T> MakeWriteMethod()
    {
      var iceptor = Expression.Parameter(typeof(Interceptor<T>));
      var writer = Expression.Parameter(typeof(DataWriter));
      var obj = Expression.Parameter(typeof(T));
      var ops = new List<Expression>();

      foreach (var member in _members.Values)
        if (member.Member != null)
        {
          var writePropertyId = Expression.Call(writer, _writeShort, Expression.Constant((short)member.Id));
          var writePropertyValue = Serializers.WriteValue(writer, obj.Member(member));
          var block = Expression.Block(writePropertyId, writePropertyValue);

          if (_interceptor == null)
            ops.Add(block);
          else
            ops.Add(Expression.IfThen(Expression.Call(iceptor, _filter, obj, Expression.Constant(member.Name)), block));
        }

      ops.Add(Expression.Call(writer, _writeShort, Expression.Constant((short)-1)));

      return Expression.Lambda<Action<Interceptor<T>, DataWriter, T>>(Expression.Block(ops), iceptor, writer, obj).Compile();
    }

    #endregion

    #region MakeReadMethod logic

    static Action<DataReader, T> MakeReadMethod(MemberMap member)
    {
      var reader = Expression.Parameter(typeof(DataReader), "reader");
      var obj = Expression.Parameter(typeof(T), "obj");

      var body = Serializers.ReadValue(reader, member.MemberType);

      if (member.Member != null)
        body = Expression.Assign(obj.Member(member), body);

      return Expression.Lambda<Action<DataReader, T>>(body, reader, obj).Compile();
    }

    #endregion

    public Action<Interceptor<T>, DataWriter, T> Serialize;

    public void Deserialize(DataReader reader, T item)
    {
      for (var id = reader.ReadInt16(); id != -1; id = reader.ReadInt16())
        _members[id].Deserialize(reader, item);
    }

    public void Deserialize(byte[] data, T item)
    {
      Deserialize(new DataReader(new MStream(data)), item);
    }
  }
}
