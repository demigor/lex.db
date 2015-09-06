using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db.Mapping
{
  using Serialization;

  public interface IMetadata
  {
    string Name { get; }
    Type Key { get; }
    int MemberCount { get; }
    IEnumerable<IMemberMap> Members { get; }
  }

  class Metadata<T> : IMetadata where T : class
  {
    public Metadata() { }

    public Metadata(DataReader reader, uint hash = 0)
    {
      _hash = hash;
      Key = DbType.Read(reader);

      var count = reader.ReadInt32();

      for (int i = 0; i < count; ++i)
      {
        var map = new MemberMap<T>(reader);
        _members.Add(map.Id, map);
      }
    }

    public static Metadata<T> ReadMetadata(Stream stream)
    {
      using (var reader = new DataReader(stream))
      {
        var hash = reader.ReadUInt32();

        if (hash == 0)
          throw new ArgumentException("Invalid index stream");

        var result = DbFormat.Initial;

        // Special signature hash
        if (hash == Signature)
        {
          result = (DbFormat)reader.ReadUInt32();
          hash = reader.ReadUInt32();
        }

        return new Metadata<T>(reader, hash);
      }
    }

    public int MemberCount { get { return _members.Count; } }

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

    public void Assign(Metadata<T> source)
    {
      Key = source.Key;
      _hash = source._hash;
      _members = source._members;
    }

    public void Clear()
    {
      _members.Clear();
    }

    public void Add(IIndex<T> index)
    {
      //
    }

    internal const int Signature = 0x0058454C;

    public DbFormat Read(DataReader reader)
    {
      var hash = reader.ReadUInt32();

      if (hash == 0)
        throw new ArgumentException("Invalid index stream");

      var result = DbFormat.Initial;

      // Special signature hash
      if (hash == Signature)
      {
        result = (DbFormat)reader.ReadUInt32();
        hash = reader.ReadUInt32();
      }

      if (hash == _hash)
      {
        Skip(reader);
        ReadProperties(reader);
      }
      else
      {
        Upgrade(new Metadata<T>(reader));
        ReadProperties(reader);
      }

      return result;
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
      writer.Write(Signature);
      writer.Write((int)DbFormat.Current);
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

      var idx = 0;

      if (typeof(T) == typeof(object[]))
        foreach (var member in _members.Values)
        {
          member.Deserialize = MakeDynamicReadMethod(member, idx);
          idx++;
        }
      else
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

#if iOS
    class WriteJob
    {
      public short Id;
      public MemberInfo Member;
      public Func<T, object> Getter;
      public Action<DataWriter, object> Writer;
    }
#else
    static readonly MethodInfo _writeShort = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(short) });
#endif

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
#if iOS
      var jobs = (from member in _members.Values
                  let m = member.Member
                  where m != null
                  select new WriteJob
                  {
                    Id = (short)member.Id,
                    Member = m,
                    Getter = m.GetGetter<T>(),
                    Writer = Serializers.GetWriter(member.MemberType)
                  })
                  .ToArray();

      if (_interceptor == null)
        return (interceptor, writer, obj) =>
        {
          for (var i = 0; i < jobs.Length; i++)
          {
            var job = jobs[i];
            writer.Write(job.Id);
            job.Writer(writer, job.Getter(obj));
          }

          writer.Write((short)-1);
        };


      return (interceptor, writer, obj) =>
      {
        for (var i = 0; i < jobs.Length; i++)
        {
          var job = jobs[i];

          if (interceptor.Filter(obj, job.Member.Name))
          {
            writer.Write(job.Id);
            job.Writer(writer, job.Getter(obj));
          }
        }

        writer.Write((short)-1);
      };

#else
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
#endif
    }

    #endregion

    #region MakeReadMethod logic

    static Action<DataReader, T> MakeReadMethod(MemberMap member)
    {
#if iOS
      var read = Serializers.GetReader(member.MemberType);
      var data = member.Member;

      if (data != null)
      {
        var setter = data.GetSetter<T>();
        return (reader, obj) => setter(obj, read(reader));
      }

      return (reader, obj) => read(reader);
#else
      var reader = Expression.Parameter(typeof(DataReader), "reader");
      var obj = Expression.Parameter(typeof(T), "obj");

      var body = Serializers.ReadValue(reader, member.MemberType);

      if (member.Member != null)
        body = Expression.Assign(obj.Member(member), body);

      return Expression.Lambda<Action<DataReader, T>>(body, reader, obj).Compile();
#endif
    }

    static Action<DataReader, T> MakeDynamicReadMethod(MemberMap member, int idx)
    {
#if iOS
      var read = Serializers.GetReader(member.MemberType);

      return (reader, obj) => ((object[])(object)obj)[idx] = read(reader);
#else
      var reader = Expression.Parameter(typeof(DataReader), "reader");
      var obj = Expression.Parameter(typeof(T), "obj");

      var body = Serializers.ReadValue(reader, member.MemberType);

      body = Expression.Assign(Expression.ArrayAccess(obj, Expression.Constant(idx)), Expression.Convert(body, typeof(object)));

      return Expression.Lambda<Action<DataReader, T>>(body, reader, obj).Compile();
#endif
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

    #region IMetadata Members

    Type IMetadata.Key { get { return Key.Type; } }
    public IEnumerable<IMemberMap> Members { get { return _members.Values; } }

    public string Name { get; set; }

    #endregion
  }
}
