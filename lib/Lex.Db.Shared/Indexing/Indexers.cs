using System;
using System.Collections.Generic;

namespace Lex.Db.Indexing
{
  using Serialization;

  class Indexer<I1, I2>
  {
    public Indexer(I1 key1, I2 key2)
    {
      Key1 = key1;
      Key2 = key2;
    }

    public readonly I1 Key1;
    public readonly I2 Key2;

    static readonly Action<DataWriter, I1> _serializer1 = Serializer<I1>.Writer;
    static readonly Func<DataReader, I1> _deserializer1 = Serializer<I1>.Reader;
    static readonly Action<DataWriter, I2> _serializer2 = Serializer<I2>.Writer;
    static readonly Func<DataReader, I2> _deserializer2 = Serializer<I2>.Reader;

#if iOS
    public static Action<DataWriter, object> Writer = (w, o) => Serialize(w, (Indexer<I1, I2>)o);
    public static Func<DataReader, object> Reader = r => Deserialize(r);
#endif

    internal static void Serialize(DataWriter writer, Indexer<I1, I2> value)
    {
      _serializer1(writer, value.Key1);
      _serializer2(writer, value.Key2);
    }

    internal static Indexer<I1, I2> Deserialize(DataReader reader)
    {
      return new Indexer<I1, I2>(_deserializer1(reader), _deserializer2(reader));
    }

    public class Comparer : IComparer<Indexer<I1, I2>>
    {
      readonly IComparer<I1> _comparer1;
      readonly IComparer<I2> _comparer2;

      public Comparer(IComparer<I1> comparer1, IComparer<I2> comparer2)
      {
        _comparer1 = comparer1 ?? Comparer<I1>.Default;
        _comparer2 = comparer2 ?? Comparer<I2>.Default;
      }

      public int Compare(Indexer<I1, I2> x, Indexer<I1, I2> y)
      {
        var result = _comparer1.Compare(x.Key1, y.Key1);
        if (result != 0)
          return result;

        return _comparer2.Compare(x.Key2, y.Key2);
      }
    }
  }

  class Indexer<I1, I2, I3>
  {
    public readonly I1 Key1;
    public readonly I2 Key2;
    public readonly I3 Key3;

    public Indexer(I1 key1, I2 key2, I3 key3)
    {
      Key1 = key1;
      Key2 = key2;
      Key3 = key3;
    }

    static readonly Action<DataWriter, I1> _serializer1 = Serializer<I1>.Writer;
    static readonly Func<DataReader, I1> _deserializer1 = Serializer<I1>.Reader;
    static readonly Action<DataWriter, I2> _serializer2 = Serializer<I2>.Writer;
    static readonly Func<DataReader, I2> _deserializer2 = Serializer<I2>.Reader;
    static readonly Action<DataWriter, I3> _serializer3 = Serializer<I3>.Writer;
    static readonly Func<DataReader, I3> _deserializer3 = Serializer<I3>.Reader;

#if iOS
    public static Action<DataWriter, object> Writer = (w, o) => Serialize(w, (Indexer<I1, I2, I3>)o);
    public static Func<DataReader, object> Reader = r => Deserialize(r);
#endif

    internal static void Serialize(DataWriter writer, Indexer<I1, I2, I3> value)
    {
      _serializer1(writer, value.Key1);
      _serializer2(writer, value.Key2);
      _serializer3(writer, value.Key3);
    }

    internal static Indexer<I1, I2, I3> Deserialize(DataReader reader)
    {
      return new Indexer<I1, I2, I3>(_deserializer1(reader), _deserializer2(reader), _deserializer3(reader));
    }

    public class Comparer : IComparer<Indexer<I1, I2, I3>>
    {
      readonly IComparer<I1> _comparer1;
      readonly IComparer<I2> _comparer2;
      readonly IComparer<I3> _comparer3;

      public Comparer(IComparer<I1> comparer1, IComparer<I2> comparer2, IComparer<I3> comparer3)
      {
        _comparer1 = comparer1 ?? Comparer<I1>.Default;
        _comparer2 = comparer2 ?? Comparer<I2>.Default;
        _comparer3 = comparer3 ?? Comparer<I3>.Default;
      }

      public int Compare(Indexer<I1, I2, I3> x, Indexer<I1, I2, I3> y)
      {
        var result = _comparer1.Compare(x.Key1, y.Key1);
        if (result != 0)
          return result;

        result = _comparer2.Compare(x.Key2, y.Key2);
        if (result != 0)
          return result;

        return _comparer3.Compare(x.Key3, y.Key3);
      }
    }
  }
}
