using System;
using System.Collections.Generic;
using System.IO;

namespace Lex.Db.Indexing
{
  using Serialization;

  public class Indexer<I1, I2> : IComparable<Indexer<I1, I2>>
  {
    static readonly IComparer<I1> _comparer1 = Comparer<I1>.Default;
    static readonly IComparer<I2> _comparer2 = Comparer<I2>.Default;

    public Indexer(I1 key1, I2 key2)
    {
      Key1 = key1;
      Key2 = key2;
    }

    public readonly I1 Key1;
    public readonly I2 Key2;

    int IComparable<Indexer<I1, I2>>.CompareTo(Indexer<I1, I2> other)
    {
      var result = _comparer1.Compare(Key1, other.Key1);
      if (result != 0)
        return result;

      return _comparer2.Compare(Key2, other.Key2);
    }

    static readonly Action<DataWriter, I1> _serializer1 = Serializers.GetWriter<I1>();
    static readonly Func<DataReader, I1> _deserializer1 = Serializers.GetReader<I1>();
    static readonly Action<DataWriter, I2> _serializer2 = Serializers.GetWriter<I2>();
    static readonly Func<DataReader, I2> _deserializer2 = Serializers.GetReader<I2>();

    internal static void Serialize(DataWriter writer, Indexer<I1, I2> value)
    {
      _serializer1(writer, value.Key1);
      _serializer2(writer, value.Key2);
    }

    internal static Indexer<I1, I2> Deserialize(DataReader reader)
    {
      return new Indexer<I1, I2>(_deserializer1(reader), _deserializer2(reader));
    }
  }

  public class Indexer<I1, I2, I3> : IComparable<Indexer<I1, I2, I3>>
  {
    static readonly IComparer<I1> _comparer1 = Comparer<I1>.Default;
    static readonly IComparer<I2> _comparer2 = Comparer<I2>.Default;
    static readonly IComparer<I3> _comparer3 = Comparer<I3>.Default;

    public readonly I1 Key1;
    public readonly I2 Key2;
    public readonly I3 Key3;

    public Indexer(I1 key1, I2 key2, I3 key3)
    {
      Key1 = key1;
      Key2 = key2;
      Key3 = key3;
    }

    int IComparable<Indexer<I1, I2, I3>>.CompareTo(Indexer<I1, I2, I3> other)
    {
      var result = _comparer1.Compare(Key1, other.Key1);
      if (result != 0)
        return result;

      result = _comparer2.Compare(Key2, other.Key2);
      if (result != 0)
        return result;

      return _comparer3.Compare(Key3, other.Key3);
    }

    static readonly Action<DataWriter, I1> _serializer1 = Serializers.GetWriter<I1>();
    static readonly Func<DataReader, I1> _deserializer1 = Serializers.GetReader<I1>();
    static readonly Action<DataWriter, I2> _serializer2 = Serializers.GetWriter<I2>();
    static readonly Func<DataReader, I2> _deserializer2 = Serializers.GetReader<I2>();
    static readonly Action<DataWriter, I3> _serializer3 = Serializers.GetWriter<I3>();
    static readonly Func<DataReader, I3> _deserializer3 = Serializers.GetReader<I3>();

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
  }
}
