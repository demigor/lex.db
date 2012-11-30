using System;
using System.Collections.Generic;
using System.IO;

namespace Lex.Db.Serialization
{
  public class DictSerializers<K, V>
  {
    static readonly Action<DataWriter, K> _keySerializer = Serializers.GetWriter<K>();
    static readonly Func<DataReader, K> _keyDeserializer = Serializers.GetReader<K>();
    static readonly Action<DataWriter, V> _valueSerializer = Serializers.GetWriter<V>();
    static readonly Func<DataReader, V> _valueDeserializer = Serializers.GetReader<V>();

    public static void WriteDictionary(DataWriter writer, Dictionary<K, V> value)
    {
      writer.Write(value.Count);

      foreach (var i in value)
      {
        _keySerializer(writer, i.Key);
        _valueSerializer(writer, i.Value);
      }
    }

    static void ReadCore(DataReader reader, Dictionary<K, V> target)
    {
      var count = reader.ReadInt32();
      for (var i = 0; i < count; i++)
      {
        var key = _keyDeserializer(reader);
        var value = _valueDeserializer(reader);

        target[key] = value;
      }
    }

    public static void Read(DataReader reader, Dictionary<K, V> target)
    {
      target.Clear();
      ReadCore(reader, target);
    }

    public static Dictionary<K, V> ReadDictionary(DataReader reader)
    {
      var result = new Dictionary<K, V>();
      
      ReadCore(reader, result);

      return result;
    }
  }
}
