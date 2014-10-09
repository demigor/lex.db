using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Lex.Db
{
  /// <summary>
  /// Pure POCO data entity
  /// </summary>
  public class MyData 
  {
    public int Id { get; set; }
    public string Name { get; set; }
    public string LastName { get; set; }

    public int IntField { get; set; }
    public int? IntNField { get; set; }

    public long LongField { get; set; }
    public long? LongNField { get; set; }

    public double DoubleField { get; set; }
    public double? DoubleNField { get; set; }

    public decimal DecimalField { get; set; }
    public decimal? DecimalNField { get; set; }

    public float FloatField { get; set; }
    public float? FloatNField { get; set; }

    public bool BoolField { get; set; }
    public bool? BoolNField { get; set; }

    public DateTime DateTimeField { get; set; }
    public DateTime? DateTimeNField { get; set; }

    public DateTimeOffset DateTimeOffsetField { get; set; }
    public DateTimeOffset? DateTimeOffsetNField { get; set; }

    public TimeSpan TimeSpanField { get; set; }
    public TimeSpan? TimeSpanNField { get; set; }

    public Guid GuidField { get; set; }
    public Guid? GuidNField { get; set; }

    public List<int> ListField { get; set; }
#if !SILVERLIGHT || WINDOWS_PHONE
    public SortedSet<int> SortedSetField { get; set; }
#endif
    public Dictionary<string, int> DictField { get; set; }
    public ObservableCollection<int> CollectionField { get; set; }

    public TestEnum EnumField { get; set; }
    public TestEnum? EnumNField { get; set; }

    public byte[] BlobField;
  }

  /// <summary>
  /// Test enumeration for serialization roundtrip
  /// </summary>
  public enum TestEnum
  {
    None,
    EnumValue1,
    EnumValue2
  }

  /// <summary>
  /// Pure POCO with references
  /// </summary>
  public class MyDataGroup
  {
    public int Id { get; set; }
    public string Name { get; set; }
    public List<MyData> Items { get; set; }
    public MyDataGroup Parent { get; set; }
  }

  /// <summary>
  /// Interface for interface based data entity 
  /// </summary>
  public interface IData
  {
    int Id { get; set; }
    string Name { get; set; }
  }

  /// <summary>
  /// Implementation for interface based data entity
  /// </summary>
  public class InterfaceBasedData: IData
  {
    public int Id { get; set; }
    public string Name { get; set; }
  }

  /// <summary>
  /// Prototype for prototype based data entity
  /// </summary>
  public abstract class AData
  {
    public abstract int Id { get; set; }
    public abstract string Name { get; set; }
  }

  /// <summary>
  /// Implementation for prototype based data entity
  /// </summary>
  public class PrototypeBasedData: AData
  {
    public override int Id { get; set; }
    public override string Name { get; set; }
  }
}
