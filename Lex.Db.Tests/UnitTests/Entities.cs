using System;
using System.Collections.Generic;

namespace Lex.Db
{
  public interface IMyData
  {
      int Id { get; set; }
      string Name { get; set; }
      string LastName { get; set; }

      int IntField { get; set; }
      int? IntNField { get; set; }

      long LongField { get; set; }
      long? LongNField { get; set; }

      double DoubleField { get; set; }
      double? DoubleNField { get; set; }

      decimal DecimalField { get; set; }
      decimal? DecimalNField { get; set; }

      float FloatField { get; set; }
      float? FloatNField { get; set; }

      bool BoolField { get; set; }
      bool? BoolNField { get; set; }

      DateTime DateTimeField { get; set; }
      DateTime? DateTimeNField { get; set; }

      DateTimeOffset DateTimeOffsetField { get; set; }
      DateTimeOffset? DateTimeOffsetNField { get; set; }

      TimeSpan TimeSpanField { get; set; }
      TimeSpan? TimeSpanNField { get; set; }

      Guid GuidField { get; set; }
      Guid? GuidNField { get; set; }

      List<int> ListField { get; set; }
      Dictionary<string, int> DictField { get; set; }

      TestEnum EnumField { get; set; }
      TestEnum? EnumNField { get; set; }
  }

  public class MyData : IMyData
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
    public Dictionary<string, int> DictField { get; set; }

    public TestEnum EnumField { get; set; }
    public TestEnum? EnumNField { get; set; }

    public MyData()
    {
      ListField = new List<int>();
      DictField = new Dictionary<string, int>();
    }
  }

  public enum TestEnum
  {
    None,
    EnumValue1,
    EnumValue2
  }

  public class MyDataGroup
  {
    public int Id { get; set; }
    public string Name { get; set; }
    public List<MyData> Items { get; set; }
    public MyDataGroup Parent { get; set; }
  }
}
