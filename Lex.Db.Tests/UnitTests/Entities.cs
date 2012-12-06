using System;
using System.Collections.Generic;

namespace Lex.Db
{
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
    public Dictionary<string, int> DictField { get; set; }

    public MyData()
    {
      ListField = new List<int>();
      DictField = new Dictionary<string, int>();
    }
  }

  public class MyDataGroup
  {
    public int Id { get; set; }
    public string Name { get; set; }
    public List<MyData> Items { get; set; }
    public MyDataGroup Parent { get; set; }
  }
}
