using System.Collections.Generic;
using System.IO;

namespace Lex.Db.Serialization
{
  internal class DbDictType : DbType
  {
    public DbDictType(DbType key, DbType value)
      : base((short)KnownDbType.Dict, typeof(Dictionary<,>).MakeGenericType(key.Type, value.Type))
    {
      Key = key;
      Value = value;
    }

    public override void Write(DataWriter writer)
    {
      writer.Write(Id);
      Key.Write(writer);
      Value.Write(writer);
    }

    public override bool Equals(DbType type)
    {
      return type.Id == Id && ((DbDictType)type).Key.Equals(Key) && ((DbDictType)type).Value.Equals(Value);
    }

    public readonly DbType Key, Value;
  }
}
