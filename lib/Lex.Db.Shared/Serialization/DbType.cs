using System;
using System.IO;

namespace Lex.Db.Serialization
{
  class DbType
  {
    protected DbType(sbyte id, Type type)
    {
      Id = id;
      Type = type;
    }

    public DbType(sbyte id)
      : this(id, DbTypes.GetType(id))
    {
    }

    public virtual void Write(DataWriter writer)
    {
      writer.Write(Id);
    }

    public virtual bool Equals(DbType type)
    {
      return type.Id == Id;
    }

    public readonly sbyte Id;
    public readonly Type Type;

    public static DbType Read(DataReader reader)
    {
      var id = reader.ReadSByte();

      switch ((KnownDbType)id)
      {
        case KnownDbType.List:
          return new DbListType(Read(reader));

        case KnownDbType.Dict:
          return new DbDictType(Read(reader), Read(reader));

        default:
          return new DbType(id);
      }
    }
  }
}
