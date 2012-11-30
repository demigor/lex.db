using System.IO;

namespace Lex.Db.Serialization
{
  internal class DbListType : DbType
  {
    public DbListType(DbType element)
      : base((short)KnownDbType.List, element.Type.MakeArrayType())
    {
      Element = element;
    }

    public override void Write(DataWriter writer)
    {
      writer.Write(Id);
      Element.Write(writer);
    }

    public override bool Equals(DbType type)
    {
      return type.Id == Id && ((DbListType)type).Element.Equals(Element);
    }

    public readonly DbType Element;
  }
}
