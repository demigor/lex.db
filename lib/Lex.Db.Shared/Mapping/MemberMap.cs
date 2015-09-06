using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db
{
  using Serialization;

  enum DbFormat : int
  {
    /// <summary>
    /// Original DB Format, data indexes are non-unique, upgrade needed
    /// </summary>
    Initial = 0x0,
    Current = 0x1
  }

  public interface IMemberMap
  {
    string Name { get; }
    Type Type { get; }
  }

  class MemberMap : IMemberMap
  {
    public MemberMap(DataReader reader)
    {
      Id = reader.ReadInt16();
      Name = reader.ReadString();

      DbType = DbType.Read(reader);
      MemberType = DbType.Type;
    }

    public MemberMap(MemberInfo member, Expression target = null, Expression self = null)
    {
      Self = self;
      Member = member;
      Target = target;
      Name = Member.Name;

      MemberType = member.GetMemberType();
    }

    public void Write(DataWriter writer)
    {
      writer.Write((short)Id);
      writer.Write(Name);
      DbType.Write(writer);
    }

    public void Initialize(DbInstance db)
    {
      // simple or already known type
      DbType = DbTypes.TryGetDbType(MemberType);
      if (DbType != null)
        return;

      // reference 
      {
        var key = db.GetKeyType(MemberType);
        if (key != null)
        {
          DbType = DbTypes.GetDbType(key);
          return;
        }
      }

      // dictionary of references
      {
        var elements = DbTypes.GetDictionaryElementTypes(MemberType);
        if (elements != null)
        {
          var key = db.GetKeyType(elements.Item1) ?? elements.Item1;
          var value = db.GetKeyType(elements.Item2) ?? elements.Item2;
          DbType = DbTypes.GetDbType(typeof(Dictionary<,>).MakeGenericType(key, value));
          return;
        }
      }

      // collection of references 
      {
        var element = DbTypes.GetCollectionElementType(MemberType);
        if (element != null)
        {
          var key = db.GetKeyType(element);
          if (key != null)
          {
            DbType = DbTypes.GetDbType(key.MakeArrayType());
            return;
          }
        }
      }

      throw new NotSupportedException(string.Format("Serialization of '{0}' is not supported", MemberType));
    }

    #region Codegen logic

    public readonly MemberInfo Member;
    public readonly Expression Target;
    public readonly Expression Self;

    #endregion

    public DbType DbType;
    public readonly Type MemberType;

    public string Name;
    public int Id;

    #region IMemberMap Members

    string IMemberMap.Name { get { return Name; } }
    Type IMemberMap.Type { get { return MemberType; } }

    #endregion
  }

  class MemberMap<T> : MemberMap
  {
    public MemberMap(MemberInfo member, Expression target = null, ParameterExpression self = null) : base(member, target, self) { }

    public MemberMap(DataReader reader) : base(reader) { }

    public Action<DataReader, T> Deserialize;
  }

  static partial class Extensions
  {
    internal static Expression Member(this Expression target, MemberInfo member)
    {
      var prop = member as PropertyInfo;
      return prop != null ? Expression.Property(target, prop) : Expression.Field(target, (FieldInfo)member);
    }

    internal static Expression Member(this Expression target, MemberMap member)
    {
      return member.Target == null ? target.Member(member.Member) : member.Target.Clone(member.Self, target).Member(member.Member);
    }

    static Expression Clone(this Expression target, Expression oldExpr, Expression newExpr)
    {
      if (target == oldExpr)
        return newExpr;

      switch (target.NodeType)
      {
        case ExpressionType.MemberAccess:
          {
            var ma = (MemberExpression)target;
            return Expression.MakeMemberAccess(ma.Expression.Clone(oldExpr, newExpr), ma.Member);
          }
      }

      throw new NotImplementedException();
    }

    public static Type GetMemberType(this MemberInfo member)
    {
      var pi = member as PropertyInfo;
      return pi != null ? pi.PropertyType : ((FieldInfo)member).FieldType;
    }
  }
}
