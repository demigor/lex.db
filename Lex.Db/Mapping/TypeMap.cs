using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Serialization;

namespace Lex.Db
{
  using Serialization;

  public abstract class TypeMap
  {
    public abstract string Name { get; }
    internal abstract Type KeyType { get; }
    internal abstract void Clear();
    internal abstract DbTable Initialize(IDbTableStorage table);
  }

  [DebuggerDisplay("{Name}")]
  public sealed class TypeMap<T> : TypeMap where T : class
  {
    DbInstance _db;
    DbTable<T> _table;

    internal TypeMap(DbInstance db)
    {
      _db = db;
      Reset();
    }

    internal override void Clear()
    {
      _table = new DbTable<T>(_db);
    }

    public override string Name { get { return _table.Name; } }

    public TypeMap<T> ToTable(string name)
    {
      _table.Name = name;
      return this;
    }

    public TypeMap<T> Reset()
    {
      Clear();
      return this;
    }

    public TypeMap<T> WithInterceptor(Interceptor<T> interceptor)
    {
      if (interceptor == null)
        throw new ArgumentNullException();

      _table.Add(interceptor);

      return this;
    }

    public TypeMap<T> WithInterceptor(Func<T, string, bool?> interceptor)
    {
      if (interceptor == null)
        throw new ArgumentNullException();

      _table.Add(new DelegateInterceptor<T>(interceptor));

      return this;
    }

    MemberInfo _key;

    public TypeMap<T> Key<K>(Expression<Func<T, K>> keyBuilder, bool autoGen = false)
    {
      if (_key != null)
        throw new InvalidOperationException("Key is already defined");

      _table.Add(keyBuilder, _key = ExtractMember(keyBuilder, MemberUsage.KeyIndex), autoGen);

      return this;
    }

    public TypeMap<T> Automap<K>(Expression<Func<T, K>> keyBuilder, bool autoGen = false)
    {
      return Key(keyBuilder, autoGen).MapAll();
    }

    /*
       public TypeMap<T> Ref<K, R>(Expression<Func<T, K>> foreignKey, Expression<Func<T, R>> reference)
       {
         if (reference == null)
           throw new ArgumentNullException("reference");

         if (foreignKey == null)
           throw new ArgumentNullException("foreignKey");

         var fkMember = ExtractMember(foreignKey);
         if (fkMember == null)
           throw new InvalidOperationException("Invalid foreign key definition");

         var refMember = ExtractMember(reference);
         if (refMember == null)
           throw new InvalidOperationException("Invalid reference definition");

         _table.Add(new RefMemberMap<T,R,K>(fkMember, refMember));

         return this;
       }
    */

    public TypeMap<T> Ref<R>(Expression<Func<T, R>> reference)
    {
      if (reference == null)
        throw new ArgumentNullException("reference");

      var refMember = ExtractMember(reference);
      if (refMember == null)
        throw new InvalidOperationException("Invalid reference definition");

      _table.Add(new RefMemberMap<T, R>(refMember));

      return this;
    }


    /*
        public TypeMap<T> Refs<K, R>(Expression<Func<T, IEnumerable<K>>> source, Expression<Func<K, R>> key)
        {
          // TODO!!! - implement array storage
          throw new NotSupportedException("This feature is not yet supported // LL");

          if (source == null)
            throw new ArgumentNullException("source");

          if (key == null)
            throw new ArgumentNullException("key");

          return this;
        }
    */

    public TypeMap<T> MapAll()
    {
      var fields = from f in typeof(T).GetPublicInstanceFields()
                   where f != _key
                   && !f.Attributes.HasFlag(FieldAttributes.InitOnly)
                   && !f.IsDefined(typeof(XmlIgnoreAttribute), false)
                   select f;

      foreach (var f in fields)
        _table.Add(new MemberMap<T>(f));

      var properties = from p in typeof(T).GetPublicInstanceProperties()
                       where p != _key
                       && p.CanRead && p.CanWrite && p.GetGetMethod().IsPublic && p.GetSetMethod().IsPublic
                       && !p.IsDefined(typeof(XmlIgnoreAttribute), false)
                       select p;

      foreach (var p in properties)
        _table.Add(new MemberMap<T>(p));

      return this;
    }

    public TypeMap<T> Unmap<K>(Expression<Func<T, K>> property)
    {
      var member = ExtractMember(property);
      if (member == null)
        throw new ArgumentException("Invalid member definition");

      _table.Remove(member);

      return this;
    }

    public TypeMap<T> Map<K>(Expression<Func<T, K>> property)
    {
      Expression target;
      var member = ExtractMemberEx(property, out target);
      if (member == null)
        throw new ArgumentException("Invalid member definition");

      _table.Add(new MemberMap<T>(member, target, property.Parameters[0]));

      return this;
    }

    public TypeMap<T> WithIndex<I1>(string name, Expression<Func<T, I1>> indexBy)
      where I1 : IComparable<I1>
    {
      CheckIndexDoesNotExists(name);

      var member = ExtractMember(indexBy, MemberUsage.DataIndex);

      var index = _table.GetIndex(member);
      if (index != null)
        throw new InvalidOperationException(string.Format("Index for member {0} is already defined", member.Name));

      _table.CreateIndex(name, indexBy.Compile(), member);

      return this;
    }

    public TypeMap<T> WithIndex<I1, I2>(string name, Expression<Func<T, I1>> indexBy, Expression<Func<T, I2>> thenBy)
      where I1 : IComparable<I1>
      where I2 : IComparable<I2>
    {
      CheckIndexDoesNotExists(name);

      var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
      var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);

      CheckUnique(member1, member2);

      var index = _table.GetIndex(member1, member2);
      if (index != null)
        throw new InvalidOperationException(string.Format("Index for members {0} | {1} is already defined", member1.Name, member2.Name));

      _table.CreateIndex<I1, I2>(name, member1, member2);

      return this;
    }

    public TypeMap<T> WithIndex<I1, I2, I3>(string name, Expression<Func<T, I1>> indexBy, Expression<Func<T, I2>> thenBy, Expression<Func<T, I3>> andThenBy)
      where I1 : IComparable<I1>
      where I2 : IComparable<I2>
      where I3 : IComparable<I3>
    {
      CheckIndexDoesNotExists(name);

      var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
      var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);
      var member3 = ExtractMember(andThenBy, MemberUsage.DataIndex);

      CheckUnique(member1, member2, member3);

      var index = _table.GetIndex(member1, member2, member3);
      if (index != null)
        throw new InvalidOperationException(string.Format("Index for members {0} | {1} | {2} is already defined", member1.Name, member2.Name, member3.Name));

      _table.CreateIndex<I1, I2, I3>(name, member1, member2, member3);

      return this;
    }

    static void CheckUnique(params MemberInfo[] members)
    {
      if (members.Distinct().Count() != members.Length)
        throw new InvalidOperationException("Duplicate index members");
    }

    void CheckIndexDoesNotExists(string name)
    {
      if (string.IsNullOrWhiteSpace(name))
        throw new ArgumentException("name");

      var index = _table.GetIndex(name);
      if (index != null)
        throw new InvalidOperationException(string.Format("Index name {0} is already defined", name));
    }

    enum MemberUsage
    {
      Serialization,
      KeyIndex,
      DataIndex
    }

    static MemberInfo ExtractMember<K>(Expression<Func<T, K>> prop, MemberUsage usage = MemberUsage.Serialization)
    {
      Expression target;

      var result = ExtractMemberEx(prop, out target, usage);

      if (target != null)
        throw new InvalidOperationException("Invalid member provided");

      return result;
    }

    static MemberInfo ExtractMemberEx<K>(Expression<Func<T, K>> prop, out Expression target, MemberUsage usage = MemberUsage.Serialization)
    {
      target = null;

      var param = prop.Parameters[0];
      var expr = prop.Body as MemberExpression;

      if (expr == null)
      {
        if (usage == MemberUsage.KeyIndex) return null;

        throw new ArgumentException("Cannot extract member information");
      }

      var member = expr.Member;

      var fi = member as FieldInfo;
      if (fi != null)
      {
        if (fi.IsInitOnly && usage != MemberUsage.DataIndex)
          throw new ArgumentException("Read only fields are not supported");

        if (expr.Expression != param)
          target = expr.Expression;

        return fi;
      }

      var pi = member as PropertyInfo;
      if (pi != null)
      {
        if (!pi.CanRead)
          throw new ArgumentException("Property must be readable");

        if (!pi.CanWrite && usage != MemberUsage.DataIndex)
          throw new ArgumentException("Property must be writable");

        if (expr.Expression != param)
          target = expr.Expression;

        return pi;
      }

      if (usage == MemberUsage.KeyIndex) // constant key
        return null;

      throw new ArgumentException("Cannot extract member information");
    }

    internal override DbTable Initialize(IDbTableStorage table)
    {
      var result = _table;
      result.Initialize(table);
      return result;
    }

    internal override Type KeyType
    {
      get { return _table.KeyIndex.KeyType; }
    }
  }
}
