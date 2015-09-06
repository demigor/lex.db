using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lex.Db
{
  using Serialization;

  /// <summary>
  /// Entity type to table mapping base
  /// </summary>
  public abstract class TypeMap
  {
    /// <summary>
    /// Indicates name of the table
    /// </summary>
    public abstract string Name { get; }
    internal abstract Type KeyType { get; }
    internal abstract void Clear();
    internal abstract DbTable Initialize(IDbTableStorage table);

    protected static Type _xmlIgnoreAttribute, _ignoreDataMemberAttribute;


#if NETFX_CORE
    
    protected static bool IsIgnored(IEnumerable<Attribute> attributes)
    {
      if (attributes != null)
        foreach (var attribute in attributes)
          if (IsIgnored(attribute.GetType()))
            return true;

      return false;
    }

#else

    protected static bool IsIgnored(object[] attributes)
    {
      for (var i = 0; i < attributes.Length; i++)
        if (IsIgnored(attributes[i].GetType()))
          return true;

      return false;
    }

#endif

    protected static bool IsIgnored(Type type)
    {
      if (type == _xmlIgnoreAttribute || type == _ignoreDataMemberAttribute)
        return true;

      var typeName = type.FullName;

      if (typeName == "System.Runtime.Serialization.IgnoreDataMemberAttribute")
      {
        _ignoreDataMemberAttribute = type;
        return true;
      }

      if (typeName == "System.Xml.Serialization.XmlIgnoreAttribute")
      {
        _xmlIgnoreAttribute = type;
        return true;
      }

      return false;
    }
  }

  /// <summary>
  /// Entity type to table mapping
  /// </summary>
  /// <typeparam name="T"></typeparam>
  [DebuggerDisplay("{Name}")]
  public sealed class TypeMap<T> : TypeMap where T : class
  {
    readonly DbInstance _db;
    readonly Func<T> _ctor;
    DbTable<T> _table;

    internal TypeMap(DbInstance db, Func<T> ctor)
    {
      _db = db;
      _ctor = ctor;
      Reset();
    }

    internal override void Clear()
    {
      _table = new DbTable<T>(_db, _ctor);
    }

    /// <summary>
    /// Indicates name of the table
    /// </summary>
    public override string Name { get { return _table.Name; } }

    /// <summary>
    /// Defines a non-default name of the entity table
    /// </summary>
    /// <param name="name">Name of the table file without extension</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> ToTable(string name)
    {
      if (string.IsNullOrEmpty(name))
        throw new ArgumentException("name");

      _table.Name = name;
      return this;
    }

    /// <summary>
    /// Resets all mappings
    /// </summary>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> Reset()
    {
      Clear();
      return this;
    }

    /// <summary>
    /// Registers interceptor to control serialization of properties
    /// </summary>
    /// <param name="interceptor">Custom interceptor implementation</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> WithInterceptor(Interceptor<T> interceptor)
    {
      if (interceptor == null)
        throw new ArgumentNullException();

      _table.Add(interceptor);

      return this;
    }

    /// <summary>
    /// Registers interceptor function to control serialization of properties
    /// </summary>
    /// <param name="interceptor">Custom interceptor function</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> WithInterceptor(Func<T, string, bool?> interceptor)
    {
      if (interceptor == null)
        throw new ArgumentNullException();

      _table.Add(new DelegateInterceptor<T>(interceptor));

      return this;
    }

    MemberInfo _key;

    /// <summary>
    /// Defines primary key expression, indicating optional automatic generation of PK values
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="keyBuilder">Primary key expression</param>
    /// <param name="autoGen">Indicates automatic generation of PK values (int, long, Guid types only)</param>
    /// <param name="comparer">Optional primary key comparer</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> Key<K>(Expression<Func<T, K>> keyBuilder, bool autoGen = false, IComparer<K> comparer = null)
    {
      if (_key != null)
        throw new InvalidOperationException("Key is already defined");

      _table.Add(keyBuilder, _key = ExtractMember(keyBuilder, MemberUsage.KeyIndex), autoGen, comparer);

      return this;
    }

    /// <summary>
    /// Defines complete mapping for public properties and fields via reflection, 
    /// with specified primary key expression and optional automatic generation
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="keyBuilder">Primary key expression</param>
    /// <param name="autoGen">Indicates automatic generation of PK values (int, long, Guid types only)</param>
    /// <param name="comparer">Optional primary key comparer</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> Automap<K>(Expression<Func<T, K>> keyBuilder, bool autoGen = false, IComparer<K> comparer = null)
    {
      return Key(keyBuilder, autoGen, comparer).MapAll();
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

    /*
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
    */

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

    /// <summary>
    /// Defines complete mapping for public properties and fields via reflection
    /// </summary>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> MapAll()
    {
#if NETFX_CORE
      if (!typeof(T).GetTypeInfo().IsInterface)
#else
      if (!typeof(T).IsInterface)
#endif
      {
        var fields = from f in typeof(T).GetPublicInstanceFields()
                     where f != _key
                     && !f.Attributes.HasFlag(FieldAttributes.InitOnly)
                     && !IsIgnored(f.GetCustomAttributes(false))
                     select f;

        foreach (var f in fields)
          _table.Add(new MemberMap<T>(f));
      }

      var properties = from p in typeof(T).GetPublicInstanceProperties()
                       where p != _key
                       && p.CanRead && p.CanWrite && p.GetGetMethod().IsPublic && p.GetSetMethod().IsPublic
                       && !IsIgnored(p.GetCustomAttributes(false))
                       select p;

      foreach (var p in properties)
        _table.Add(new MemberMap<T>(p));

      return this;
    }

    /// <summary>
    /// Removes mapping for specified member
    /// </summary>
    /// <typeparam name="K">Type of the member</typeparam>
    /// <param name="property">Member access expression</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> Unmap<K>(Expression<Func<T, K>> property)
    {
      var member = ExtractMember(property);
      if (member == null)
        throw new ArgumentException("Invalid member definition");

      _table.Remove(member);

      return this;
    }

    /// <summary>
    /// Adds mapping for specified member
    /// </summary>
    /// <typeparam name="K">Type of the member</typeparam>
    /// <param name="property">Members access expression (public readable/writable property or field)</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> Map<K>(Expression<Func<T, K>> property)
    {
      Expression target;
      var member = ExtractMemberEx(property, out target);
      if (member == null)
        throw new ArgumentException("Invalid member definition");

      _table.Add(new MemberMap<T>(member, target, property.Parameters[0]));

      return this;
    }

    /// <summary>
    /// Adds non-unique typed index over single component
    /// </summary>
    /// <typeparam name="I1">Type of the index key</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="indexBy">Indexing expression</param>
    /// <param name="comparer">Optional comparer for indexing expression</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> WithIndex<I1>(string name, Expression<Func<T, I1>> indexBy, IComparer<I1> comparer = null)
    {
      CheckIndexDoesNotExists(name);

      var member = ExtractMember(indexBy, MemberUsage.DataIndex);

#if iOS
      var indexByGetter = member.GetGetter<T, I1>();
#else
      var indexByGetter = indexBy.Compile();
#endif

      _table.CreateIndex(name, indexByGetter, member, comparer);

      return this;
    }

    /// <summary>
    /// Adds non-unique typed index over two components
    /// </summary>
    /// <typeparam name="I1">Type of the first index key component</typeparam>
    /// <typeparam name="I2">Type of the second index key component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="indexBy">First index key component expression</param>
    /// <param name="thenBy">Second index key component expression</param>
    /// <param name="comparerIndexBy">Optional comparer for first index key component</param>
    /// <param name="comparerThenBy">Optional comparer for second index key component</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> WithIndex<I1, I2>(string name, Expression<Func<T, I1>> indexBy, Expression<Func<T, I2>> thenBy, IComparer<I1> comparerIndexBy = null, IComparer<I2> comparerThenBy = null)
    {
      CheckIndexDoesNotExists(name);

      var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
      var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);

      CheckUnique(member1, member2);

      _table.CreateIndex<I1, I2>(name, member1, comparerIndexBy, member2, comparerThenBy);

      return this;
    }

    /// <summary>
    /// Adds non-unique typed index over three components
    /// </summary>
    /// <typeparam name="I1">Type of the first index key component</typeparam>
    /// <typeparam name="I2">Type of the second index key component</typeparam>
    /// <typeparam name="I3">Type of the third index key component</typeparam>
    /// <param name="name">Name of the index</param>
    /// <param name="indexBy">First index key component expression</param>
    /// <param name="thenBy">Second index key component expression</param>
    /// <param name="andThenBy">Third index key component expression</param>
    /// <param name="comparerIndexBy">Optional comparer for first index key component</param>
    /// <param name="comparerThenBy">Optional comparer for second index key component</param>
    /// <param name="comparerAndThenBy">Optional comparer for thirt index key component</param>
    /// <returns>Entity type mapping to continue with</returns>
    public TypeMap<T> WithIndex<I1, I2, I3>(string name, Expression<Func<T, I1>> indexBy, Expression<Func<T, I2>> thenBy, Expression<Func<T, I3>> andThenBy, IComparer<I1> comparerIndexBy = null, IComparer<I2> comparerThenBy = null, IComparer<I3> comparerAndThenBy = null)
    {
      CheckIndexDoesNotExists(name);

      var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
      var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);
      var member3 = ExtractMember(andThenBy, MemberUsage.DataIndex);

      CheckUnique(member1, member2, member3);

      _table.CreateIndex<I1, I2, I3>(name, member1, comparerIndexBy, member2, comparerThenBy, member3, comparerAndThenBy);

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
