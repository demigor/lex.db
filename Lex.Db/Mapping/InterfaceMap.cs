using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Serialization;
using Lex.Db.Serialization;

namespace Lex.Db
{
    /// <summary>
    /// Represents a <see cref="TypeMap{T}"/> which supports writing from an interface
    /// </summary>
    /// <typeparam name="TInterface"></typeparam>
    /// <typeparam name="TType"></typeparam>
    [DebuggerDisplay("{Name}")]
    public sealed class InterfaceMap<TInterface, TType> : TypeMap where TType : class, TInterface
    {
        private DbInstance _db;
        private DbTable<TInterface> _table;
        private MemberInfo _key;

        internal InterfaceMap(DbInstance db)
        {
            _db = db;
            Reset();
        }

        internal override void Clear()
        {
            _table = new DbTable<TInterface, TType>(_db, Ctor<TInterface, TType>.New);
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
        public InterfaceMap<TInterface, TType> ToTable(string name)
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
        public InterfaceMap<TInterface, TType> Reset()
        {
            Clear();
            return this;
        }

        /// <summary>
        /// Registers interceptor to control serialization of properties
        /// </summary>
        /// <param name="interceptor">Custom interceptor implementation</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> WithInterceptor(Interceptor<TInterface> interceptor)
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
        public InterfaceMap<TInterface, TType> WithInterceptor(Func<TInterface, string, bool?> interceptor)
        {
            if (interceptor == null)
                throw new ArgumentNullException();

            _table.Add(new DelegateInterceptor<TInterface>(interceptor));

            return this;
        }

        /// <summary>
        /// Defines primary key expression, indicating optional automatic generation of PK values
        /// </summary>
        /// <typeparam name="K">Type of the PK</typeparam>
        /// <param name="keyBuilder">Primary key expression</param>
        /// <param name="autoGen">Indicates automatic generation of PK values (int, long, Guid types only)</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> Key<TKey>(Expression<Func<TInterface, TKey>> keyBuilder, bool autoGen = false)
        {
            if (_key != null)
                throw new InvalidOperationException("Key is already defined");

            _table.Add(keyBuilder, _key = ExtractMember(keyBuilder, MemberUsage.KeyIndex), autoGen);

            return this;
        }

        /// <summary>
        /// Defines complete mapping for public properties and fields via reflection, 
        /// with specified primary key expression and optional automatic generation
        /// </summary>
        /// <typeparam name="K">Type of the PK</typeparam>
        /// <param name="keyBuilder">Primary key expression</param>
        /// <param name="autoGen">Indicates automatic generation of PK values (int, long, Guid types only)</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> Automap<K>(Expression<Func<TInterface, K>> keyBuilder, bool autoGen = false)
        {
            return Key(keyBuilder, autoGen).MapAll();
        }

        /// <summary>
        /// Defines complete mapping for public properties and fields via reflection
        /// </summary>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> MapAll()
        {
            var fields = from f in typeof(TInterface).GetPublicInstanceFields()
                         where f != _key
                         && !f.Attributes.HasFlag(FieldAttributes.InitOnly)
                         && !f.IsDefined(typeof(XmlIgnoreAttribute), false)
                         select f;

            foreach (var f in fields)
                _table.Add(new MemberMap<TInterface>(f));

            var properties = from p in typeof(TInterface).GetPublicInstanceProperties()
                             where p != _key
                             && p.CanRead && p.CanWrite && p.GetGetMethod().IsPublic && p.GetSetMethod().IsPublic
                             && !p.IsDefined(typeof(XmlIgnoreAttribute), false)
                             select p;

            foreach (var p in properties)
                _table.Add(new MemberMap<TInterface>(p));

            return this;
        }

        /// <summary>
        /// Removes mapping for specified member
        /// </summary>
        /// <typeparam name="K">Type of the member</typeparam>
        /// <param name="property">Member access expression</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> Unmap<TKey>(Expression<Func<TInterface, TKey>> property)
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
        public InterfaceMap<TInterface, TType> Map<TKey>(Expression<Func<TInterface, TKey>> property)
        {
            Expression target;
            var member = ExtractMemberEx(property, out target);
            if (member == null)
                throw new ArgumentException("Invalid member definition");

            _table.Add(new MemberMap<TInterface>(member, target, property.Parameters[0]));

            return this;
        }

        /// <summary>
        /// Adds non-unique typed index over single component
        /// </summary>
        /// <typeparam name="TIndex1">Type of the index key</typeparam>
        /// <param name="name">Name of the index</param>
        /// <param name="indexBy">Indexing expression</param>
        /// <param name="comparer">Optional comparer for indexing expression</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> WithIndex<TIndex1>(string name, Expression<Func<TInterface, TIndex1>> indexBy, IComparer<TIndex1> comparer = null)
        {
            CheckIndexDoesNotExists(name);

            var member = ExtractMember(indexBy, MemberUsage.DataIndex);

            _table.CreateIndex(name, indexBy.Compile(), member, comparer);

            return this;
        }

        /// <summary>
        /// Adds non-unique typed index over two components
        /// </summary>
        /// <typeparam name="TIndex1">Type of the first index key component</typeparam>
        /// <typeparam name="TIndex2">Type of the second index key component</typeparam>
        /// <param name="name">Name of the index</param>
        /// <param name="indexBy">First index key component expression</param>
        /// <param name="thenBy">Second index key component expression</param>
        /// <param name="comparerIndexBy">Optional comparer for first index key component</param>
        /// <param name="comparerThenBy">Optional comparer for second index key component</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> WithIndex<TIndex1, TIndex2>(string name, Expression<Func<TInterface, TIndex1>> indexBy, Expression<Func<TInterface, TIndex2>> thenBy, IComparer<TIndex1> comparerIndexBy = null, IComparer<TIndex2> comparerThenBy = null)
        {
            CheckIndexDoesNotExists(name);

            var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
            var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);

            CheckUnique(member1, member2);

            _table.CreateIndex<TIndex1, TIndex2>(name, member1, comparerIndexBy, member2, comparerThenBy);

            return this;
        }

        /// <summary>
        /// Adds non-unique typed index over three components
        /// </summary>
        /// <typeparam name="TIndex1">Type of the first index key component</typeparam>
        /// <typeparam name="TIndex2">Type of the second index key component</typeparam>
        /// <typeparam name="TIndex3">Type of the third index key component</typeparam>
        /// <param name="name">Name of the index</param>
        /// <param name="indexBy">First index key component expression</param>
        /// <param name="thenBy">Second index key component expression</param>
        /// <param name="andThenBy">Third index key component expression</param>
        /// <param name="comparerIndexBy">Optional comparer for first index key component</param>
        /// <param name="comparerThenBy">Optional comparer for second index key component</param>
        /// <param name="comparerAndThenBy">Optional comparer for thirt index key component</param>
        /// <returns>Entity type mapping to continue with</returns>
        public InterfaceMap<TInterface, TType> WithIndex<TIndex1, TIndex2, TIndex3>(string name, Expression<Func<TInterface, TIndex1>> indexBy, Expression<Func<TInterface, TIndex2>> thenBy, Expression<Func<TInterface, TIndex3>> andThenBy, IComparer<TIndex1> comparerIndexBy = null, IComparer<TIndex2> comparerThenBy = null, IComparer<TIndex3> comparerAndThenBy = null)
        {
            CheckIndexDoesNotExists(name);

            var member1 = ExtractMember(indexBy, MemberUsage.DataIndex);
            var member2 = ExtractMember(thenBy, MemberUsage.DataIndex);
            var member3 = ExtractMember(andThenBy, MemberUsage.DataIndex);

            CheckUnique(member1, member2, member3);

            _table.CreateIndex<TIndex1, TIndex2, TIndex3>(name, member1, comparerIndexBy, member2, comparerThenBy, member3, comparerAndThenBy);

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

        static MemberInfo ExtractMember<TKey>(Expression<Func<TInterface, TKey>> prop, MemberUsage usage = MemberUsage.Serialization)
        {
            Expression target;

            var result = ExtractMemberEx(prop, out target, usage);

            if (target != null)
                throw new InvalidOperationException("Invalid member provided");

            return result;
        }

        static MemberInfo ExtractMemberEx<TKey>(Expression<Func<TInterface, TKey>> prop, out Expression target, MemberUsage usage = MemberUsage.Serialization)
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
