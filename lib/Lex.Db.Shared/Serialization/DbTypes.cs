using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Lex.Db.Serialization
{
  static class DbTypes
  {
    readonly static Dictionary<int, Type> _simpleTypes = new Dictionary<int, Type>();
    readonly static Dictionary<Type, DbType> _dbTypes = new Dictionary<Type, DbType>();

    static DbTypes()
    {
      Register<Guid>(KnownDbType.Guid);
      Register<string>(KnownDbType.String);
      Register<short>(KnownDbType.Short);
      Register<ushort>(KnownDbType.UShort);
      Register<int>(KnownDbType.Integer);
      Register<uint>(KnownDbType.UInteger);
      Register<long>(KnownDbType.Long);
      Register<ulong>(KnownDbType.ULong);
      Register<float>(KnownDbType.Float);
      Register<double>(KnownDbType.Double);
      Register<DateTime>(KnownDbType.DateTime);
      Register<DateTimeOffset>(KnownDbType.DateTimeOffset);
      Register<TimeSpan>(KnownDbType.TimeSpan);
      Register<bool>(KnownDbType.Boolean);
      Register<decimal>(KnownDbType.Decimal);
      Register<sbyte>(KnownDbType.SByte);
      Register<byte>(KnownDbType.Byte);
      Register<Uri>(KnownDbType.Uri);
      Register<UriBuilder>(KnownDbType.UriBuilder);
      Register<StringBuilder>(KnownDbType.StringBuilder);
    }

    static void Register<T>(KnownDbType dbType)
    {
      Register<T>((short)dbType);
    }

    public static void Register<T>(short dbId)
    {
      lock (_simpleTypes)
        _simpleTypes.Add(dbId, typeof(T));

      lock (_dbTypes)
        _dbTypes.Add(typeof(T), new DbType(dbId));
    }

    public static Type GetType(short type)
    {
      lock (_simpleTypes)
      {
        Type result;

        if (_simpleTypes.TryGetValue(type, out result))
          return result;
      }

      throw new NotSupportedException(string.Format("Unknown type code {0}", type));
    }

    public static DbType TryGetDbType(Type type)
    {
      type = Serializers.GetBinaryType(type);

      lock (_dbTypes)
      {
        DbType result;

        if (_dbTypes.TryGetValue(type, out result))
          return result;

        if (type.IsArray)
        {
          var element = TryGetDbType(type.GetElementType());
          if (element == null)
            return null;

          return _dbTypes[type] = new DbListType(element);
        }

        {
          var elements = GetDictionaryElementTypes(type);
          if (elements != null)
          {
            var key = TryGetDbType(elements.Item1);
            if (key == null)
              return null;

            var value = TryGetDbType(elements.Item2);
            if (value == null)
              return null;

            return _dbTypes[type] = new DbDictType(key, value);
          }
        }

        {
          var element = GetCollectionElementType(type);
          if (element != null)
          {
            var elementDbType = TryGetDbType(element);
            if (elementDbType == null)
              return null;

            return _dbTypes[type] = GetDbType(element.MakeArrayType()); // to reuse cached dbType 
          }
        }
      }
      return null;
    }

    public static DbType GetDbType(Type type)
    {
      var result = TryGetDbType(type);
      
      if (result == null)
        throw new NotSupportedException(string.Format("Serialization of '{0}' is not supported", type));

      return result;
    }

    public static Type GetCollectionElementType(Type type)
    {
#if CORE
      return (from i in type.GetTypeInfo().ImplementedInterfaces
          where i.IsGenericType() && i.GetGenericTypeDefinition() == typeof(ICollection<>)
          select i.GenericTypeArguments[0]).FirstOrDefault();
#else
      return (from i in type.GetInterfaces()
          where i.IsGenericType() && i.GetGenericTypeDefinition() == typeof(ICollection<>)
          select i.GetGenericArguments()[0]).FirstOrDefault();
#endif
    }

    public static Tuple<Type, Type> GetDictionaryElementTypes(Type type)
    {
#if CORE
      return (from i in type.GetTypeInfo().ImplementedInterfaces
              where i.IsGenericType() && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
              let args = i.GenericTypeArguments
              select Tuple.Create(args[0], args[1])).FirstOrDefault();
#else
      return (from i in type.GetInterfaces()
              where i.IsGenericType() && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
              let args = i.GetGenericArguments()
              select Tuple.Create(args[0], args[1])).FirstOrDefault();
#endif
    }
  }
}
