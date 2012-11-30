using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
#if NETFX_CORE
using System.Linq;
#endif

namespace Lex.Db
{
  public static class TypeHelper
  {
#if NETFX_CORE
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Type> GetInterfaces(this Type type)
    {
      return type.GetTypeInfo().ImplementedInterfaces;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Type[] GetGenericArguments(this Type type)
    {
      return type.GenericTypeArguments;
    }

    public static MethodInfo GetMethod(this Type type, string name)
    {
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.IsPublic && i.Name == name); // add hierarchy
    }

    public static MethodInfo GetMethod(this Type type, string name, params Type[] args)
    {
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.IsPublic && i.Name == name && CheckArgs(i.GetParameters(), args)); // add hierarchy
    }

    static bool CheckArgs(ParameterInfo[] parameterInfo, Type[] args)
    {
      if (args.Length != parameterInfo.Length)
        return false;

      for (var i = 0; i < args.Length; i++)
        if (parameterInfo[i].ParameterType != args[i])
          return false;

      return true;
    }

    public static ConstructorInfo GetConstructor(this Type type, params Type[] args)
    {
      return type.GetTypeInfo().DeclaredConstructors.SingleOrDefault(i => i.IsPublic && CheckArgs(i.GetParameters(), args)); // add hierarchy
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MethodInfo GetGetMethod(this PropertyInfo prop)
    {
      return prop.GetMethod;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MethodInfo GetSetMethod(this PropertyInfo prop)
    {
      return prop.SetMethod;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsValueType(this Type type)
    {
      return type.GetTypeInfo().IsValueType;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsGenericType(this Type type)
    {
      return type.GetTypeInfo().IsGenericType;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsEnum(this Type type)
    {
      return type.GetTypeInfo().IsEnum;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T GetCustomAttribute<T>(this Type type) where T: Attribute 
    {
       return type.GetTypeInfo().GetCustomAttribute<T>();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsAssignableFrom(this Type type, Type source) 
    {
      return type.GetTypeInfo().IsAssignableFrom(source.GetTypeInfo());
    }

#else

#if !NET40
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    public static T GetCustomAttribute<T>(this Type type) where T : Attribute
    {
      return (T)Attribute.GetCustomAttribute(type, typeof(T));
    }

#if !NET40
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    public static bool IsValueType(this Type type)
    {
      return type.IsValueType;
    }

#if !NET40
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    public static bool IsGenericType(this Type type)
    {
      return type.IsGenericType;
    }

#if !NET40
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
    public static bool IsEnum(this Type type)
    {
      return type.IsEnum;
    }

#endif

    public static IEnumerable<MethodInfo> GetStaticMethods(this Type type)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredMethods.Where(i => i.IsStatic); // add hierarchy
#else
      return type.GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
#endif
    }

    public static MethodInfo GetPublicStaticMethod(this Type type, string name)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.Name == name && i.IsStatic && i.IsPublic); // add hierarchy
#else
      return type.GetMethod(name, BindingFlags.Static | BindingFlags.Public);
#endif
    }

    public static MethodInfo GetPublicInstanceMethod(this Type type, string name)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.Name == name && !i.IsStatic && i.IsPublic); // add hierarchy
#else
      return type.GetMethod(name, BindingFlags.Instance | BindingFlags.Public);
#endif
    }

    public static MethodInfo GetStaticMethod(this Type type, string name)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.Name == name && i.IsStatic); // add hierarchy
#else
      return type.GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
#endif
    }

    public static MethodInfo GetPrivateInstanceMethod(this Type type, string name)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredMethods.SingleOrDefault(i => i.Name == name && !i.IsStatic && i.IsPrivate); // add hierarchy
#else
      return type.GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic);
#endif
    }

    public static IEnumerable<FieldInfo> GetPublicInstanceFields(this Type type)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredFields.Where(i => !i.IsStatic && i.IsPublic); // add hierarchy
#else
      return type.GetFields(BindingFlags.Public | BindingFlags.Instance);
#endif
    }

    public static IEnumerable<PropertyInfo> GetPublicInstanceProperties(this Type type)
    {
#if NETFX_CORE
      return type.GetTypeInfo().DeclaredProperties.Where(i => i.GetMethod != null && !i.GetMethod.IsStatic && i.GetMethod.IsPublic); // add hierarchy
#else
      return type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
#endif
    }
  }
}
