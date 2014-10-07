using System;
using System.Linq.Expressions;
#if NLOG
using NLog;
#endif

namespace Lex.Db
{
  /// <summary>
  /// Extreme fast generic constructor. 
  /// </summary>
  /// <remarks>
  /// About 20 times faster than "new T()", because the last one uses slow reflection-based Activator.CreateInstance internally.
  /// </remarks>
  /// <typeparam name="T">Type to construct</typeparam>
  static class Ctor<T>
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    static Ctor()
    {
      Log.Trace("Constructor for {0} created", typeof(T).Name);
    }
#endif

    /// <summary>
    /// Generic <typeparamref name="T"/> constructor function
    /// </summary>
#if iOS
    public static readonly Func<T> New = () => Activator.CreateInstance<T>();
#else
    public static readonly Func<T> New = Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();
#endif
  }

  /// <summary>
  /// Extreme fast generic constructor. Constructs <typeparamref name="T"/>, but returns <typeparamref name="R"/>. So <typeparamref name="T"/> must be direct assignable to <typeparamref name="R"/>.
  /// </summary>
  /// <remarks>
  /// About 20 times faster than "new T()", because the last one uses slow reflection-based Activator.CreateInstance internally.
  /// </remarks>
  /// <typeparam name="T">Type to construct</typeparam>
  /// <typeparam name="R">Type to return</typeparam>
  static class Ctor<R, T> where T : R
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    static Ctor()
    {
      Log.Trace("Constructor for {0} as {1} created", typeof(T).Name, typeof(R).Name);
    }
#endif

    /// <summary>
    /// Generic <typeparamref name="T"/> constructor function, returning <typeparamref name="T"/> as <typeparamref name="R"/> type
    /// </summary>
#if iOS
    public static readonly Func<R> New = () => (R)Activator.CreateInstance<T>();
#else
    public static readonly Func<R> New = Expression.Lambda<Func<R>>(Expression.New(typeof(T))).Compile();
#endif
  }
}
