using System;
using System.Linq.Expressions;
#if NLOG
using NLog;
#endif

namespace Lex.Db
{
  static class Ctor<T>
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    static Ctor()
    {
      Log.Trace("Constructor for {0} created", typeof(T).Name);
    }
#endif
    public static readonly Func<T> New = Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();
  }

  static class Ctor<TInterface, TType> where TType : TInterface
  {
#if NLOG
    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    static Ctor()
    {
      Log.Trace("Constructor for {0} created", typeof(T).Name);
    }
#endif
      public static readonly Func<TInterface> New = Expression.Lambda<Func<TInterface>>(Expression.New(typeof(TType))).Compile();
  }
}
