using System;

namespace Lex.Db.Serialization
{
  /// <summary>
  /// Incapsulates logic to decide which members should be serialized
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  public abstract class Interceptor<T>
  {
    internal Interceptor<T> _next;

    internal bool Filter(T instance, string member)
    {
      var scan = this;

      do
      {
        var result = scan.NeedSerialize(instance, member);
        if (result != null)
          return result.Value;

        scan = scan._next;
      } while (scan != null);

      return true;
    }

    /// <summary>
    /// Implements deciding logic whether member should be stored in table or not
    /// </summary>
    /// <param name="instance">Entity instance to inspect</param>
    /// <param name="member">Name of the member to serialize</param>
    /// <returns>True is member should be serialized, false otherwise</returns>
    protected abstract bool? NeedSerialize(T instance, string member);
  }

  sealed class DelegateInterceptor<T> : Interceptor<T>
  {
    readonly Func<T, string, bool?> _filter;

    public DelegateInterceptor(Func<T, string, bool?> filter)
    {
      _filter = filter;
    }

    protected override bool? NeedSerialize(T instance, string member)
    {
      return _filter(instance, member);
    }
  }
}
