using System;

namespace Lex.Db.Serialization
{
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
