using System;
using System.IO;

namespace Lex.Db
{
  static partial class Extensions
  {
    public static bool Like(this string source, string substring, StringComparison comparison = StringComparison.CurrentCultureIgnoreCase)
    {
      return source != null && substring != null && source.IndexOf(substring, comparison) >= 0;
    }
  }
}
