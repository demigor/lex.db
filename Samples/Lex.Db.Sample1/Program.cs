using System;

namespace Lex.Db.Sample1
{
  class Program
  {
    static void Main(string[] args)
    {
      Console.WriteLine("Raw DbInstance");
      new RawDbInstanceSample().Test();

      Console.WriteLine("Custom DbInstance");
      new CustomDbInstanceSample().Test();
      
      Console.WriteLine("Custom DbInstance Brute Force test");
      new CustomDbInstanceSample().Test2();
    }
  }
}
