using System;
using System.Collections.Generic;

namespace Lex.Db.Sample1
{
  class RawDbInstanceSample
  {
    public void Test()
    {
      using (var db = OpenInstance())
      {
        // Upsert / Merge
        db.Save(new Person { Id = "ll", FirstName = "Lex", LastName = "Lavnikov" },
                new Person { Id = "jd", FirstName = "John", LastName = "Doe" },
                new Person { Id = "sgu", FirstName = "Scott", LastName = "Guthrie" },
                new Person { Id = "jp", FirstName = "John", LastName = "Papa" });

        {
          // Query all
          var allPersons = db.LoadAll<Person>();
          WriteToConsole("All persons", allPersons);
        }

        {
          // Query by primary index
          var person = db.LoadByKey<Person>("ll");
          WriteToConsole("By primary key 'll'", person);
        }

        {
          // Query by secondary index (calculated field)
          var persons = db.Table<Person>().IndexQueryByKey("FullName", "Lex Lavnikov").ToList();
          WriteToConsole("By secondary key 'Lex Lavnikov'", persons);
        }

        {
          // Query by secondary index (normal field)
          var persons = db.Table<Person>().IndexQueryByKey("LastName", "Papa").ToList();
          WriteToConsole("By secondary key 'Papa'", persons);
        }

        // Query count
        Console.WriteLine("Count: {0}", db.Count<Person>());
      }
    }

    static void WriteToConsole(string title, params Person[] items)
    {
      WriteToConsole(title, (IEnumerable<Person>)items);
    }

    static void WriteToConsole(string title, IEnumerable<Person> items)
    {
      Console.WriteLine(title);
      foreach (var i in items)
        Console.WriteLine("Id: {0}, First Name: {1}, Last Name: {2}, Full Name: {3}", i.Id, i.FirstName, i.LastName, i.FullName);
      Console.WriteLine();
    }

    static DbInstance OpenInstance()
    {
      var result = new DbInstance("test.db");
      result.Map<Person>().Automap(i => i.Id).
        WithIndex("FullName", i => i.FullName).
        WithIndex("LastName", i => i.LastName);
      result.Initialize();
      return result;
    }
  }
}
