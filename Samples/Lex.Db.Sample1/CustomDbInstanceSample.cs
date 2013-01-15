using System;
using System.Collections.Generic;

namespace Lex.Db.Sample1
{
  class CustomDbInstanceSample
  {
    public void Test()
    {
      using (var db = new MyDbInstance("test.db.2"))
      {
        // Upsert / Merge
        db.Save(new Person { Id = "ll", FirstName = "Lex", LastName = "Lavnikov" },
                new Person { Id = "jd", FirstName = "John", LastName = "Doe" },
                new Person { Id = "sgu", FirstName = "Scott", LastName = "Guthrie" },
                new Person { Id = "jp", FirstName = "John", LastName = "Papa" });

        {
          // Query all
          var allPersons = db.Persons.LoadAll();
          WriteToConsole("All persons", allPersons);
        }

        {
          // Query by primary index
          var person = db.Persons.LoadByKey("ll");
          WriteToConsole("By primary key 'll'", person);
        }

        {
          // Query by secondary index (calculated field)
          var persons = db.Persons.IndexQueryByKey("FullName", "Lex Lavnikov").ToList();
          WriteToConsole("By secondary key 'Lex Lavnikov'", persons);
        }

        {
          // Query by secondary index (normal field)
          var persons = db.Persons.IndexQueryByKey("LastName", "Papa").ToList();
          WriteToConsole("By secondary key 'Papa'", persons);
        }

        // Query count
        Console.WriteLine("Count: {0}", db.Persons.Count());
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
    }
  }


  class MyDbInstance : DbInstance
  {
    public MyDbInstance(string path)
      : base(path)
    {
      Map<Person>()
        .Automap(i => i.Id)
        .WithIndex("FullName", i => i.FullName)
        .WithIndex("LastName", i => i.LastName);

      Initialize();
    }

    DbTable<Person> _persons;
    public DbTable<Person> Persons { get { return _persons ?? (_persons = Table<Person>()); } }
  }
}
