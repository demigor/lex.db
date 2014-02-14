using System;
using System.Linq;
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

    public void Test2()
    {
      var d = Test2Init();

      for (var i = 0; i < 100; i++)
        Test2Modify(d);
    }

    const int t2Count = 100000;

    Dictionary<string, Person> Test2Init()
    {
      Console.WriteLine("Generating stuff...");

      var r = new Random();
      var result = new Dictionary<string, Person>();

      using (var db = new MyDbInstance("test.db.3"))
      {
        db.Purge();

        var people = db.Table<Person>();

        for (var i = 0; i < t2Count; i++)
        {
          var p = new Person { Id = "T" + i, FirstName = "A" + r.Next(1000), LastName = "B" + r.Next(1000) };
          result.Add(p.Id, p);
        }

        people.Save(result.Values);
      }

      return result;
    }

    void Test2Modify(Dictionary<string, Person> dic)
    {
      Console.WriteLine("Deleting stuff...");
      Person p;

      var r = new Random();

      using (var db = new MyDbInstance("test.db.3"))
      {
        var people = db.Table<Person>();
        var keys = new List<string>();

        for (var i = 0; i < 1000; i++)
        {
          var x = "T" + r.Next(t2Count);

          if (dic.TryGetValue(x, out p))
          {
            keys.Add(p.Id);
            dic.Remove(p.Id);
          }
        }
        people.DeleteByKeys(keys);
      }

      Console.WriteLine("Modifying stuff...");

      using (var db = new MyDbInstance("test.db.3"))
      {
        var people = db.Table<Person>();
        var updates = new List<Person>();

        for (var i = 0; i < 1000; i++)
        {
          var x = "T" + r.Next(t2Count);

          if (dic.TryGetValue(x, out p))
          {
            p.FirstName = "A" + r.Next(1000);
            p.LastName = "B" + r.Next(1000);

            updates.Add(p);
          }
        }
        people.Save(updates);
      }

      Console.WriteLine("Comparing stuff...");

      using (var db = new MyDbInstance("test.db.3"))
      {
        var people = db.Table<Person>().LoadAll().OrderBy(i => i.Id).ToArray();
        var dicPeople = dic.Values.OrderBy(i => i.Id).ToArray();

        if (!people.Select(i => i.Id).SequenceEqual(dicPeople.Select(i => i.Id)))
          throw new InvalidOperationException("Id mismatch");

        if (!people.Select(i => i.FirstName).SequenceEqual(dicPeople.Select(i => i.FirstName)))
          throw new InvalidOperationException("FirstName mismatch");

        if (!people.Select(i => i.LastName).SequenceEqual(dicPeople.Select(i => i.LastName)))
          throw new InvalidOperationException("LastName mismatch");
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
