using System;
using Lex.Db.Serialization;

namespace Lex.Db
{
  public class DbTests3: IDisposable
  {
    DbInstance db;
    DbTable<AData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase3");
      db.Map<AData, PrototypeBasedData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    public DbTests3()
    {
      PurgeDb();
    }

    public void Dispose()
    {
      CleanUp();
    }

    public void PurgeDb()
    {
      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<AData>();
    }

    public void CleanUp()
    {
      db.Purge();
      db.Dispose();
    }

    public void Indexing3()
    {
      var db = new DbInstance(@"MyDatabase3\Indexing");

      db.Map<AData, PrototypeBasedData>().Automap(i => i.Id, true)
        .WithIndex("LastName", i => i.Name, StringComparer.CurrentCulture)
        .WithIndex("LastNameText", i => i.Name, StringComparer.CurrentCultureIgnoreCase);
      db.Initialize();

      var table = db.Table<AData>();
      table.Purge();

      db.BulkWrite(() =>
      {
        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new PrototypeBasedData { Name = "Test" + i });

        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new PrototypeBasedData { Name = "TeST" + i });
      });

      var list1count = table.IndexQueryByKey("LastName", "Test5").Count();
      var list2count = table.IndexQueryByKey("LastNameText", "TEst5").Count();

      Assert.AreEqual(list1count, 100);
      Assert.AreEqual(list2count, 200);
    }

    public void LoadData3()
    {
      var table = db.Table<AData>();
      var items = table.LoadAll();
    }

    public void SaveData3()
    {
      var swatch = DateTime.Now;

      db.BulkWrite(() =>
      {
        table.Purge();
        var key = 1;
        var newObj = new PrototypeBasedData { Id = key, Name = "test" };
        table.Save(newObj);

        var obj = table.LoadByKey(key);

        Assert.AreEqual(newObj.Name, obj.Name);
#if !(NETFX_CORE || WINDOWS_PHONE)
        TestContext.WriteLine("Completed: " + (DateTime.Now - swatch).TotalMilliseconds);
#endif
      });
    }

    public class Person
    {
      public int PersonID;
      public string Forename, Surname;
    }

    public void TestGordon()
    {
      var db = new DbInstance("gordon.db");
      db.Map<Person>().Automap(i => i.PersonID, true).WithIndex("Surname", i => i.Surname);
      db.Initialize();

      var table = db.Table<Person>();

      table.Purge();

      Person newPerson1 = new Person { Forename = "Joe", Surname = "Bloggs" };
      Person newPerson2 = new Person { Forename = "James", Surname = "Smith" };
      Person newPerson3 = new Person { Forename = "David", Surname = "Peterson" };
      Person newPerson4 = new Person { Forename = "Steve", Surname = "Gordon" };
      Person newPerson5 = new Person { Forename = "David", Surname = "Gordon" };
      Person newPerson6 = new Person { Forename = "Colin", Surname = "Gordon" };
      Person newPerson7 = new Person { Forename = "Michael", Surname = "Gordon" };

      var newPeople = new[]
      {
        newPerson1,
        newPerson2,
        newPerson3,
        newPerson4,
        newPerson5,
        newPerson6,
        newPerson7
      };

      table.Save(newPeople);

      var index = table.IndexQuery<string>("Surname");
      // HIJKLMNOPQRS

      var queryindex = index.GreaterThan("H", true).LessThan("T", true).ToLazyList();
      Assert.AreEqual(2, queryindex.Count);
    }

    public static void RunTests()
    {
      using (var t = new DbTests3())
        t.Indexing3();

      using (var t = new DbTests3())
        t.LoadData3();

      using (var t = new DbTests3())
        t.SaveData3();

      using (var t = new DbTests3())
        t.TestGordon();
    }
  }

  public struct Point
  {
    public int X, Y;
  }

  public class PointSerializer
  {
    public static Point ReadPoint(DataReader reader)
    {
      return new Point { X = reader.ReadInt32(), Y = reader.ReadInt32() };
    }

    public static void WritePoint(DataWriter writer, Point point)
    {
      writer.Write(point.X);
      writer.Write(point.Y);
    }
  }

  public static class Assert
  {
    public static void AreEqual(object expected, object value)
    {
      if (!Equals(expected, value))
        throw new InvalidOperationException();
    }

    public static void Fail(string message)
    {
      throw new InvalidOperationException(message);
    }

    public static void IsTrue(bool condition)
    {
      if (!condition)
        throw new InvalidOperationException();
    }

    public static void AreNotEqual(object expected, object value)
    {
      if (Equals(expected, value))
        throw new InvalidOperationException();
    }
  }
}
