using System;
using System.Linq;
using System.Collections.Generic;
using Lex.Db.Serialization;

#if NETFX_CORE || WINDOWS_PHONE
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
#if SILVERLIGHT
using Microsoft.Silverlight.Testing;
#endif
#endif

namespace Lex.Db
{
  [TestClass]
  public class DbTests2 : WorkItemTest
  {
    public TestContext TestContext { get; set; }

    DbInstance db;
    DbTable<IData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase2");
      db.Map<IData, InterfaceBasedData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    static DbTests2()
    {
      Serialization.Extender.RegisterType<Point, PointSerializer>(2000);
    }

    [TestInitialize]
    public void PurgeDb()
    {
      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<IData>();
    }

    [TestCleanup]
    public void CleanUp()
    {
      db.Purge();
      db.Dispose();
    }

    [TestMethod]
    public void Indexing2()
    {
      var db = new DbInstance(@"MyDatabase2\Indexing");

      db.Map<IData, InterfaceBasedData>().Automap(i => i.Id, true)
        .WithIndex("LastName", i => i.Name, StringComparer.CurrentCulture)
        .WithIndex("LastNameText", i => i.Name, StringComparer.CurrentCultureIgnoreCase);
      db.Initialize();

      var table = db.Table<IData>();
      table.Purge();

      db.BulkWrite(() =>
      {
        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new InterfaceBasedData { Name = "Test" + i });

        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new InterfaceBasedData { Name = "TeST" + i });
      });

      var list1count = table.IndexQuery<string>("LastName").Key("Test5").Count();
      var list2count = table.IndexQuery<string>("LastNameText").Key("TEst5").Count();

      Assert.AreEqual(list1count, 100);
      Assert.AreEqual(list2count, 200);

      var list3count = table.IndexQuery<string>("LastName").GreaterThan("Test5").Count();
      var list4count = table.IndexQuery<string>("LastName").LessThan("Test6").Count();
      var list5count = table.IndexQuery<string>("LastName").LessThan("Test6").GreaterThan("Test5").Count();

      Assert.AreEqual(900, list3count);
      Assert.AreEqual(1200, list4count);
      Assert.AreEqual(100, list5count);

      var list6count = table.IndexQuery<string>("LastName").GreaterThan("Test5", true).Count();
      var list7count = table.IndexQuery<string>("LastName").LessThan("Test6", true).Count();
      var list8count = table.IndexQuery<string>("LastName").LessThan("Test6", true).GreaterThan("Test5", true).Count();

      Assert.AreEqual(1000, list6count);
      Assert.AreEqual(1300, list7count);
      Assert.AreEqual(300, list8count);
    }

    [TestMethod]
    public void LoadData2()
    {
      var table = db.Table<IData>();
      var items = table.LoadAll();
    }

    [TestMethod]
    public void SaveData2()
    {
      var swatch = DateTime.Now;

      db.BulkWrite(() =>
      {
        table.Purge();
        var key = 1;
        var newObj = new InterfaceBasedData { Id = key, Name = "test" };
        table.Save(newObj);

        var obj = table.LoadByKey(key);

        Assert.AreEqual(newObj.Name, obj.Name);
#if !(NETFX_CORE || WINDOWS_PHONE)
        TestContext.WriteLine("Completed: " + (DateTime.Now - swatch).TotalMilliseconds);
#endif
      });
    }
  }

  [TestClass]
#if SILVERLIGHT
  public class DbTests3 : WorkItemTest
#else
  public class DbTests3
#endif
  {
    public TestContext TestContext { get; set; }

    DbInstance db;
    DbTable<AData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase3");
      db.Map<AData, PrototypeBasedData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    [TestInitialize]
    public void PurgeDb()
    {
      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<AData>();
    }

    [TestCleanup]
    public void CleanUp()
    {
      db.Purge();
      db.Dispose();
    }

    [TestMethod]
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

    [TestMethod]
    public void LoadData3()
    {
      var table = db.Table<AData>();
      var items = table.LoadAll();
    }

    [TestMethod]
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

    [TestMethod]
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
}
