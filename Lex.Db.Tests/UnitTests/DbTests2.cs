using System;
using System.Linq;
using System.Collections.Generic;
#if NETFX_CORE
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#if WINDOWS_PHONE
using Microsoft.Phone.Testing;
#elif SILVERLIGHT
using Microsoft.Silverlight.Testing;
#endif
#endif

namespace Lex.Db.Silverlight
{
  [TestClass]
#if SILVERLIGHT
  public class DbTests2 : WorkItemTest
#else
  public class DbTests2
#endif
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

      var list1 = table.LoadAll("LastName", "Test5");
      var list2 = table.LoadAll("LastNameText", "TEst5");

      Assert.AreEqual(list1.Count, 100);
      Assert.AreEqual(list2.Count, 200);
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
#if !NETFX_CORE
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

      var list1 = table.LoadAll("LastName", "Test5");
      var list2 = table.LoadAll("LastNameText", "TEst5");

      Assert.AreEqual(list1.Count, 100);
      Assert.AreEqual(list2.Count, 200);
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
#if !NETFX_CORE
        TestContext.WriteLine("Completed: " + (DateTime.Now - swatch).TotalMilliseconds);
#endif
      });
    }
  }
}
