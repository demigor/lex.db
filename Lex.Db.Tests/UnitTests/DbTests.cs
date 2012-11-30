using System;
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
  public class DbTests : WorkItemTest
#else
  public class DbTests 
#endif
  {
    public TestContext TestContext { get; set; }

    DbInstance db;
    DbTable<MyData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase");
      db.Map<MyData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    [TestInitialize]
    public void PurgeDb()
    {
      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<MyData>();
    }

    [TestCleanup]
    public void CleanUp()
    {
      db.Purge();
      db.Dispose();
    }

    [TestMethod]
    public void OpenDb()
    {
      var db = new DbInstance("My Database");
      db.Initialize();
    }

    [TestMethod]
    public void OpenDbComplexPath()
    {
      var db = new DbInstance(@"My Database\My Schema");
      db.Initialize();
    }

    [TestMethod]
//    [ExpectedException(typeof(InvalidOperationException))]
    public void DoubleOpenDbComplexPath()
    {
      try
      {
        var db = new DbInstance(@"My Database\My Schema");
        db.Initialize();
        db.Initialize();

        Assert.Fail("InvalidOperationException expected");
      }
      catch (InvalidOperationException)
      {
      }
    }

    [TestMethod]
//    [ExpectedException(typeof(InvalidOperationException))]
    public void MapDb()
    {
      try
      {
        var db = new DbInstance(@"My Database\My Schema");
        db.Map<MyData>();

        db.Initialize();

        
        Assert.Fail("InvalidOperationException expected");
      }
      catch (InvalidOperationException)
      {
      }
    }

    [TestMethod]
//    [ExpectedException(typeof(InvalidOperationException))]
    public void MapDbWrong()
    {
      try
      {
        var db = new DbInstance(@"My Database\My Schema");

        db.Initialize();

        db.Map<MyData>().Automap(i => i.Id);

        Assert.Fail("InvalidOperationException expected");
      }
      catch (InvalidOperationException)
      {
      }
    }

    [TestMethod]
    public void LoadData()
    {
      var table = db.Table<MyData>();
      var items = table.LoadAll();
    }

    [TestMethod]
    public void SaveData()
    {
      var swatch = DateTime.Now;

      db.BulkWrite(() =>
      {
        table.Purge();
        var key = 1;
        var newObj = new MyData { Id = key, Name = "test" };
        table.Save(newObj);

        var obj = table.LoadByKey(key);

        Assert.AreEqual(newObj.Name, obj.Name);
#if !NETFX_CORE
        TestContext.WriteLine("Completed: " + (DateTime.Now - swatch).TotalMilliseconds);
#endif
      });
    }

    [TestMethod]
    public void SaveDataBulk()
    {
      db.BulkWrite(() =>
      {
        var cnt = DoSaveDataBulk();

        Assert.AreEqual(table.Count(), cnt);
      });
    }

    int DoSaveDataBulk()
    {
      table.Purge();
      var list = new List<MyData>();
      var cnt = 50000;
      for (int i = 0; i < cnt; i++)
        list.Add(new MyData { Name = "test " + i, LastName = "My Some Last Name " + i });

      table.Save(list);
      return cnt;
    }

    [TestMethod]
    public void LoadDataBulk()
    {
      db.BulkWrite(() =>
      {
        var cnt = DoSaveDataBulk();
        var load = table.LoadAll();
        Assert.AreEqual(cnt, load.Count);
      });
    }

    [TestMethod]
    public void Compact()
    {
      table.Compact();
    }
  }
}
