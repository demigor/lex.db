using System;
using Lex.Db.Serialization;

namespace Lex.Db
{
  public class DbTests2 : IDisposable
  {
    DbInstance db;
    DbTable<IData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase2");
      db.Map<IData, InterfaceBasedData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    public DbTests2()
    {
      PurgeDb();
    }

    public void Dispose()
    {
      CleanUp();
    }

    public void PurgeDb()
    {
      try
      {
        Extender.RegisterType<Point, PointSerializer>(2000);
      }
      catch { }

      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<IData>();
    }

    public void CleanUp()
    {
      db.Purge();
      db.Dispose();
    }

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

    public void LoadData2()
    {
      var table = db.Table<IData>();
      var items = table.LoadAll();
    }

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

    public static void RunTests()
    {
      using (var t = new DbTests2())
        t.Indexing2();

      using (var t = new DbTests2())
        t.SaveData2();

      using (var t = new DbTests2())
        t.LoadData2();
    }
  }
}
