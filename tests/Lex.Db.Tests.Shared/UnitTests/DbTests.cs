using System;
using System.Linq;
using System.Collections.Generic;
using System.Linq.Expressions;

#if NETFX_CORE || WINDOWS_PHONE
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
#elif DROID
using NUnit.Framework;
using TestClassAttribute = NUnit.Framework.TestFixtureAttribute;
using TestInitializeAttribute = NUnit.Framework.SetUpAttribute;
using TestCleanupAttribute = NUnit.Framework.TearDownAttribute;
using TestMethodAttribute = NUnit.Framework.TestAttribute;
using TestContext = System.Console;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#if SILVERLIGHT
using Microsoft.Silverlight.Testing;
using System.IO.IsolatedStorage;
#endif
#endif

namespace Lex.Db
{
#if WINDOWS_PHONE || !SILVERLIGHT
  [AttributeUsage(AttributeTargets.Method)]
  public class AsynchronousAttribute : Attribute { }

  public class WorkItemTest
  {
    public void TestComplete() { }
  }
#endif

  [TestClass]
  public class DbTests : WorkItemTest
  {
#if !DROID
    public TestContext TestContext { get; set; }
#endif

    DbInstance db;
    DbTable<MyData> table;

    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase");
      db.Map<MyData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    [TestMethod]
    public void TestPKTypes()
    {
      TestPKKey(i => i.KeyBool, (o, v) => o.KeyBool = v, true);
      TestPKKey(i => i.KeyBool, (o, v) => o.KeyBool = v, false);

      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, null);
      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, true);
      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, false);

      TestPKKey(i => i.KeyGuid, (o, v) => o.KeyGuid = v, Guid.NewGuid());
      TestPKKey(i => i.KeyGuidN, (o, v) => o.KeyGuidN = v, Guid.NewGuid());
      TestPKKey(i => i.KeyGuidN, (o, v) => o.KeyGuidN = v, null);



    }

    public void TestPKKey<T>(Expression<Func<MyDataKeys, T>> pkGetter, Action<MyDataKeys, T> pkSetter, T key)
    {
      var db = new DbInstance("DbKeys");
      db.Map<MyDataKeys>().Key(pkGetter);
      db.Initialize();
      var getter = pkGetter.Compile();
      var obj1 = new MyDataKeys();
      pkSetter(obj1, key);
      db.Save(obj1);

      var obj2 = db.LoadByKey<MyDataKeys>(key);

      Assert.AreEqual(getter(obj1), getter(obj2));

      db.Purge();
    }
#if NETFX_CORE
    [TestMethod]
    public void TestPackageLocation()
    {
      using (var db = new DbInstance("TestPackage", Windows.ApplicationModel.Package.Current.InstalledLocation))
      {
        db.Map<MyData>().Automap(i => i.Id, true);
        db.Initialize();

        db.Save(new MyData());
      }
    }
#endif

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

#if NETFX_CORE
    [TestMethod]
    public void OpenDbComplexPath2()
    {
      var db = new DbInstance(@"My Database\My Schema", Windows.Storage.ApplicationData.Current.TemporaryFolder);
      db.Initialize();
    }
#else
    public void OpenDbComplexPath2()
    {
      try
      {
        var db = new DbInstance(@"d:\test.db");
        db.Initialize();
      }
#if SILVERLIGHT
      catch (System.IO.IsolatedStorage.IsolatedStorageException) 
      {
        // SL without ElevatedPriviliges does not allow absolute path access
      }
#endif
      finally
      {
      }
    }
#endif

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
    public void Indexing()
    {
      var db = new DbInstance(@"My Database\Indexing");

      db.Map<MyData>().Automap(i => i.Id, true)
        .WithIndex("LastName", i => i.Name, StringComparer.CurrentCulture)
        .WithIndex("LastNameText", i => i.Name, StringComparer.CurrentCultureIgnoreCase);
      db.Initialize();

      var table = db.Table<MyData>();
      table.Purge();

      db.BulkWrite(() =>
      {
        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new MyData { Name = "Test" + i });

        for (var s = 0; s < 100; s++)
          for (var i = 0; i < 10; i++)
            table.Save(new MyData { Name = "TeST" + i });
      });

      var list1count = table.IndexQueryByKey("LastName", "Test5").Count();
      var list2count = table.IndexQueryByKey("LastNameText", "TEst5").Count();

      Assert.AreEqual(list1count, 100);
      Assert.AreEqual(list2count, 200);
    }

    [TestMethod]
    public void IndexingDetails()
    {
      var db = new DbInstance(@"My Database\Indexing2");

      db.Map<MyData>().Automap(i => i.Id, true).WithIndex("Test", i => i.IntField);
      db.Initialize();

      var table = db.Table<MyData>();
      table.Purge();

      db.BulkWrite(() =>
      {
        table.Save(new MyData { IntField = 1 });
        table.Save(new MyData { IntField = 1 });
        table.Save(new MyData { IntField = 1 });
        table.Save(new MyData { IntField = 1 });
        table.Save(new MyData { IntField = 1 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 3 });
        table.Save(new MyData { IntField = 3 });
        table.Save(new MyData { IntField = 3 });
        table.Save(new MyData { IntField = 3 });
        table.Save(new MyData { IntField = 3 });
        table.Save(new MyData { IntField = 4 });
        table.Save(new MyData { IntField = 5 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 6 });
        table.Save(new MyData { IntField = 7 });
        table.Save(new MyData { IntField = 8 });
        table.Save(new MyData { IntField = 8 });
        table.Save(new MyData { IntField = 8 });
        table.Save(new MyData { IntField = 8 });
        table.Save(new MyData { IntField = 8 });
        table.Save(new MyData { IntField = 9 });
      });

      var list1 = table.LoadAll();

      var index = table.IndexQuery<int>("Test");

      Assert.AreEqual(index.Key(1).Count(), list1.Count(i => i.IntField == 1));
      Assert.AreEqual(index.Key(8).Count(), list1.Count(i => i.IntField == 8));

      Assert.AreEqual(index.GreaterThan(6, true).LessThan(8).Count(), list1.Count(i => i.IntField >= 6 && i.IntField < 8));

      IdSequenceEqual(index.GreaterThan(6).LessThan(8).ToList(), list1.Where(i => i.IntField > 6 && i.IntField < 8));
      IdSequenceEqual(index.LessThan(8).ToList(), list1.Where(i => i.IntField < 8));
      IdSequenceEqual(index.GreaterThan(6, true).ToList(), list1.Where(i => i.IntField >= 6));
      IdSequenceEqual(index.GreaterThan(7, true).LessThan(7).ToList(), list1.Where(i => i.IntField >= 7 && i.IntField < 7));
      IdSequenceEqual(index.GreaterThan(7).LessThan(7, true).ToList(), list1.Where(i => i.IntField > 7 && i.IntField <= 7));
    }

    static void IdSequenceEqual(IEnumerable<MyData> a, IEnumerable<MyData> b)
    {
      Assert.IsTrue(a.OrderBy(i => i.Id).Select(i => i.Id).SequenceEqual(b.OrderBy(i => i.Id).Select(i => i.Id)));
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
#if !(NETFX_CORE || WINDOWS_PHONE)
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
        Assert.AreEqual(cnt, load.Length);
      });
    }

    [TestMethod]
    public void Compact()
    {
      table.Compact();
    }

    [TestMethod]
    public void CheckInfo()
    {
      var info1 = table.GetInfo();
      var info2 = db.GetInfo();
    }


    [TestMethod]
    public void RountripNulls()
    {
      var obj = new MyData();

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.BoolNField, newObj.BoolNField);
      Assert.AreEqual(obj.IntNField, newObj.IntNField);
      Assert.AreEqual(obj.LongNField, newObj.LongNField);
      Assert.AreEqual(obj.DoubleNField, newObj.DoubleNField);
      Assert.AreEqual(obj.FloatNField, newObj.FloatNField);
      Assert.AreEqual(obj.DecimalNField, newObj.DecimalNField);
      Assert.AreEqual(obj.TimeSpanNField, newObj.TimeSpanNField);
      Assert.AreEqual(obj.DateTimeNField, newObj.DateTimeNField);
      Assert.AreEqual(obj.DateTimeOffsetNField, newObj.DateTimeOffsetNField);
      Assert.AreEqual(obj.GuidNField, newObj.GuidNField);
      Assert.AreEqual(obj.EnumNField, newObj.EnumNField);
      Assert.AreEqual(obj.Name, newObj.Name);

      var info = table.GetInfo();
      Assert.AreNotEqual(0, info.DataSize);
      Assert.AreNotEqual(0, info.IndexSize);
    }

#region Bool Rountrip Tests

    [TestMethod]
    public void RountripBool1()
    {
      var obj = new MyData { BoolField = true, BoolNField = false };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.BoolField, newObj.BoolField);
      Assert.AreEqual(obj.BoolNField, newObj.BoolNField);
    }

    [TestMethod]
    public void RountripBool2()
    {
      var obj = new MyData { BoolField = false, BoolNField = true };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.BoolField, newObj.BoolField);
      Assert.AreEqual(obj.BoolNField, newObj.BoolNField);
    }

#endregion

#region Int Rountrip Tests

    [TestMethod]
    public void RountripInt1()
    {
      var obj = new MyData { IntField = int.MaxValue, IntNField = int.MinValue };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.IntField, newObj.IntField);
      Assert.AreEqual(obj.IntNField, newObj.IntNField);
    }

    [TestMethod]
    public void RountripInt2()
    {
      var obj = new MyData { IntField = int.MinValue, IntNField = int.MaxValue };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.IntField, newObj.IntField);
      Assert.AreEqual(obj.IntNField, newObj.IntNField);
    }

#endregion

#region Long Rountrip Tests

    [TestMethod]
    public void RountripLong1()
    {
      var obj = new MyData { LongField = long.MaxValue, LongNField = long.MinValue };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.LongField, newObj.LongField);
      Assert.AreEqual(obj.LongNField, newObj.LongNField);
    }

    [TestMethod]
    public void RountripLong2()
    {
      var obj = new MyData { LongField = long.MinValue, LongNField = long.MaxValue };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.LongField, newObj.LongField);
      Assert.AreEqual(obj.LongNField, newObj.LongNField);
    }

#endregion

#region Float Rountrip Tests

    [TestMethod]
    public void RountripFloat1()
    {
      var obj = new MyData { FloatField = (float)Math.PI, FloatNField = (float)-Math.PI };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.FloatField, newObj.FloatField);
      Assert.AreEqual(obj.FloatNField, newObj.FloatNField);
    }

#endregion

#region Double Rountrip Tests

    [TestMethod]
    public void RountripDouble1()
    {
      var obj = new MyData { DoubleField = Math.PI, DoubleNField = -Math.PI };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.DoubleField, newObj.DoubleField);
      Assert.AreEqual(obj.DoubleNField, newObj.DoubleNField);
    }

#endregion

#region Decimal Rountrip Tests

    [TestMethod]
    public void RountripDecimal1()
    {
      var obj = new MyData { DecimalField = (decimal)Math.PI, DecimalNField = (decimal)-Math.PI };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.DecimalField, newObj.DecimalField);
      Assert.AreEqual(obj.DecimalNField, newObj.DecimalNField);
    }

#endregion

#region String Rountrip Tests

    [TestMethod]
    public void RountripString1()
    {
      var obj = new MyData { Name = "Test ABC" };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.Name, newObj.Name);
    }

#endregion

#region Guid Rountrip Tests

    [TestMethod]
    public void RountripGuid1()
    {
      var obj = new MyData { GuidField = Guid.NewGuid(), GuidNField = Guid.NewGuid() };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.GuidField, newObj.GuidField);
      Assert.AreEqual(obj.GuidNField, newObj.GuidNField);
    }

#endregion

#region Enum Rountrip Tests

    [TestMethod]
    public void RountripEnum1()
    {
      var obj = new MyData { EnumField = TestEnum.EnumValue1, EnumNField = TestEnum.EnumValue2 };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.EnumField, newObj.EnumField);
      Assert.AreEqual(obj.EnumNField, newObj.EnumNField);
    }

#endregion

#region TimeSpan Rountrip Tests

    [TestMethod]
    public void RountripTimeSpan1()
    {
      var obj = new MyData { TimeSpanField = new TimeSpan(1, 2, 3), TimeSpanNField = new TimeSpan(2, 3, 4) };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.TimeSpanField, newObj.TimeSpanField);
      Assert.AreEqual(obj.TimeSpanNField, newObj.TimeSpanNField);
    }

#endregion

#region DateTime Rountrip Tests

    [TestMethod]
    public void RountripDateTime1()
    {
      var obj = new MyData { DateTimeField = new DateTime(1, 2, 3, 4, 5, 6), DateTimeNField = new DateTime(2, 3, 4, 5, 6, 7) };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.DateTimeField, newObj.DateTimeField);
      Assert.AreEqual(obj.DateTimeNField, newObj.DateTimeNField);
    }

#endregion

#region DateTimeOffset Rountrip Tests

    [TestMethod]
    public void RountripDateTimeOffset1()
    {
      var obj = new MyData { DateTimeOffsetField = new DateTimeOffset(1, 2, 3, 4, 5, 6, TimeSpan.FromMinutes(60)), DateTimeOffsetNField = new DateTimeOffset(2, 3, 4, 5, 6, 7, TimeSpan.FromMinutes(120)) };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.AreEqual(obj.DateTimeOffsetField, newObj.DateTimeOffsetField);
      Assert.AreEqual(obj.DateTimeOffsetNField, newObj.DateTimeOffsetNField);
    }

#endregion

#region Lists Rountrip Tests

    [TestMethod]
    public void RountripLists1()
    {
      var obj = new MyData
      {
        ListField = new List<int> { 1, 2, 3, 4, 5 },
#if !SILVERLIGHT || WINDOWS_PHONE
        SortedSetField = new SortedSet<int> { 5, 2, 3, 4, 1 },
#endif
        DictField = new Dictionary<string, int> { { "test1", 111 }, { "test2", 222 }, { "test3", 333 } }
      };

      table.Save(obj);

      var newObj = table.LoadByKey(obj.Id);

      Assert.IsTrue(obj.ListField.SequenceEqual(newObj.ListField));
#if !SILVERLIGHT || WINDOWS_PHONE
      Assert.IsTrue(obj.SortedSetField.SequenceEqual(newObj.SortedSetField));
#endif
      Assert.IsTrue(obj.DictField.Keys.SequenceEqual(newObj.DictField.Keys));
      Assert.IsTrue(obj.DictField.Values.SequenceEqual(newObj.DictField.Values));
    }

#endregion

#region Bugfixes

#region github issue #9

    public class TemplateModel
    {
      public int Id { get; set; }
      public string ForeignIds { get; set; }
      public string Name { get; set; }
      public int Type { get; set; }
    }

    void TestDeleteBugfix()
    {
      var db = new DbInstance("test.fix1");

      //mapping done before init
      db.Map<TemplateModel>().Automap(i => i.Id, false).WithIndex<int>("Type", i => i.Type);

      db.Initialize();

      //testing this in a method
      db.Table<TemplateModel>().Save(new TemplateModel { Id = 66, Name = "test", Type = 3 });
      db.Table<TemplateModel>().Save(new TemplateModel { Id = 67, Name = "test2", Type = 3 });
      //The Type is 3 for both records 
      //The first indexQuery returns 2 records, OK!
      var indexQuery = db.Table<TemplateModel>().IndexQueryByKey<int>("Type", 3).ToList();

      db.Table<TemplateModel>().DeleteByKey<int>(67);

      //allItems returns 1 record, OK!
      var allItems = db.Table<TemplateModel>().LoadAll().ToList();

      //indexQuery2 returns 0 records, wrong!
      var indexQuery2 = db.Table<TemplateModel>().IndexQueryByKey<int>("Type", 3).ToList();

      Assert.AreEqual(1, indexQuery2.Count());
    }

#endregion

#region github issue #10

    [TestMethod]
    public void TestLoadByKeyObj()
    {
      var obj = new MyData { GuidField = Guid.NewGuid() };

      table.Save(obj);

      var newObj = table.LoadByKey((object)obj.Id);

      Assert.AreEqual(obj.GuidField, newObj.GuidField);
    }

    [TestMethod]
    public void TestDeleteByKeyObj()
    {
      var obj = new MyData { GuidField = Guid.NewGuid() };

      table.Save(obj);

      Assert.IsTrue(table.DeleteByKey((object)obj.Id));

    }

#endregion

#endregion
  }
}
