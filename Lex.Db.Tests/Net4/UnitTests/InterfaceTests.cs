﻿using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lex.Db.UnitTests
{
    [TestClass]
    public class InterfaceTests
    {
        public TestContext TestContext { get; set; }

        DbInstance db;
        DbTable<IMyData> table;

        DbInstance Prepare()
        {
            var db = new DbInstance("MyDatabase");
            db.Map<IMyData, MyData>().Automap(i => i.Id, true);
            db.Initialize();
            return db;
        }

        [TestInitialize]
        public void PurgeDb()
        {
            using (var i = Prepare())
                i.Purge();

            db = Prepare();
            table = db.Table<IMyData>();
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
        public void Indexing()
        {
            var db = new DbInstance(@"My Database\Indexing");

            db.Map<IMyData, MyData>().Automap(i => i.Id, true)
              .WithIndex("LastName", i => i.Name, StringComparer.CurrentCulture)
              .WithIndex("LastNameText", i => i.Name, StringComparer.CurrentCultureIgnoreCase);
            db.Initialize();

            var table = db.Table<IMyData>();
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

            var list1 = table.LoadAll("LastName", "Test5");
            var list2 = table.LoadAll("LastNameText", "TEst5");

            Assert.AreEqual(list1.Count, 100);
            Assert.AreEqual(list2.Count, 200);
        }

        [TestMethod]
        public void LoadData()
        {
            var table = db.Table<IMyData>();
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

        #region DateTime Rountrip Tests

        [TestMethod]
        public void RountripList1()
        {
            var obj = new MyData
            {
                ListField = { 1, 2, 3, 4, 5 },
                DictField = { { "test1", 111 }, { "test2", 222 }, { "test3", 333 } }
            };

            table.Save(obj);

            var newObj = table.LoadByKey(obj.Id);

            Assert.IsTrue(obj.ListField.SequenceEqual(newObj.ListField));
            Assert.IsTrue(obj.DictField.Keys.SequenceEqual(newObj.DictField.Keys));
            Assert.IsTrue(obj.DictField.Values.SequenceEqual(newObj.DictField.Values));
        }

        #endregion
    }
}
