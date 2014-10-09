using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lex.Db.Profiling
{
  class Program
  {
    DbInstance Prepare()
    {
      var db = new DbInstance("MyDatabase");
      db.Map<MyData>().Automap(i => i.Id, true);
      db.Initialize();
      return db;
    }

    public void LoadDataBulk()
    {
      db.BulkWrite(() =>
      {
        var cnt = DoSaveDataBulk();
        var load = table.LoadAll();

        if (cnt != load.Length)
          throw new InvalidProgramException();
      });
    }

    public void SaveDataBulk()
    {
      db.BulkWrite(() =>
      {
        var cnt = DoSaveDataBulk();

        if (cnt != table.Count())
          throw new InvalidProgramException();
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

    DbInstance db;
    DbTable<MyData> table;

    public Program()
    {
      using (var i = Prepare())
        i.Purge();

      db = Prepare();
      table = db.Table<MyData>();
    }

    static void Main(string[] args)
    {
      var s = Stopwatch.StartNew();

      var prog = new Program();
      prog.LoadDataBulk();
      prog.SaveDataBulk();
      prog.LoadDataBulk();
      prog.SaveDataBulk();
      prog.LoadDataBulk();
      prog.SaveDataBulk();
      prog.LoadDataBulk();
      prog.SaveDataBulk();
      prog.LoadDataBulk();
      s.Stop();

      Console.WriteLine("took : {0}", s.ElapsedMilliseconds);
    }
  }
}
