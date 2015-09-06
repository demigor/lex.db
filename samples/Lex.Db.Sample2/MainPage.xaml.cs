using System;
using System.Linq.Expressions;
using Windows.UI.Xaml.Controls;

// The Blank Page item template is documented at http://go.microsoft.com/fwlink/?LinkId=402352&clcid=0x409

namespace Lex.Db.Sample2
{
  /// <summary>
  /// An empty page that can be used on its own or navigated to within a Frame.
  /// </summary>
  public sealed partial class MainPage : Page
  {
    public MainPage()
    {
      this.InitializeComponent();

      TestPKTypes();
    }

    public void TestPKTypes()
    {
      TestPKKey(i => i.KeyBool, (o, v) => o.KeyBool = v, true);
      TestPKKey(i => i.KeyBool, (o, v) => o.KeyBool = v, false);

      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, true);
      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, false);
      TestPKKey(i => i.KeyBoolN, (o, v) => o.KeyBoolN = v, null);

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

      if (!Equals(getter(obj1), getter(obj2)))
        throw new InvalidProgramException();

      db.Purge();
    }
  }
 }
