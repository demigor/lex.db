using System.Windows;
using Microsoft.Silverlight.Testing;

namespace Lex.Db.Tests.SL5
{
  public partial class App : Application
  {

    public App()
    {
      this.Startup += this.Application_Startup;

      InitializeComponent();
    }

    private void Application_Startup(object sender, StartupEventArgs e)
    {
      this.RootVisual = UnitTestSystem.CreateTestPage();
    }
  }
}
