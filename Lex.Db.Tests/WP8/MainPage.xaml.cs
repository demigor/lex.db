using System.Windows;
using Microsoft.Phone.Controls;
using Microsoft.Phone.Testing;

namespace Lex.Db.Tests.WP8
{
  public partial class MainPage : PhoneApplicationPage
  {
    public MainPage()
    {
      InitializeComponent();
    }

    private void PhoneApplicationPage_Loaded_1(object sender, RoutedEventArgs e)
    {
      Content = UnitTestSystem.CreateTestPage();
    }
  }
}