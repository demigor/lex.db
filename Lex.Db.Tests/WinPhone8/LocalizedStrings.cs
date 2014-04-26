using Lex.Db.Tests.WinPhone8.Resources;

namespace Lex.Db.Tests.WinPhone8
{
  /// <summary>
  /// Provides access to string resources.
  /// </summary>
  public class LocalizedStrings
  {
    private static AppResources _localizedResources = new AppResources();

    public AppResources LocalizedResources { get { return _localizedResources; } }
  }
}