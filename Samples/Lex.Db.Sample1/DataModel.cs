namespace Lex.Db.Sample1
{
  public class Person
  {
    public string Id { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string FullName { get { return FirstName + " " + LastName; } }
  }
}
