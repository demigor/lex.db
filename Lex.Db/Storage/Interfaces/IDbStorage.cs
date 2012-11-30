namespace Lex.Db
{
  public interface IDbStorage
  {
    IDbSchemaStorage OpenSchema(string path);

    bool IncreaseQuotaTo(long quota);

    bool HasEnoughQuota(long quota);
  }
}
