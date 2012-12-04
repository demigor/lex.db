namespace Lex.Db
{
  interface IDbStorage
  {
    IDbSchemaStorage OpenSchema(string path);

    bool IncreaseQuotaTo(long quota);

    bool HasEnoughQuota(long quota);
  }
}
