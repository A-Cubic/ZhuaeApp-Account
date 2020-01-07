using System;
using Com.ACBC.Framework.Database;
namespace QuartzRedis.Common
{
    public class DBManagerZE : IType
    {
        private DBType dbt;
        private string str = "";

        public DBManagerZE()
        {
            var url = System.Environment.GetEnvironmentVariable("ZEMysqlDBUrl", EnvironmentVariableTarget.User);
            var uid = System.Environment.GetEnvironmentVariable("ZEMysqlDBUser", EnvironmentVariableTarget.User);
            var port = System.Environment.GetEnvironmentVariable("ZEMysqlDBPort", EnvironmentVariableTarget.User);
            var passd = System.Environment.GetEnvironmentVariable("ZEMysqlDBPassword", EnvironmentVariableTarget.User);

            this.str = "Server=" + url
                     + ";Port=" + port
                     + ";Database=dolldb;Uid=" + uid
                     + ";Pwd=" + passd
                     + ";CharSet=utf8mb4; SslMode =none;";

            Console.Write(this.str);
            this.dbt = DBType.Mysql;
        }

        public DBManagerZE(DBType d, string s)
        {
            this.dbt = d;
            this.str = s;
        }

        public DBType getDBType()
        {
            return dbt;
        }

        public string getConnString()
        {
            return str;
        }

        public void setConnString(string s)
        {
            this.str = s;
        }
    }
}

