using System;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_SqlServer_DatabaseName";
        private const string ConnectionStringTemplateVariable = "Hangfire_SqlServer_ConnectionStringTemplate";

        private const string MasterDatabaseName = "oracle";
        private const string DefaultDatabaseName = @"Hangfire.Oracle.Tests";
        private const string DefaultConnectionStringTemplate =
            "server=127.0.0.1;uid=root;pwd=root;database={0};Allow User Variables=True";
        private const string DefaultConnectionString =
            "User Id=GESCOOPER;Password=G35C00p3r;Data Source=192.168.1.187:1521/cooperaguas.infogen.local";

            
        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetMasterConnectionString()
        {
            //return String.Format(GetConnectionStringTemplate(), MasterDatabaseName);
            return DefaultConnectionString;
        }

        public static string GetConnectionString()
        {
            //var conStr = String.Format(GetConnectionStringTemplate(), GetDatabaseName());

            return DefaultConnectionString;
        }

        private static string GetConnectionStringTemplate()
        {
            var template = Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;
            return template;
        }

        public static OracleConnection CreateConnection()
        {
            var connection = new OracleConnection(GetConnectionString());
            connection.Open();

            return connection;
        }
    }
}
