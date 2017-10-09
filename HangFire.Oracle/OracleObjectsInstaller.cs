using System;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle
{
    public static class OracleObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(OracleStorage));
        public static void Install(OracleConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            if (TablesExists(connection))
            {
                Log.Info("DB tables already exist. Exit install");
                return;
            }

            Log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource(
                typeof(OracleObjectsInstaller).Assembly,
                "Hangfire.Oracle.Install.sql");

            connection.Execute(script);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static bool TablesExists(OracleConnection connection)
        {
            return connection.ExecuteScalar<string>("select table_name from user_tables where table_name like 'HANGFIRE_JOB'") != null;            
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}