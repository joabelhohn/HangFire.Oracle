using System;
using System.Threading;
using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests
{
    public class TestDatabaseFixture : IDisposable
    {
        private static readonly object GlobalLock = new object();
        public TestDatabaseFixture()
        {
            Monitor.Enter(GlobalLock);
            CreateAndInitializeDatabase();
        }

        public void Dispose()
        {
            DropDatabase();
            Monitor.Exit(GlobalLock);
        }

        private static void CreateAndInitializeDatabase()
        {
            var recreateDatabaseSql = String.Format(
                @"CREATE DATABASE IF NOT EXISTS `{0}`",
                ConnectionUtils.GetDatabaseName());

            //using (var connection = new OracleConnection(
            //    ConnectionUtils.GetMasterConnectionString()))
            //{
            //    connection.Execute(recreateDatabaseSql);
            //}

            using (var connection = new OracleConnection(
                ConnectionUtils.GetConnectionString()))
            {
                OracleObjectsInstaller.Install(connection);
            }
        }

        private static void DropDatabase()
        {
            var recreateDatabaseSql = String.Format(
                   @"DROP DATABASE IF EXISTS `{0}`",
                   ConnectionUtils.GetDatabaseName());

            //using (var connection = new OracleConnection(
            //    ConnectionUtils.GetMasterConnectionString()))
            //{
            //    connection.Execute(recreateDatabaseSql);
            //}
            using (var connection = new OracleConnection(
                ConnectionUtils.GetMasterConnectionString()))
            {
                connection.Execute(@"
begin
	delete from HANGFIRE_STATE;
	delete from HANGFIRE_SET;
	delete from HANGFIRE_SERVER;
	delete from HANGFIRE_LIST;
	delete from HANGFIRE_JOBSTATE;
	delete from HANGFIRE_JOBQUEUE;
	delete from HANGFIRE_JOBPARAMETER;
	delete from HANGFIRE_JOB;
	delete from HANGFIRE_HASH;
	delete from HANGFIRE_DISTRIBUTEDLOCK;
	delete from HANGFIRE_COUNTER;
	delete from HANGFIRE_AGGREGATEDCOUNTER;
end;");
            }
        }
    }
}
