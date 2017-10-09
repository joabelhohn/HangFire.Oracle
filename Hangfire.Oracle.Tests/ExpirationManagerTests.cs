using System;
using System.Linq;
using System.Threading;
using Dapper;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests
{
    public class ExpirationManagerTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly CancellationToken _token;

        public ExpirationManagerTests()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(1));
                var manager = CreateManager(connection);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                        "insert into HANGFIRE_AGGREGATEDCOUNTER (KEY, VALUE, EXPIREAT) values ('key', 1, :expireAt)", 
                        new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                var count = connection.Query<int>(@"select count(*) from HANGFIRE_COUNTER").Single();
                // Assert
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into HANGFIRE_JOB (InvocationData, Arguments, CreatedAt, ExpireAt) " +
                    "values (' ', ' ', sysdate, :expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                var countResult = connection.Query<int>(@"select count(*) from HANGFIRE_JOB").Single();
                // Assert
                Assert.Equal(0, countResult);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into HANGFIRE_LIST (Key, ExpireAt) values ('key', :expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HANGFIRE_LIST").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into HANGFIRE_SET (KEY, SCORE, VALUE, EXPIREAT) values ('key', 0, ' ', :expireAt)", 
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HANGFIRE_SET").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = CreateConnection())
            {
                // Arrange
                const string createSql = @"
begin
  insert into HANGFIRE_HASH (KEY, FIELD, VALUE, ExpireAt) values ('key1', 'field', ' ', :expireAt);
  insert into HANGFIRE_HASH (KEY, FIELD, VALUE, ExpireAt) values ('key2', 'field', ' ', :expireAt);
end;";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from HANGFIRE_HASH").Single());
            }
        }

        private static int CreateExpirationEntry(OracleConnection connection, DateTime? expireAt)
        {
            const string insertSql = @"
begin
    delete from HANGFIRE_AGGREGATEDCOUNTER;
    insert into HANGFIRE_AGGREGATEDCOUNTER (KEY, VALUE, ExpireAt) values ('key', 1, :expireAt) returning ID into :id;
end;";
            var param = new DynamicParameters();
            param.Add(name: "expireAt", value: expireAt, direction: System.Data.ParameterDirection.Input);
            param.Add(name: "id", dbType: System.Data.DbType.Int32, direction: System.Data.ParameterDirection.Output);

            connection.Execute(insertSql, param);
            var recordId = param.Get<int>("id");
            return recordId;
        }

        private static bool IsEntryExpired(OracleConnection connection, int entryId)
        {
            var count = connection.Query<int>("select count(*) from HANGFIRE_AGGREGATEDCOUNTER where Id = :id", new { id = entryId }).Single();
            return count == 0;
        }

        private OracleConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private ExpirationManager CreateManager(OracleConnection connection)
        {
            var storage = new OracleStorage(connection);
            return new ExpirationManager(storage, TimeSpan.Zero);
        }
    }
}
