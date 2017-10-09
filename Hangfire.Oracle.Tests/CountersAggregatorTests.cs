using System;
using System.Linq;
using System.Threading;
using Dapper;
using Xunit;
using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Tests
{
    public class CountersAggregatorTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly CountersAggregator _sut;
        private readonly OracleStorage _storage;
        private readonly OracleConnection _connection;

        public CountersAggregatorTests()
        {
            _connection = ConnectionUtils.CreateConnection();
            _storage = new OracleStorage(_connection);
            _sut = new CountersAggregator(_storage, TimeSpan.Zero);
        }
        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void CountersAggregatorExecutesProperly()
        {
            const string createSql = @"insert into HANGFIRE_COUNTER (KEY, VALUE, ExpireAt) values ('key', 1, :expireAt)";

            _storage.UseConnection(connection =>
            {
                // Arrange
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddHours(1) });

                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                _sut.Execute(cts.Token);

                // Assert
                Assert.Equal(1, connection.Query<int>(@"select count(*) from HANGFIRE_AGGREGATEDCOUNTER").Single());
            });
        }
    }
}
