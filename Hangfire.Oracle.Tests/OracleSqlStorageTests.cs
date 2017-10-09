using System;
using System.Linq;
using System.Transactions;
using Xunit;

namespace Hangfire.Oracle.Tests
{
    public class OracleStorageTests :  IClassFixture<TestDatabaseFixture>
    {
        private readonly OracleStorageOptions _options;

        public OracleStorageTests()
        {
            _options = new OracleStorageOptions { PrepareSchemaIfNecessary = false };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleStorage((string)null));

            Assert.Equal("nameOrConnectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new OracleStorage("hello", null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_CanCreateSqlServerStorage_WithExistingConnection()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var storage = new OracleStorage(connection);

                Assert.NotNull(storage);
            }
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            using (var connection = (OracleStorageConnection)storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact, CleanDatabase]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            var storage = CreateStorage();

            var components = storage.GetComponents();

            var componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(ExpirationManager), componentTypes);
        }

        [Fact, CleanDatabase(isolationLevel: IsolationLevel.ReadUncommitted)]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        private OracleStorage CreateStorage()
        {
            return new OracleStorage(
                ConnectionUtils.GetConnectionString(),
                _options);
        }
    }
}
