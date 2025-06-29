using System.Data;
using System.Data.SQLite;
using System.Linq.Expressions;
namespace sysSQLite.Clases
{
    public class SQLiteDBManager : IDisposable
    {
        private readonly string _connectionString;
        private SQLiteConnection _connection;
        private SQLiteTransaction _transaction;

        public SQLiteDBManager(string dbFilePath)
        {
            _connectionString = $"Data Source={dbFilePath};Version=3;BusyTimeout=5000;";
            _connection = new SQLiteConnection(_connectionString);
        }

        public async Task OpenAsync()
        {
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync();
                using var pragmaCmd = new SQLiteCommand("PRAGMA journal_mode=WAL;", _connection);
                await pragmaCmd.ExecuteNonQueryAsync();
            }
        }

        public void BeginTransaction()
        {
            if(_connection.State != ConnectionState.Open)
                _connection.Open();

            _transaction = _connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            _transaction?.Commit();
            _transaction = null;
        }

        public void RollbackTransaction()
        {
            _transaction?.Rollback();
            _transaction = null;
        }

        public async Task<int> ExecuteNonQueryAsync(string query, Dictionary<string, object> parameters)
        {
            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync();

            using var command = new SQLiteCommand(query, _connection, _transaction);

            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue("@" + param.Key, param.Value ?? DBNull.Value);
            }

            return await command.ExecuteNonQueryAsync();
        }

        public async Task<object> ExecuteScalarAsync(string sql, Dictionary<string, object> parameters = null)
        {
            using var cmd = new SQLiteCommand(sql, _connection, _transaction);
            if (parameters != null)
            {
                foreach (var p in parameters)
                    cmd.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);
            }

            return await cmd.ExecuteScalarAsync();
        }

        public async Task<List<Dictionary<string, object>>> ExecuteQueryAsync(string sql, Dictionary<string, object> parameters = null)
        {
            using var cmd = new SQLiteCommand(sql, _connection, _transaction);
            if (parameters != null)
            {
                foreach (var p in parameters)
                    cmd.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);
            }

            var result = new List<Dictionary<string, object>>();
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                    row[reader.GetName(i)] = reader.GetValue(i);

                result.Add(row);
            }

            return result;
        }

        public async Task<int> InsertAsync<T>(T entity, string overrideColumns = null, Dictionary<string, object> additionalParams = null) where T : class, new()
        {
            string tableName = typeof(T).Name;

            var properties = typeof(T).GetProperties()
                .Where(p => p.CanRead)
                .ToList();

            var columnNames = properties.Select(p => p.Name).ToList();

            if (!string.IsNullOrWhiteSpace(overrideColumns))
                columnNames = overrideColumns.Split(',').Select(c => c.Trim()).ToList();

            string columns = string.Join(", ", columnNames);
            string values = string.Join(", ", columnNames.Select(name => "@" + name));
            string query = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";

            var parameters = new Dictionary<string, object>();

            foreach (var prop in properties)
            {
                if (columnNames.Contains(prop.Name))
                {
                    var value = prop.GetValue(entity);

                    var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                    if (type == typeof(Guid))
                    {
                        parameters[prop.Name] = value != null ? value.ToString() : DBNull.Value;
                    }
                    else
                    {
                        parameters[prop.Name] = value ?? DBNull.Value;
                    }
                }
            }

            if (additionalParams != null)
            {
                foreach (var kvp in additionalParams)
                {
                    if (!parameters.ContainsKey(kvp.Key))
                        parameters[kvp.Key] = kvp.Value ?? DBNull.Value;
                }
            }

            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync();

            using var cmd = new SQLiteCommand(query, _connection, _transaction);

            foreach (var param in parameters)
            {
                cmd.Parameters.AddWithValue("@" + param.Key, param.Value);
            }

            return await cmd.ExecuteNonQueryAsync();
        }


        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                await _connection.OpenAsync();
                using var cmd = new SQLiteCommand("SELECT 1", _connection);
                await cmd.ExecuteScalarAsync();

                return _connection.State == System.Data.ConnectionState.Open;
            }
            catch
            {
                return false;
            }
            finally
            {
                if (_connection.State == System.Data.ConnectionState.Open)
                    _connection.Close();
            }
        }

        private string GetColumnName<T>(Expression<Func<T, object>> propExpression)
        {
            if (propExpression.Body is MemberExpression memberExp)
                return memberExp.Member.Name;

            if (propExpression.Body is UnaryExpression unaryExp && unaryExp.Operand is MemberExpression memberOperand)
                return memberOperand.Member.Name;

            throw new ArgumentException("Invalid lambda expression");
        }

        public async Task<IEnumerable<T>> Select<T>(
            List<Expression<Func<T, object>>> selectColumns = null,
            string rawColumns = null,
            string where = "",
            Dictionary<string, object> parameters = null,
            int limit = 0) where T : class, new()
        {
            string tableName = typeof(T).Name;
            string columns = "*";

            if (selectColumns != null && selectColumns.Count > 0)
            {
                var columnNames = selectColumns.Select(col => GetColumnName(col)).ToList();
                columns = string.Join(", ", columnNames);
            }
            else if (!string.IsNullOrWhiteSpace(rawColumns))
            {
                columns = rawColumns;
            }

            string whereClause = string.IsNullOrWhiteSpace(where) ? "" : $" {where.TrimStart()}";

            string query = $"SELECT {columns} FROM {tableName}{whereClause}";

            if (limit > 0)
                query += $" LIMIT {limit}";

            if (_connection.State != ConnectionState.Open)
                await _connection.OpenAsync();

            var rows = await ExecuteQueryAsync(query, parameters);
            return DataTableToList<T>(rows);
        }

        private IEnumerable<T> DataTableToList<T>(List<Dictionary<string, object>> rows) where T : class, new()
        {
            var properties = typeof(T).GetProperties();

            foreach (var row in rows)
            {
                var item = new T();

                foreach (var prop in properties)
                {
                    // Buscar la clave ignorando mayúsculas/minúsculas
                    var key = row.Keys.FirstOrDefault(k => string.Equals(k, prop.Name, StringComparison.OrdinalIgnoreCase));

                    if (key != null)
                    {
                        var value = row[key];

                        if (value == DBNull.Value || value == null)
                        {
                            prop.SetValue(item, null);
                            continue;
                        }

                        // resto de la conversión que ya tienes
                        Type targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                        // Conversión igual que antes
                        try
                        {
                            if (targetType.IsEnum)
                            {
                                var enumValue = value.ToString();
                                if (Enum.TryParse(targetType, enumValue, out var parsedEnum))
                                    prop.SetValue(item, parsedEnum);
                            }
                            else if (targetType == typeof(Guid))
                            {
                                prop.SetValue(item, Guid.Parse(value.ToString()));
                            }
                            else if (targetType == typeof(int))
                            {
                                if (value is long l)
                                    prop.SetValue(item, (int)l);
                                else
                                    prop.SetValue(item, Convert.ToInt32(value));
                            }
                            else if (targetType == typeof(DateTime))
                            {
                                if (value is string s)
                                {
                                    if (DateTime.TryParse(s, out var dt))
                                        prop.SetValue(item, dt);
                                }
                                else if (value is long ticks)
                                {
                                    prop.SetValue(item, new DateTime(ticks));
                                }
                                else
                                {
                                    prop.SetValue(item, Convert.ToDateTime(value));
                                }
                            }
                            else
                            {
                                prop.SetValue(item, Convert.ChangeType(value, targetType));
                            }
                        }
                        catch
                        {
                            // opcional: loguear error
                        }
                    }
                }

                yield return item;
            }
        }

        public void Dispose()
        {
            _transaction?.Dispose();
            _connection?.Dispose();
        }
    }
}
