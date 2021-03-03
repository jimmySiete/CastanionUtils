using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Castanion.DataAccess
{
    public class DataAccessObject
    {
        #region properties

        public String ConnectionString { get; set; }
        private bool TakeConnectionStringFromConfig { get; set; }

        #endregion

        #region Constructors

        public DataAccessObject()
        {
            this.TakeConnectionStringFromConfig = true;
            this.ConnectionString = ConfigurationManager.AppSettings["ConnectionString"] != null ?
                ConfigurationManager.AppSettings["ConnectionString"].ToString() : String.Empty;
        }

        public DataAccessObject(String ConnectionString)
        {
            this.ConnectionString = ConnectionString;
            this.TakeConnectionStringFromConfig = false;
        }


        #endregion

        #region Methods

        #region Datatables

        public DataTable GetDataTableFromStoredProcedure(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public DataTable GetDataTableFromStoredProcedure(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public DataTable GetDataTableFromStoredProcedure(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.StoredProcedure, storedProcedure, connectionString, null, sqlTransaction);
        }

        public DataTable GetDataTableFromStoredProcedure(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.StoredProcedure, storedProcedure, connectionString, parameters, sqlTransaction);
        }

        public DataTable GetDataTableFromQuery(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.Text, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public DataTable GetDataTableFromQuery(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.Text, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public DataTable GetDataTableFromQuery(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.Text, storedProcedure, connectionString, null, sqlTransaction);
        }

        public DataTable GetDataTableFromQuery(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataTable(CommandType.Text, storedProcedure, connectionString, parameters, sqlTransaction);
        }

        private DataTable GetDataTable(CommandType commandType, String query, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            SqlCommand command = Connection.GetConnection(commandType, query, connectionString, sqlTransaction);
            if (parameters != null && parameters.Any())
                command.Parameters.AddRange(parameters.ToArray());
            return Connection.GetDataTable(command);
        }

        #endregion

        #region DataSets

        public DataSet GetDataSetFromStoredProcedure(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public DataSet GetDataSetFromStoredProcedure(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public DataSet GetDataSetFromStoredProcedure(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.StoredProcedure, storedProcedure, connectionString, null, sqlTransaction);
        }

        public DataSet GetDataSetFromStoredProcedure(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.StoredProcedure, storedProcedure, connectionString, parameters, sqlTransaction);
        }


        public DataSet GetDataSetFromQuery(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.Text, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public DataSet GetDataSetFromQuery(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.Text, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public DataSet GetDataSetFromQuery(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.Text, storedProcedure, connectionString, null, sqlTransaction);
        }

        public DataSet GetDataSetFromQuery(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetDataSet(CommandType.Text, storedProcedure, connectionString, parameters, sqlTransaction);
        }

        private DataSet GetDataSet(CommandType commandType, String query, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            SqlCommand command = Connection.GetConnection(commandType, query, connectionString, sqlTransaction);
            if (parameters != null && parameters.Any())
                command.Parameters.AddRange(parameters.ToArray());
            return Connection.GetDataSet(command);
        }

        #endregion

        #region ExecuteNonQuery

        public bool GetExecuteNonQueryFromStoredProcedure(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromStoredProcedure(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.StoredProcedure, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromStoredProcedure(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.StoredProcedure, storedProcedure, connectionString, null, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromStoredProcedure(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.StoredProcedure, storedProcedure, connectionString, parameters, sqlTransaction);
        }


        public bool GetExecuteNonQueryFromQuery(String storedProcedure, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.Text, storedProcedure, this.ConnectionString, null, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromQuery(String storedProcedure, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.Text, storedProcedure, this.ConnectionString, parameters, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromQuery(String storedProcedure, String connectionString, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.Text, storedProcedure, connectionString, null, sqlTransaction);
        }

        public bool GetExecuteNonQueryFromQuery(String storedProcedure, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            return GetExecuteNonQuery(CommandType.Text, storedProcedure, connectionString, parameters, sqlTransaction);
        }

        private bool GetExecuteNonQuery(CommandType commandType, String query, String connectionString, List<SqlParameter> parameters, SqlTransaction sqlTransaction = null)
        {
            SqlCommand command = Connection.GetConnection(commandType, query, connectionString, sqlTransaction);
            if (parameters != null && parameters.Any())
                command.Parameters.AddRange(parameters.ToArray());
            return Connection.ExecuteNonQuery(command);
        }

        #endregion

        #region Bulkdata

        /// <summary>
        /// Method to Execute bulk data
        /// </summary>
        /// <param name="table"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public bool ExecuteBulkData(DataTable table,String TableName) 
        {
            SqlConnection connection = new SqlConnection(this.ConnectionString);

            return SqlBulkData.BulkCopy(table,connection,TableName);
        }

        #endregion

        #endregion
    }
}
